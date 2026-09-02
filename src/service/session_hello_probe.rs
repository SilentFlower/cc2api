use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{ACCEPT, ACCEPT_ENCODING, CONNECTION, USER_AGENT};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::AppError;
use crate::model::account::Account;
use crate::model::identity::device_profile;
use crate::service::version_profile::profile_for_version;
use crate::store::cache::{CacheStore, SessionHelloProbeState};

const DEFAULT_HELLO_ENDPOINT: &str = "https://api.anthropic.com/api/hello";
const HELLO_ACCEPT_ENCODING: &str = "gzip, deflate, br, zstd";
const FOLLOWER_POLL_INTERVAL: Duration = Duration::from_millis(50);
const LOCK_SAFETY_MARGIN: Duration = Duration::from_secs(2);

/// 有效上游 Session 首次 Hello 代理探测运行配置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionHelloProbeConfig {
    /// 是否启用探测。
    pub enabled: bool,
    /// 是否在失败时阻断业务请求。
    pub strict: bool,
    /// 单次网络探测总超时。
    pub timeout: Duration,
    /// 成功状态的滑动空闲 TTL。
    pub success_ttl: Duration,
    /// 失败或超时状态的固定冷却时间。
    pub failure_cooldown: Duration,
}

impl SessionHelloProbeConfig {
    /// 从 settings 字符串构造探测配置。
    ///
    /// @param enabled 功能开关字符串。
    /// @param strict 严格模式开关字符串。
    /// @param timeout_secs 探测超时秒数。
    /// @param success_ttl_secs 成功状态 TTL 秒数。
    /// @param failure_cooldown_secs 失败冷却秒数。
    /// @return 合法配置；任一值非法时返回 `BadRequest`。
    pub fn parse(
        enabled: &str,
        strict: &str,
        timeout_secs: &str,
        success_ttl_secs: &str,
        failure_cooldown_secs: &str,
    ) -> Result<Self, AppError> {
        Ok(Self {
            enabled: parse_bool("session_hello_probe_enabled", enabled)?,
            strict: parse_bool("session_hello_probe_strict", strict)?,
            timeout: Duration::from_secs(parse_u64_range(
                "session_hello_probe_timeout_secs",
                timeout_secs,
                1,
                30,
            )?),
            success_ttl: Duration::from_secs(parse_u64_range(
                "session_hello_probe_success_ttl_secs",
                success_ttl_secs,
                60,
                86_400,
            )?),
            failure_cooldown: Duration::from_secs(parse_u64_range(
                "session_hello_probe_failure_cooldown_secs",
                failure_cooldown_secs,
                10,
                3_600,
            )?),
        })
    }
}

impl Default for SessionHelloProbeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            strict: false,
            timeout: Duration::from_secs(5),
            success_ttl: Duration::from_secs(3_600),
            failure_cooldown: Duration::from_secs(300),
        }
    }
}

/// Gateway 根据 Session Hello 探测结果采取的动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionHelloProbeDecision {
    /// 继续发送当前业务请求。
    Proceed,
    /// 普通网络错误或非 200，严格模式应返回 502。
    BlockFailure,
    /// 探测超时，严格模式应返回 504。
    BlockTimeout,
    /// 缓存或 singleflight 不可用，严格模式应返回 503。
    BlockUnavailable,
}

/// 负责有效上游 Session 首次 Hello 请求、缓存去重和失败模式判定的服务。
pub struct SessionHelloProbeService {
    cache: Arc<dyn CacheStore>,
    endpoint: String,
}

struct ProbeSessionLogs {
    downstream: String,
    upstream: String,
}

impl ProbeSessionLogs {
    fn new(real_session_id: &str, upstream_session_id: &str) -> Self {
        Self {
            downstream: short_hash(real_session_id.as_bytes()),
            upstream: short_hash(upstream_session_id.as_bytes()),
        }
    }
}

impl SessionHelloProbeService {
    /// 构造使用 Anthropic 官方 Hello 端点的探测服务。
    ///
    /// @param cache Memory 或 Redis 缓存实现。
    /// @return Session Hello 探测服务实例。
    pub fn new(cache: Arc<dyn CacheStore>) -> Self {
        Self {
            cache,
            endpoint: DEFAULT_HELLO_ENDPOINT.to_string(),
        }
    }

    /// 确保当前上游 session、账号与代理路径已经完成 Hello 探测。
    ///
    /// @param account 最终选中且已经通过业务 admission 的账号。
    /// @param real_session_id 未经上游 session 池改写的真实下游 session。
    /// @param upstream_session_id 最终发往上游的有效 session；池关闭时等于真实 session。
    /// @param config 当前热加载配置。
    /// @return 返回继续业务请求或严格模式阻断原因。
    pub async fn ensure_ready(
        &self,
        account: &Account,
        real_session_id: &str,
        upstream_session_id: &str,
        config: SessionHelloProbeConfig,
    ) -> SessionHelloProbeDecision {
        if !config.enabled {
            return SessionHelloProbeDecision::Proceed;
        }

        let state_key = probe_state_key(account.id, upstream_session_id, &account.proxy_url);
        let session_logs = ProbeSessionLogs::new(real_session_id, upstream_session_id);
        let lock_key = format!("{}:lock", state_key);
        let owner = Uuid::new_v4().to_string();
        let lock_ttl = config.timeout.saturating_add(LOCK_SAFETY_MARGIN);
        let follower_deadline = Instant::now() + lock_ttl + LOCK_SAFETY_MARGIN;
        let mut waited_for_leader = false;

        loop {
            match self
                .cache
                .get_session_hello_probe_state(&state_key, config.success_ttl)
                .await
            {
                Ok(Some(state)) => {
                    let source = if waited_for_leader {
                        "follower"
                    } else {
                        "cache"
                    };
                    info!(
                        "session hello probe cache hit: account={} downstream_session={} upstream_session={} proxy_configured={} source={} result={}",
                        account.id,
                        session_logs.downstream,
                        session_logs.upstream,
                        !account.proxy_url.is_empty(),
                        source,
                        state.as_str()
                    );
                    return decision_for_state(state, config.strict);
                }
                Ok(None) => {}
                Err(error) => {
                    return cache_failure_decision(
                        account,
                        &session_logs,
                        config.strict,
                        "read",
                        &error,
                    );
                }
            }

            match self.cache.acquire_lock(&lock_key, &owner, lock_ttl).await {
                Ok(true) => {
                    // 状态可能在读缓存和拿锁之间由上一任 leader 写入，拿锁后必须再检查一次。
                    match self
                        .cache
                        .get_session_hello_probe_state(&state_key, config.success_ttl)
                        .await
                    {
                        Ok(Some(state)) => {
                            info!(
                                "session hello probe cache hit after lock: account={} downstream_session={} upstream_session={} proxy_configured={} source=cache result={}",
                                account.id,
                                session_logs.downstream,
                                session_logs.upstream,
                                !account.proxy_url.is_empty(),
                                state.as_str()
                            );
                            self.cache.release_lock(&lock_key, &owner).await;
                            return decision_for_state(state, config.strict);
                        }
                        Ok(None) => {
                            if waited_for_leader {
                                // follower 已经见证过本轮 leader；拿到过期锁仅表示结果未落盘，不能放大为重复发包。
                                self.cache.release_lock(&lock_key, &owner).await;
                                warn!(
                                    "session hello probe leader result unavailable: account={} downstream_session={} upstream_session={} proxy_configured={} source=follower",
                                    account.id,
                                    session_logs.downstream,
                                    session_logs.upstream,
                                    !account.proxy_url.is_empty()
                                );
                                return if config.strict {
                                    SessionHelloProbeDecision::BlockUnavailable
                                } else {
                                    SessionHelloProbeDecision::Proceed
                                };
                            }
                            return self
                                .run_leader_probe(
                                    account,
                                    &session_logs,
                                    &state_key,
                                    &lock_key,
                                    &owner,
                                    config,
                                )
                                .await;
                        }
                        Err(error) => {
                            self.cache.release_lock(&lock_key, &owner).await;
                            return cache_failure_decision(
                                account,
                                &session_logs,
                                config.strict,
                                "leader_recheck",
                                &error,
                            );
                        }
                    }
                }
                Ok(false) => {
                    waited_for_leader = true;
                    if Instant::now() >= follower_deadline {
                        warn!(
                            "session hello probe follower timed out: account={} downstream_session={} upstream_session={} proxy_configured={} source=follower",
                            account.id,
                            session_logs.downstream,
                            session_logs.upstream,
                            !account.proxy_url.is_empty()
                        );
                        return if config.strict {
                            SessionHelloProbeDecision::BlockUnavailable
                        } else {
                            SessionHelloProbeDecision::Proceed
                        };
                    }
                    sleep(FOLLOWER_POLL_INTERVAL).await;
                }
                Err(error) => {
                    return cache_failure_decision(
                        account,
                        &session_logs,
                        config.strict,
                        "lock",
                        &error,
                    );
                }
            }
        }
    }

    async fn run_leader_probe(
        &self,
        account: &Account,
        session_logs: &ProbeSessionLogs,
        state_key: &str,
        lock_key: &str,
        owner: &str,
        config: SessionHelloProbeConfig,
    ) -> SessionHelloProbeDecision {
        let started_at = Instant::now();
        let (state, http_status) =
            if !account.proxy_url.is_empty() && reqwest::Proxy::all(&account.proxy_url).is_err() {
                (SessionHelloProbeState::Failure, None)
            } else {
                let client = crate::tlsfp::get_request_client(&account.proxy_url);
                let user_agent = hello_user_agent_for_account(account);
                let request = client
                    .head(&self.endpoint)
                    .header(USER_AGENT, user_agent)
                    .header(ACCEPT, "*/*")
                    .header(ACCEPT_ENCODING, HELLO_ACCEPT_ENCODING)
                    .header(CONNECTION, "keep-alive");
                match tokio::time::timeout(config.timeout, request.send()).await {
                    Err(_) => (SessionHelloProbeState::Timeout, None),
                    Ok(Err(_)) => (SessionHelloProbeState::Failure, None),
                    Ok(Ok(response)) if response.status() == reqwest::StatusCode::OK => (
                        SessionHelloProbeState::Success,
                        Some(response.status().as_u16()),
                    ),
                    Ok(Ok(response)) => (
                        SessionHelloProbeState::Failure,
                        Some(response.status().as_u16()),
                    ),
                }
            };

        let ttl = if state == SessionHelloProbeState::Success {
            config.success_ttl
        } else {
            config.failure_cooldown
        };
        let cache_result = self
            .cache
            .set_session_hello_probe_state(state_key, state, ttl)
            .await;
        if cache_result.is_ok() {
            self.cache.release_lock(lock_key, owner).await;
        }

        info!(
            "session hello probe completed: account={} downstream_session={} upstream_session={} proxy_configured={} source=network duration_ms={} http_status={:?} result={}",
            account.id,
            session_logs.downstream,
            session_logs.upstream,
            !account.proxy_url.is_empty(),
            started_at.elapsed().as_millis(),
            http_status,
            state.as_str()
        );

        if let Err(error) = cache_result {
            // 写入失败时让锁自然过期，以阻止当前并发波次的 follower 重复探测。
            return cache_failure_decision(account, session_logs, config.strict, "write", &error);
        }
        decision_for_state(state, config.strict)
    }

    /// 构造使用测试 endpoint 的探测服务。
    ///
    /// @param cache Memory 或 Redis 缓存实现。
    /// @param endpoint 测试服务地址。
    /// @return 使用指定测试地址的探测服务实例。
    #[cfg(test)]
    pub(crate) fn with_endpoint(cache: Arc<dyn CacheStore>, endpoint: String) -> Self {
        Self { cache, endpoint }
    }

    /// 测试中替换 Hello endpoint，生产代码始终使用 Anthropic 官方地址。
    ///
    /// @param endpoint 测试服务地址。
    #[cfg(test)]
    pub(crate) fn set_endpoint_for_test(&mut self, endpoint: String) {
        self.endpoint = endpoint;
    }
}

fn hello_user_agent_for_account(account: &Account) -> &'static str {
    let profile = device_profile(account);
    profile_for_version(&profile.env.version)
        .telemetry
        .growthbook_user_agent
}

fn parse_bool(key: &str, raw: &str) -> Result<bool, AppError> {
    match raw.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(AppError::BadRequest(format!(
            "'{}' 必须是 true 或 false",
            key
        ))),
    }
}

fn parse_u64_range(key: &str, raw: &str, min: u64, max: u64) -> Result<u64, AppError> {
    match raw.trim().parse::<u64>() {
        Ok(value) if value >= min && value <= max => Ok(value),
        _ => Err(AppError::BadRequest(format!(
            "'{}' 必须是 {} 到 {} 之间的整数",
            key, min, max
        ))),
    }
}

fn probe_state_key(account_id: i64, upstream_session_id: &str, proxy_url: &str) -> String {
    format!(
        "session_hello_probe:v1:{}:{}:{}",
        account_id,
        hex::encode(Sha256::digest(upstream_session_id.as_bytes())),
        hex::encode(Sha256::digest(proxy_url.as_bytes()))
    )
}

fn short_hash(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))[..12].to_string()
}

fn decision_for_state(state: SessionHelloProbeState, strict: bool) -> SessionHelloProbeDecision {
    if !strict || state == SessionHelloProbeState::Success {
        return SessionHelloProbeDecision::Proceed;
    }
    match state {
        SessionHelloProbeState::Success => SessionHelloProbeDecision::Proceed,
        SessionHelloProbeState::Failure => SessionHelloProbeDecision::BlockFailure,
        SessionHelloProbeState::Timeout => SessionHelloProbeDecision::BlockTimeout,
    }
}

fn cache_failure_decision(
    account: &Account,
    session_logs: &ProbeSessionLogs,
    strict: bool,
    operation: &str,
    error: &AppError,
) -> SessionHelloProbeDecision {
    warn!(
        "session hello probe cache failed: account={} downstream_session={} upstream_session={} proxy_configured={} source=cache operation={} error={}",
        account.id,
        session_logs.downstream,
        session_logs.upstream,
        !account.proxy_url.is_empty(),
        operation,
        error
    );
    if strict {
        SessionHelloProbeDecision::BlockUnavailable
    } else {
        SessionHelloProbeDecision::Proceed
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::head;
    use chrono::Utc;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    use super::*;
    use crate::model::account::{
        AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData, DEFAULT_ALLOW_1M_MODELS,
        DEFAULT_UPSTREAM_SESSION_POOL_SIZE, DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY,
        DEFAULT_UPSTREAM_SESSION_TTL_MINUTES,
    };
    use crate::store::cache::{
        RpmAcquire, UpstreamSessionPoolResolve, UpstreamSessionPoolStatus,
        UpstreamSessionRefreshPolicy,
    };
    use crate::store::memory::MemoryStore;
    use crate::store::redis::RedisStore;

    struct FaultInjectingProbeCache {
        inner: MemoryStore,
        fail_reads: bool,
        write_failures_remaining: AtomicUsize,
    }

    impl FaultInjectingProbeCache {
        fn with_read_failure() -> Self {
            Self {
                inner: MemoryStore::new(),
                fail_reads: true,
                write_failures_remaining: AtomicUsize::new(0),
            }
        }

        fn with_write_failures(count: usize) -> Self {
            Self {
                inner: MemoryStore::new(),
                fail_reads: false,
                write_failures_remaining: AtomicUsize::new(count),
            }
        }
    }

    #[axum::async_trait]
    impl CacheStore for FaultInjectingProbeCache {
        async fn get_session_account_id(
            &self,
            session_hash: &str,
        ) -> Result<Option<i64>, AppError> {
            self.inner.get_session_account_id(session_hash).await
        }

        async fn set_session_account_id(
            &self,
            session_hash: &str,
            account_id: i64,
            ttl: Duration,
        ) -> Result<(), AppError> {
            self.inner
                .set_session_account_id(session_hash, account_id, ttl)
                .await
        }

        async fn delete_session(&self, session_hash: &str) -> Result<(), AppError> {
            self.inner.delete_session(session_hash).await
        }

        async fn acquire_slot(&self, key: &str, max: i32, ttl: Duration) -> Result<bool, AppError> {
            self.inner.acquire_slot(key, max, ttl).await
        }

        async fn release_slot(&self, key: &str) {
            self.inner.release_slot(key).await;
        }

        async fn get_slot_count(&self, key: &str) -> i64 {
            self.inner.get_slot_count(key).await
        }

        async fn get_account_rpm(&self, account_id: i64, minute_ts: i64) -> Result<i64, AppError> {
            self.inner.get_account_rpm(account_id, minute_ts).await
        }

        async fn try_acquire_account_rpm(
            &self,
            account_id: i64,
            minute_ts: i64,
            limit: i32,
            ttl: Duration,
        ) -> Result<RpmAcquire, AppError> {
            self.inner
                .try_acquire_account_rpm(account_id, minute_ts, limit, ttl)
                .await
        }

        async fn resolve_upstream_session_pool(
            &self,
            account_id: i64,
            real_session_id: &str,
            pool_size: i32,
            ttl: Duration,
            refresh_policy: UpstreamSessionRefreshPolicy,
            allow_insert: bool,
        ) -> Result<UpstreamSessionPoolResolve, AppError> {
            self.inner
                .resolve_upstream_session_pool(
                    account_id,
                    real_session_id,
                    pool_size,
                    ttl,
                    refresh_policy,
                    allow_insert,
                )
                .await
        }

        async fn get_upstream_session_pool_status(
            &self,
            account_id: i64,
            pool_size: i32,
            ttl: Duration,
        ) -> Result<UpstreamSessionPoolStatus, AppError> {
            self.inner
                .get_upstream_session_pool_status(account_id, pool_size, ttl)
                .await
        }

        async fn get_session_hello_probe_state(
            &self,
            key: &str,
            success_ttl: Duration,
        ) -> Result<Option<SessionHelloProbeState>, AppError> {
            if self.fail_reads {
                return Err(AppError::Internal("测试缓存读取不可用".into()));
            }
            self.inner
                .get_session_hello_probe_state(key, success_ttl)
                .await
        }

        async fn set_session_hello_probe_state(
            &self,
            key: &str,
            state: SessionHelloProbeState,
            ttl: Duration,
        ) -> Result<(), AppError> {
            if self
                .write_failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(AppError::Internal("测试缓存写入不可用".into()));
            }
            self.inner
                .set_session_hello_probe_state(key, state, ttl)
                .await
        }

        async fn acquire_lock(
            &self,
            key: &str,
            owner: &str,
            ttl: Duration,
        ) -> Result<bool, AppError> {
            self.inner.acquire_lock(key, owner, ttl).await
        }

        async fn release_lock(&self, key: &str, owner: &str) {
            self.inner.release_lock(key, owner).await;
        }
    }

    fn test_account() -> Account {
        Account {
            id: 42,
            name: "测试账号".into(),
            email: "user@example.com".into(),
            status: AccountStatus::Active,
            auth_type: AccountAuthType::SetupToken,
            setup_token: String::new(),
            access_token: String::new(),
            refresh_token: String::new(),
            expires_at: None,
            oauth_refreshed_at: None,
            auth_error: String::new(),
            proxy_url: String::new(),
            device_id: String::new(),
            canonical_env: json!({}),
            canonical_prompt: json!({}),
            canonical_process: json!({}),
            billing_mode: BillingMode::Strip,
            account_uuid: None,
            organization_uuid: None,
            subscription_type: None,
            concurrency: 3,
            priority: 50,
            rpm_limit: 0,
            rate_limited_at: None,
            rate_limit_reset_at: None,
            disable_reason: String::new(),
            auto_telemetry: false,
            auto_poll_usage: false,
            allow_1m_models: DEFAULT_ALLOW_1M_MODELS.into(),
            allow_fast_mode: false,
            upstream_session_pool_enabled: false,
            upstream_session_pool_size: DEFAULT_UPSTREAM_SESSION_POOL_SIZE,
            upstream_session_ttl_minutes: DEFAULT_UPSTREAM_SESSION_TTL_MINUTES,
            upstream_session_refresh_policy: DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY.into(),
            telemetry_count: 0,
            usage_data: json!({}),
            usage_fetched_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn hello_user_agent_follows_selected_account_profile() {
        let current = test_account();
        assert_eq!(hello_user_agent_for_account(&current), "Bun/1.4.1");

        let mut rollback = test_account();
        let profile = crate::service::version_profile::profile_for_key("2.1.220").unwrap();
        rollback.canonical_env = serde_json::to_value(CanonicalEnvData {
            version: profile.identity.version.into(),
            version_base: profile.identity.version_base.into(),
            build_time: profile.identity.build_time.into(),
            node_version: profile.identity.stainless_runtime_version.into(),
            ..Default::default()
        })
        .unwrap();
        crate::service::version_profile::apply_identity_to_env_json(
            &mut rollback.canonical_env,
            &profile.identity,
        );
        assert_eq!(hello_user_agent_for_account(&rollback), "Bun/1.4.0");
    }

    #[tokio::test]
    async fn first_request_sends_exact_anonymous_head_and_second_hits_cache() {
        async fn capture_headers(
            State(sender): State<mpsc::Sender<HeaderMap>>,
            headers: HeaderMap,
        ) -> StatusCode {
            sender.send(headers).await.expect("发送捕获 headers");
            StatusCode::OK
        }

        let (sender, mut receiver) = mpsc::channel(4);
        let app = Router::new()
            .route("/api/hello", head(capture_headers))
            .with_state(sender);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试端口");
        let address = listener.local_addr().expect("读取测试地址");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("运行测试服务");
        });

        let service = SessionHelloProbeService::with_endpoint(
            Arc::new(MemoryStore::new()),
            format!("http://{}/api/hello", address),
        );
        let config = SessionHelloProbeConfig {
            enabled: true,
            strict: true,
            timeout: Duration::from_secs(1),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };
        let account = test_account();

        assert_eq!(
            service
                .ensure_ready(&account, "session-a", "session-a", config)
                .await,
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(
            service
                .ensure_ready(&account, "session-a", "session-a", config)
                .await,
            SessionHelloProbeDecision::Proceed
        );

        let headers = receiver.recv().await.expect("收到探测请求");
        assert_eq!(
            headers.get(USER_AGENT).unwrap(),
            hello_user_agent_for_account(&account)
        );
        assert_eq!(headers.get(ACCEPT).unwrap(), "*/*");
        assert_eq!(headers.get(ACCEPT_ENCODING).unwrap(), HELLO_ACCEPT_ENCODING);
        assert_eq!(headers.get(CONNECTION).unwrap(), "keep-alive");
        assert!(headers.get("authorization").is_none());
        assert!(headers.get("cookie").is_none());
        assert!(headers.get("x-anthropic-billing-header").is_none());
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn concurrent_first_requests_share_one_network_probe() {
        async fn delayed_ok(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(80)).await;
            StatusCode::OK
        }

        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/api/hello", head(delayed_ok))
            .with_state(count.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试端口");
        let address = listener.local_addr().expect("读取测试地址");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("运行测试服务");
        });

        let service = Arc::new(SessionHelloProbeService::with_endpoint(
            Arc::new(MemoryStore::new()),
            format!("http://{}/api/hello", address),
        ));
        let config = SessionHelloProbeConfig {
            enabled: true,
            strict: true,
            timeout: Duration::from_secs(1),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };
        let account = Arc::new(test_account());
        let first = {
            let service = service.clone();
            let account = account.clone();
            tokio::spawn(async move {
                service
                    .ensure_ready(&account, "session-a", "session-a", config)
                    .await
            })
        };
        let second = {
            let service = service.clone();
            let account = account.clone();
            tokio::spawn(async move {
                service
                    .ensure_ready(&account, "session-a", "session-a", config)
                    .await
            })
        };

        assert_eq!(
            first.await.expect("首请求完成"),
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(
            second.await.expect("并发请求完成"),
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn write_failure_does_not_trigger_follower_probe_and_recovers_after_lock_expiry() {
        async fn delayed_ok(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(80)).await;
            StatusCode::OK
        }

        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/api/hello", head(delayed_ok))
            .with_state(count.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试端口");
        let address = listener.local_addr().expect("读取测试地址");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("运行测试服务");
        });

        let service = Arc::new(SessionHelloProbeService::with_endpoint(
            Arc::new(FaultInjectingProbeCache::with_write_failures(1)),
            format!("http://{}/api/hello", address),
        ));
        let config = SessionHelloProbeConfig {
            enabled: true,
            strict: false,
            timeout: Duration::from_millis(100),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };
        let account = Arc::new(test_account());
        let first = {
            let service = service.clone();
            let account = account.clone();
            tokio::spawn(async move {
                service
                    .ensure_ready(&account, "session-write", "session-write", config)
                    .await
            })
        };
        let second = {
            let service = service.clone();
            let account = account.clone();
            tokio::spawn(async move {
                service
                    .ensure_ready(&account, "session-write", "session-write", config)
                    .await
            })
        };

        assert_eq!(
            first.await.expect("leader 请求完成"),
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(
            second.await.expect("follower 请求完成"),
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);

        assert_eq!(
            service
                .ensure_ready(&account, "session-write", "session-write", config)
                .await,
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(
            service
                .ensure_ready(&account, "session-write", "session-write", config)
                .await,
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failure_cooldown_is_reused_when_switching_to_strict_mode() {
        async fn unavailable(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            StatusCode::SERVICE_UNAVAILABLE
        }

        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/api/hello", head(unavailable))
            .with_state(count.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试端口");
        let address = listener.local_addr().expect("读取测试地址");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("运行测试服务");
        });

        let service = SessionHelloProbeService::with_endpoint(
            Arc::new(MemoryStore::new()),
            format!("http://{}/api/hello", address),
        );
        let mut config = SessionHelloProbeConfig {
            enabled: true,
            strict: false,
            timeout: Duration::from_secs(1),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };
        let account = test_account();

        assert_eq!(
            service
                .ensure_ready(&account, "session-a", "session-a", config)
                .await,
            SessionHelloProbeDecision::Proceed
        );
        config.strict = true;
        assert_eq!(
            service
                .ensure_ready(&account, "session-a", "session-a", config)
                .await,
            SessionHelloProbeDecision::BlockFailure
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn invalid_configured_proxy_fails_without_direct_fallback() {
        async fn direct_endpoint(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            StatusCode::OK
        }

        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/api/hello", head(direct_endpoint))
            .with_state(count.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试端口");
        let address = listener.local_addr().expect("读取测试地址");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("运行测试服务");
        });

        let service = SessionHelloProbeService::with_endpoint(
            Arc::new(MemoryStore::new()),
            format!("http://{}/api/hello", address),
        );
        let mut account = test_account();
        account.proxy_url = "://invalid-proxy".into();
        let config = SessionHelloProbeConfig {
            enabled: true,
            strict: true,
            timeout: Duration::from_secs(1),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };

        assert_eq!(
            service
                .ensure_ready(&account, "session-a", "session-a", config)
                .await,
            SessionHelloProbeDecision::BlockFailure
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn configured_proxy_carries_the_hello_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试代理端口");
        let address = listener.local_addr().expect("读取测试代理地址");
        let (sender, mut receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("接受代理连接");
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).await.expect("读取代理请求");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let request_text = String::from_utf8(request).expect("代理请求为 UTF-8");
            let first_line = request_text.lines().next().unwrap_or_default().to_string();
            sender.send(first_line).await.expect("发送代理请求行");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .expect("返回代理响应");
        });

        let service = SessionHelloProbeService::with_endpoint(
            Arc::new(MemoryStore::new()),
            "http://probe.invalid/api/hello".into(),
        );
        let mut account = test_account();
        account.proxy_url = format!("http://{}", address);
        let config = SessionHelloProbeConfig {
            enabled: true,
            strict: true,
            timeout: Duration::from_secs(1),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };

        assert_eq!(
            service
                .ensure_ready(&account, "session-proxy", "session-proxy", config)
                .await,
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(
            receiver.recv().await.as_deref(),
            Some("HEAD http://probe.invalid/api/hello HTTP/1.1")
        );
    }

    #[tokio::test]
    async fn timeout_is_cached_and_reused_in_strict_mode() {
        async fn delayed(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(150)).await;
            StatusCode::OK
        }

        let count = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/api/hello", head(delayed))
            .with_state(count.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试端口");
        let address = listener.local_addr().expect("读取测试地址");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("运行测试服务");
        });
        let service = SessionHelloProbeService::with_endpoint(
            Arc::new(MemoryStore::new()),
            format!("http://{}/api/hello", address),
        );
        let config = SessionHelloProbeConfig {
            enabled: true,
            strict: true,
            timeout: Duration::from_millis(30),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };
        let account = test_account();

        assert_eq!(
            service
                .ensure_ready(&account, "session-timeout", "session-timeout", config)
                .await,
            SessionHelloProbeDecision::BlockTimeout
        );
        assert_eq!(
            service
                .ensure_ready(&account, "session-timeout", "session-timeout", config)
                .await,
            SessionHelloProbeDecision::BlockTimeout
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cache_failure_opens_or_blocks_according_to_strict_mode() {
        let service = SessionHelloProbeService::with_endpoint(
            Arc::new(FaultInjectingProbeCache::with_read_failure()),
            "http://127.0.0.1:9/api/hello".into(),
        );
        let account = test_account();
        let mut config = SessionHelloProbeConfig {
            enabled: true,
            strict: false,
            timeout: Duration::from_secs(1),
            success_ttl: Duration::from_secs(60),
            failure_cooldown: Duration::from_secs(10),
        };

        assert_eq!(
            service
                .ensure_ready(&account, "session-cache", "session-cache", config)
                .await,
            SessionHelloProbeDecision::Proceed
        );
        config.strict = true;
        assert_eq!(
            service
                .ensure_ready(&account, "session-cache", "session-cache", config)
                .await,
            SessionHelloProbeDecision::BlockUnavailable
        );
    }

    #[tokio::test]
    async fn redis_instances_share_one_network_probe_when_available() {
        let Ok(port) = std::env::var("CC2API_TEST_REDIS_PORT") else {
            return;
        };
        let port = port.parse::<u16>().expect("解析测试 Redis 端口");
        let first_store = Arc::new(
            RedisStore::new("127.0.0.1", port, "", 15)
                .await
                .expect("连接首个 RedisStore"),
        );
        let second_store = Arc::new(
            RedisStore::new("127.0.0.1", port, "", 15)
                .await
                .expect("连接第二个 RedisStore"),
        );
        let count = Arc::new(AtomicUsize::new(0));

        async fn delayed_ok(State(count): State<Arc<AtomicUsize>>) -> StatusCode {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            StatusCode::OK
        }

        let app = Router::new()
            .route("/api/hello", head(delayed_ok))
            .with_state(count.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定测试端口");
        let address = listener.local_addr().expect("读取测试地址");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("运行测试服务");
        });
        let endpoint = format!("http://{}/api/hello", address);
        let first_service = Arc::new(SessionHelloProbeService::with_endpoint(
            first_store,
            endpoint.clone(),
        ));
        let second_service = Arc::new(SessionHelloProbeService::with_endpoint(
            second_store,
            endpoint,
        ));
        let account = Arc::new(test_account());
        let session = format!("session-redis-{}", Uuid::new_v4());
        let config = SessionHelloProbeConfig {
            enabled: true,
            strict: true,
            timeout: Duration::from_secs(1),
            success_ttl: Duration::from_secs(2),
            failure_cooldown: Duration::from_secs(1),
        };
        let first = {
            let service = first_service.clone();
            let account = account.clone();
            let session = session.clone();
            tokio::spawn(async move {
                service
                    .ensure_ready(&account, &session, &session, config)
                    .await
            })
        };
        let second = {
            let service = second_service.clone();
            let account = account.clone();
            let session = session.clone();
            tokio::spawn(async move {
                service
                    .ensure_ready(&account, &session, &session, config)
                    .await
            })
        };

        assert_eq!(
            first.await.expect("首实例请求完成"),
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(
            second.await.expect("第二实例请求完成"),
            SessionHelloProbeDecision::Proceed
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn state_key_changes_with_account_upstream_session_and_proxy_without_leaking_values() {
        let first = probe_state_key(1, "secret-upstream", "http://proxy-a:8080");
        let second_account = probe_state_key(2, "secret-upstream", "http://proxy-a:8080");
        let second_session = probe_state_key(1, "other-upstream", "http://proxy-a:8080");
        let second_proxy = probe_state_key(1, "secret-upstream", "http://proxy-b:8080");

        assert_ne!(first, second_account);
        assert_ne!(first, second_session);
        assert_ne!(first, second_proxy);
        assert!(!first.contains("secret-upstream"));
        assert!(!first.contains("proxy-a"));
    }

    #[test]
    fn config_rejects_values_outside_supported_ranges() {
        assert!(SessionHelloProbeConfig::parse("false", "false", "5", "3600", "300").is_ok());
        assert!(SessionHelloProbeConfig::parse("true", "false", "0", "3600", "300").is_err());
        assert!(SessionHelloProbeConfig::parse("true", "false", "5", "59", "300").is_err());
        assert!(SessionHelloProbeConfig::parse("true", "false", "5", "3600", "9").is_err());
    }
}
