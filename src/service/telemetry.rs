use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use chrono::Utc;
use rand::Rng;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::model::account::{Account, CanonicalEnvData, CanonicalProcessData};
use crate::service::account::AccountService;
use crate::service::version_profile::{
    EVENT_LOGGING_V2_PATH, OAUTH_BETA_TOKEN, claude_code_user_agent, growthbook_user_agent,
    is_event_logging_path, normalize_version,
};
use crate::store::account_store::AccountStore;

// ---------------------------------------------------------------------------
// 常量
// ---------------------------------------------------------------------------

const SESSION_TTL: Duration = Duration::from_secs(10 * 60);
const EVENT_BATCH_INTERVAL: Duration = Duration::from_secs(10);
const GROWTHBOOK_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
const METRICS_INTERVAL: Duration = Duration::from_secs(60);
const TICK_INTERVAL: Duration = Duration::from_secs(1);

const UPSTREAM_BASE: &str = "https://api.anthropic.com";
const GROWTHBOOK_CLIENT_KEY: &str = "sdk-zAZezfDKGoZuXXKe";

// ---------------------------------------------------------------------------
// 遥测路径判断
// ---------------------------------------------------------------------------

/// 判断请求路径是否为遥测端点。
pub fn is_telemetry_path(path: &str) -> bool {
    is_event_logging_path(path)
        || path.starts_with("/api/eval/")
        || path.starts_with("/api/claude_code/metrics")
        || path.starts_with("/api/claude_code/organizations/metrics_enabled")
}

/// 针对 metrics_enabled 返回固定 JSON 响应。
pub fn fake_metrics_enabled_response() -> serde_json::Value {
    json!({"metrics_logging_enabled": true})
}

/// 针对其他遥测端点返回空成功响应。
pub fn fake_telemetry_response() -> serde_json::Value {
    json!({})
}

// ---------------------------------------------------------------------------
// 会话状态
// ---------------------------------------------------------------------------

struct TelemetrySession {
    account: Account,
    token: String,
    started_at: Instant,
    expires_at: Instant,
    expires_at_utc: chrono::DateTime<Utc>,
    last_event_batch_at: Instant,
    last_growthbook_at: Option<Instant>,
    last_metrics_at: Instant,
    send_count: i64,
    running: bool,
}

// ---------------------------------------------------------------------------
// TelemetryService
// ---------------------------------------------------------------------------

/// 管理自动遥测会话的后台服务。
pub struct TelemetryService {
    sessions: Arc<Mutex<HashMap<i64, TelemetrySession>>>,
    account_store: Arc<AccountStore>,
    account_svc: Arc<AccountService>,
}

impl TelemetryService {
    pub fn new(account_store: Arc<AccountStore>, account_svc: Arc<AccountService>) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            account_store,
            account_svc,
        }
    }

    /// 查询账号的遥测会话过期时间。
    pub async fn get_session_expires_at(&self, account_id: i64) -> Option<chrono::DateTime<Utc>> {
        let sessions = self.sessions.lock().await;
        sessions.get(&account_id).map(|s| s.expires_at_utc)
    }

    /// 当 /v1/messages 请求到来时调用，激活或续期遥测会话。
    pub async fn activate_session(&self, account: &Account) {
        if !account.auto_telemetry {
            return;
        }

        let token = match self.account_svc.resolve_upstream_token(account.id).await {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    "telemetry: cannot resolve token for account {}: {}",
                    account.id, e
                );
                return;
            }
        };

        let mut sessions = self.sessions.lock().await;
        let now = Instant::now();

        if let Some(session) = sessions.get_mut(&account.id) {
            // 续期
            session.expires_at = now + SESSION_TTL;
            session.expires_at_utc = Utc::now() + chrono::Duration::from_std(SESSION_TTL).unwrap();
            session.token = token;
            session.account = account.clone();
            debug!("telemetry: renewed session for account {}", account.id);
            return;
        }

        // 新建会话
        info!("telemetry: starting session for account {}", account.id);
        let session = TelemetrySession {
            account: account.clone(),
            token,
            started_at: now,
            expires_at: now + SESSION_TTL,
            expires_at_utc: Utc::now() + chrono::Duration::from_std(SESSION_TTL).unwrap(),
            last_event_batch_at: now - EVENT_BATCH_INTERVAL, // 立即触发首次
            last_growthbook_at: None,
            last_metrics_at: now - METRICS_INTERVAL,
            send_count: 0,
            running: true,
        };
        sessions.insert(account.id, session);

        // 启动后台任务
        let sessions_ref = self.sessions.clone();
        let store_ref = self.account_store.clone();
        let account_id = account.id;
        let proxy_url = account.proxy_url.clone();

        tokio::spawn(async move {
            telemetry_loop(sessions_ref, store_ref, account_id, proxy_url).await;
        });
    }
}

// ---------------------------------------------------------------------------
// 后台循环
// ---------------------------------------------------------------------------

async fn telemetry_loop(
    sessions: Arc<Mutex<HashMap<i64, TelemetrySession>>>,
    store: Arc<AccountStore>,
    account_id: i64,
    proxy_url: String,
) {
    let client = crate::tlsfp::make_request_client(&proxy_url);

    loop {
        tokio::time::sleep(TICK_INTERVAL).await;

        let mut map = sessions.lock().await;
        let session = match map.get_mut(&account_id) {
            Some(s) => s,
            None => break,
        };

        // TTL 过期 → 持久化计数并退出
        if Instant::now() >= session.expires_at {
            let count = session.send_count;
            session.running = false;
            map.remove(&account_id);
            drop(map);
            if count > 0 {
                let _ = store.increment_telemetry_count(account_id, count).await;
            }
            info!(
                "telemetry: session expired for account {}, sent {} requests",
                account_id, count
            );
            break;
        }

        let now = Instant::now();

        // --- event_logging/v2/batch ---
        if now.duration_since(session.last_event_batch_at) >= EVENT_BATCH_INTERVAL {
            let uptime_secs = now.duration_since(session.started_at).as_secs_f64();
            let payload = build_event_batch(&session.account, uptime_secs);
            let token = session.token.clone();
            let c = client.clone();
            session.last_event_batch_at = now;
            session.send_count += 1;
            drop(map);

            send_telemetry(
                &c,
                &format!("{}{}", UPSTREAM_BASE, EVENT_LOGGING_V2_PATH),
                &token,
                &payload,
                &session_ua(&store, account_id).await,
                true,
            )
            .await;

            let _ = store.increment_telemetry_count(account_id, 1).await;
            continue;
        }

        // --- GrowthBook eval ---
        let should_gb = match session.last_growthbook_at {
            None => true,
            Some(t) => now.duration_since(t) >= GROWTHBOOK_INTERVAL,
        };
        if should_gb {
            let payload = build_growthbook_eval(&session.account);
            let token = session.token.clone();
            let c = client.clone();
            session.last_growthbook_at = Some(now);
            session.send_count += 1;
            drop(map);

            send_telemetry(
                &c,
                &format!("{}/api/eval/{}", UPSTREAM_BASE, GROWTHBOOK_CLIENT_KEY),
                &token,
                &payload,
                growthbook_user_agent(),
                false,
            )
            .await;

            let _ = store.increment_telemetry_count(account_id, 1).await;
            continue;
        }

        // --- metrics (跳过：该端点不支持 OAuth 认证) ---

        drop(map);
    }
}

/// 从 account store 获取最新的 UA 版本号。
async fn session_ua(store: &Arc<AccountStore>, account_id: i64) -> String {
    let version = store
        .get_by_id(account_id)
        .await
        .ok()
        .and_then(|a| serde_json::from_value::<CanonicalEnvData>(a.canonical_env).ok())
        .map(|e| e.version)
        .unwrap_or_default();
    claude_code_user_agent(normalize_version(&version))
}

// ---------------------------------------------------------------------------
// HTTP 发送
// ---------------------------------------------------------------------------

async fn send_telemetry(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    body: &serde_json::Value,
    user_agent: &str,
    service_header: bool,
) {
    let mut req = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("User-Agent", user_agent)
        .header("anthropic-beta", OAUTH_BETA_TOKEN)
        .header("Authorization", format!("Bearer {}", token))
        .json(body);

    if service_header {
        req = req.header("x-service-name", "claude-code");
    }

    let result = req.send().await;

    match result {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                debug!("telemetry: {} → {}", url, status);
            } else {
                let text = resp.text().await.unwrap_or_default();
                warn!("telemetry: {} → {} {}", url, status, text);
            }
        }
        Err(e) => {
            warn!("telemetry: {} failed: {}", url, e);
        }
    }
}

// ---------------------------------------------------------------------------
// 请求体构造
// ---------------------------------------------------------------------------

fn parse_env(account: &Account) -> CanonicalEnvData {
    serde_json::from_value(account.canonical_env.clone()).unwrap_or_default()
}

fn parse_process(account: &Account) -> CanonicalProcessData {
    serde_json::from_value(account.canonical_process.clone()).unwrap_or_default()
}

fn random_in_range(min: i64, max: i64) -> i64 {
    if max <= min {
        return min;
    }
    rand::thread_rng().gen_range(min..max)
}

fn build_process_json(proc: &CanonicalProcessData, uptime_secs: f64) -> serde_json::Value {
    let mut rng = rand::thread_rng();
    let cpu_user = rng.gen_range(50_000i64..500_000);
    let cpu_system = rng.gen_range(15_000i64..150_000);
    let cpu_percent = rng.gen_range(0.5f64..5.0);
    json!({
        "uptime": uptime_secs,
        "rss": random_in_range(proc.rss_range[0], proc.rss_range[1]),
        "heapTotal": random_in_range(proc.heap_total_range[0], proc.heap_total_range[1]),
        "heapUsed": random_in_range(proc.heap_used_range[0], proc.heap_used_range[1]),
        "external": random_in_range(proc.external_range[0], proc.external_range[1]),
        "arrayBuffers": random_in_range(proc.array_buffers_range[0], proc.array_buffers_range[1]),
        "constrainedMemory": proc.constrained_memory,
        "cpuUsage": { "user": cpu_user, "system": cpu_system },
        "cpuPercent": cpu_percent,
    })
}

fn derive_account_uuid(account: &Account) -> String {
    account.account_uuid.clone().unwrap_or_else(|| {
        use sha2::{Digest, Sha256};
        let seed = if account.email.is_empty() {
            format!("account-{}", account.id)
        } else {
            account.email.clone()
        };
        let hash = Sha256::digest(seed.as_bytes());
        format!(
            "{}-{}-{}-{}-{}",
            hex::encode(&hash[0..4]),
            hex::encode(&hash[4..6]),
            hex::encode(&hash[6..8]),
            hex::encode(&hash[8..10]),
            hex::encode(&hash[10..16])
        )
    })
}

/// JS Date.toISOString() 兼容格式：毫秒精度 + Z 后缀。
fn js_iso_timestamp() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// 构造 /api/event_logging/v2/batch 请求体。
fn build_event_batch(account: &Account, uptime_secs: f64) -> serde_json::Value {
    let env = parse_env(account);
    let proc = parse_process(account);
    let account_uuid = derive_account_uuid(account);
    let session_id = uuid::Uuid::new_v4().to_string();
    let process_b64 = {
        let p = build_process_json(&proc, uptime_secs);
        let bytes = serde_json::to_vec(&p).unwrap_or_default();
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    };

    let env_obj = crate::model::identity::build_full_env_json(&env);

    let mut auth = json!({});
    auth["account_uuid"] = json!(account_uuid);
    if let Some(ref org) = account.organization_uuid {
        auth["organization_uuid"] = json!(org);
    }

    let event = json!({
        "event_type": "ClaudeCodeInternalEvent",
        "event_data": {
            "event_id": uuid::Uuid::new_v4().to_string(),
            "event_name": "tengu_api_success",
            "client_timestamp": js_iso_timestamp(),
            "device_id": account.device_id,
            "email": account.email,
            "session_id": session_id,
            "model": "claude-sonnet-4-20250514",
            "user_type": "external",
            "is_interactive": true,
            "client_type": "cli",
            "entrypoint": "cli",
            "betas": "",
            "agent_sdk_version": "",
            "swe_bench_run_id": "",
            "swe_bench_instance_id": "",
            "swe_bench_task_id": "",
            "agent_id": "",
            "parent_session_id": "",
            "agent_type": "",
            "team_name": "",
            "skill_name": "",
            "plugin_name": "",
            "marketplace_name": "",
            "additional_metadata": "",
            "auth": auth,
            "env": env_obj,
            "process": process_b64,
        }
    });

    json!({ "events": [event] })
}

/// 构造 /api/eval/{clientKey} 请求体（GrowthBook remote eval）。
fn build_growthbook_eval(account: &Account) -> serde_json::Value {
    let env = parse_env(account);
    let account_uuid = derive_account_uuid(account);

    let session_id = uuid::Uuid::new_v4().to_string();
    let mut attrs = json!({
        "id": account.device_id,
        "sessionId": session_id,
        "deviceID": account.device_id,
        "platform": env.platform,
        "appVersion": env.version,
        "email": account.email,
        "accountUUID": account_uuid,
        "userType": "external",
        "rateLimitTier": rate_limit_tier(account),
        "entrypoint": "cli",
    });

    if let Some(ref org) = account.organization_uuid {
        attrs["organizationUUID"] = json!(org);
    }
    if let Some(ref sub) = account.subscription_type {
        attrs["subscriptionType"] = json!(sub);
    }

    json!({
        "attributes": attrs,
        "forcedFeatures": {},
    })
}

/// 根据订阅类型生成 GrowthBook rateLimitTier。
fn rate_limit_tier(account: &Account) -> String {
    match account.subscription_type.as_deref() {
        Some("max") => "default_claude_max_20x".to_string(),
        Some("pro") => "default_claude_pro".to_string(),
        Some(other) if !other.is_empty() => format!("default_claude_{}", other),
        _ => "default".to_string(),
    }
}

/// 构造 /api/claude_code/metrics 请求体。
fn build_metrics(account: &Account) -> serde_json::Value {
    let env = parse_env(account);
    let os_type = match env.platform.as_str() {
        "darwin" => "Darwin",
        "win32" => "Windows",
        _ => "Linux",
    };

    let mut resource = json!({
        "service.name": "claude-code",
        "service.version": env.version,
        "os.type": os_type,
        "host.arch": env.arch,
        "aggregation.temporality": "delta",
        "user.customer_type": "claude_ai",
    });

    if let Some(ref sub) = account.subscription_type {
        resource["user.subscription_type"] = json!(sub);
    }

    json!({
        "resource_attributes": resource,
        "metrics": [],
    })
}

#[cfg(test)]
mod tests {
    use super::{build_growthbook_eval, is_telemetry_path};
    use crate::model::account::{
        Account, AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData,
        CanonicalProcessData, CanonicalPromptEnvData,
    };
    use crate::service::version_profile::{
        DEFAULT_CLAUDE_CODE_BUILD_TIME, DEFAULT_CLAUDE_CODE_VERSION,
        DEFAULT_CLAUDE_CODE_VERSION_BASE,
    };
    use chrono::Utc;
    use serde_json::json;

    fn test_account() -> Account {
        let env = CanonicalEnvData {
            platform: "linux".into(),
            platform_raw: "linux".into(),
            arch: "x64".into(),
            node_version: "v24.3.0".into(),
            terminal: "ssh-session".into(),
            package_managers: "npm".into(),
            runtimes: "node".into(),
            is_claude_ai_auth: true,
            version: DEFAULT_CLAUDE_CODE_VERSION.into(),
            version_base: DEFAULT_CLAUDE_CODE_VERSION_BASE.into(),
            build_time: DEFAULT_CLAUDE_CODE_BUILD_TIME.into(),
            deployment_environment: "unknown-linux".into(),
            vcs: "git".into(),
            ..Default::default()
        };
        Account {
            id: 1,
            name: "测试账号".into(),
            email: "user@example.com".into(),
            status: AccountStatus::Active,
            auth_type: AccountAuthType::Oauth,
            setup_token: String::new(),
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_at: None,
            oauth_refreshed_at: None,
            auth_error: String::new(),
            proxy_url: String::new(),
            device_id: "device-1".into(),
            canonical_env: serde_json::to_value(env).unwrap(),
            canonical_prompt: serde_json::to_value(CanonicalPromptEnvData::default()).unwrap(),
            canonical_process: serde_json::to_value(CanonicalProcessData::default()).unwrap(),
            billing_mode: BillingMode::Strip,
            account_uuid: Some("account-uuid".into()),
            organization_uuid: Some("org-uuid".into()),
            subscription_type: Some("max".into()),
            concurrency: 3,
            priority: 50,
            rate_limited_at: None,
            rate_limit_reset_at: None,
            disable_reason: String::new(),
            auto_telemetry: true,
            auto_poll_usage: false,
            allow_1m_models: "opus".into(),
            telemetry_count: 0,
            usage_data: json!({}),
            usage_fetched_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn telemetry_path_accepts_v2_and_legacy() {
        assert!(is_telemetry_path("/api/event_logging/v2/batch"));
        assert!(is_telemetry_path("/api/event_logging/batch"));
        assert!(is_telemetry_path("/api/eval/sdk-zAZezfDKGoZuXXKe"));
    }

    #[test]
    fn growthbook_eval_contains_2156_attributes() {
        let payload = build_growthbook_eval(&test_account());
        let attrs = payload.get("attributes").unwrap();
        assert_eq!(
            attrs.get("appVersion").unwrap(),
            DEFAULT_CLAUDE_CODE_VERSION
        );
        assert_eq!(attrs.get("userType").unwrap(), "external");
        assert_eq!(
            attrs.get("rateLimitTier").unwrap(),
            "default_claude_max_20x"
        );
        assert_eq!(attrs.get("entrypoint").unwrap(), "cli");
        assert_eq!(attrs.get("organizationUUID").unwrap(), "org-uuid");
    }
}
