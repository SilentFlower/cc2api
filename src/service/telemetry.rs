use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use chrono::Utc;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::model::account::Account;
use crate::model::identity::{
    build_full_env_json, device_profile, process_snapshot, process_snapshot_json, run_profile,
    RunProfile,
};
use crate::service::account::AccountService;
use crate::service::version_profile::{
    claude_code_user_agent, growthbook_user_agent, is_event_logging_path, normalize_version,
    EVENT_LOGGING_V2_PATH, OAUTH_BETA_TOKEN,
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
    run_profile: RunProfile,
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
        let started_at_utc = Utc::now();
        let session = TelemetrySession {
            account: account.clone(),
            token,
            started_at: now,
            run_profile: run_profile(account, started_at_utc),
            expires_at: now + SESSION_TTL,
            expires_at_utc: started_at_utc + chrono::Duration::from_std(SESSION_TTL).unwrap(),
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
            let payload = build_event_batch(&session.account, &session.run_profile, uptime_secs);
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
            let payload = build_growthbook_eval(&session.account, &session.run_profile);
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
        .map(|a| device_profile(&a).env.version)
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

/// JS Date.toISOString() 兼容格式：毫秒精度 + Z 后缀。
fn js_iso_timestamp() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// 构造 /api/event_logging/v2/batch 请求体。
fn build_event_batch(
    account: &Account,
    run_profile: &RunProfile,
    uptime_secs: f64,
) -> serde_json::Value {
    let profile = device_profile(account);
    let process_b64 = {
        let snapshot = process_snapshot(&profile.process, &profile.device_id, uptime_secs);
        let p = process_snapshot_json(&snapshot);
        let bytes = serde_json::to_vec(&p).unwrap_or_default();
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    };

    let env_obj = build_full_env_json(&profile.env);

    let mut auth = json!({});
    auth["account_uuid"] = json!(profile.account_uuid);
    if let Some(ref org) = profile.organization_uuid {
        auth["organization_uuid"] = json!(org);
    }

    let event = json!({
        "event_type": "ClaudeCodeInternalEvent",
        "event_data": {
            "event_id": uuid::Uuid::new_v4().to_string(),
            "event_name": "tengu_api_success",
            "client_timestamp": js_iso_timestamp(),
            "device_id": profile.device_id,
            "email": profile.email,
            "session_id": run_profile.session_id,
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
fn build_growthbook_eval(account: &Account, run_profile: &RunProfile) -> serde_json::Value {
    let profile = device_profile(account);
    let mut attrs = json!({
        "id": profile.device_id,
        "sessionId": run_profile.growthbook_session_id,
        "deviceID": profile.device_id,
        "platform": profile.env.platform,
        "appVersion": profile.env.version,
        "email": profile.email,
        "accountUUID": profile.account_uuid,
        "userType": "external",
        "rateLimitTier": rate_limit_tier(&profile.subscription_type),
        "entrypoint": "cli",
    });

    if let Some(ref org) = profile.organization_uuid {
        attrs["organizationUUID"] = json!(org);
    }
    if let Some(ref sub) = profile.subscription_type {
        attrs["subscriptionType"] = json!(sub);
    }

    json!({
        "attributes": attrs,
        "forcedFeatures": {},
    })
}

/// 根据订阅类型生成 GrowthBook rateLimitTier。
fn rate_limit_tier(subscription_type: &Option<String>) -> String {
    match subscription_type.as_deref() {
        Some("max") => "default_claude_max_20x".to_string(),
        Some("pro") => "default_claude_pro".to_string(),
        Some(other) if !other.is_empty() => format!("default_claude_{}", other),
        _ => "default".to_string(),
    }
}

/// 构造 /api/claude_code/metrics 请求体。
fn build_metrics(account: &Account) -> serde_json::Value {
    let profile = device_profile(account);
    let os_type = match profile.env.platform.as_str() {
        "darwin" => "Darwin",
        "win32" => "Windows",
        _ => "Linux",
    };

    let mut resource = json!({
        "service.name": "claude-code",
        "service.version": profile.env.version,
        "os.type": os_type,
        "host.arch": profile.env.arch,
        "aggregation.temporality": "delta",
        "user.customer_type": "claude_ai",
    });

    if let Some(ref sub) = profile.subscription_type {
        resource["user.subscription_type"] = json!(sub);
    }

    json!({
        "resource_attributes": resource,
        "metrics": [],
    })
}

#[cfg(test)]
mod tests {
    use super::{build_event_batch, build_growthbook_eval, build_metrics, is_telemetry_path};
    use crate::model::account::{
        Account, AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData,
        CanonicalProcessData, CanonicalPromptEnvData,
    };
    use crate::model::identity::run_profile;
    use crate::service::version_profile::{
        DEFAULT_CLAUDE_CODE_BUILD_TIME, DEFAULT_CLAUDE_CODE_VERSION,
        DEFAULT_CLAUDE_CODE_VERSION_BASE,
    };
    use base64::Engine;
    use chrono::{TimeZone, Utc};
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
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap(),
        );
        let payload = build_growthbook_eval(&account, &run);
        let attrs = payload.get("attributes").unwrap();
        assert_eq!(attrs.get("id").unwrap(), "device-1");
        assert_eq!(attrs.get("deviceID").unwrap(), "device-1");
        assert_eq!(
            attrs.get("sessionId").unwrap(),
            &json!(run.growthbook_session_id)
        );
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

    #[test]
    fn event_batch_uses_unified_profile_and_bounded_process() {
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap(),
        );
        let payload = build_event_batch(&account, &run, 12.5);
        let event = &payload["events"][0]["event_data"];
        assert_eq!(event["device_id"], "device-1");
        assert_eq!(event["email"], "user@example.com");
        assert_eq!(event["session_id"], run.session_id);
        assert_eq!(event["auth"]["account_uuid"], "account-uuid");
        assert_eq!(event["auth"]["organization_uuid"], "org-uuid");
        assert_eq!(event["env"]["version"], DEFAULT_CLAUDE_CODE_VERSION);
        assert_eq!(event["env"]["linux_distro_id"], "ubuntu");

        let process_b64 = event["process"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(process_b64)
            .unwrap();
        let process: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
        assert!(process["heapUsed"].as_i64().unwrap() <= process["heapTotal"].as_i64().unwrap());
        assert_eq!(process["constrainedMemory"], 0);
    }

    #[test]
    fn metrics_uses_device_profile_env_and_subscription() {
        let payload = build_metrics(&test_account());
        let resource = payload.get("resource_attributes").unwrap();
        assert_eq!(resource["service.version"], DEFAULT_CLAUDE_CODE_VERSION);
        assert_eq!(resource["os.type"], "Linux");
        assert_eq!(resource["host.arch"], "x64");
        assert_eq!(resource["user.subscription_type"], "max");
    }
}
