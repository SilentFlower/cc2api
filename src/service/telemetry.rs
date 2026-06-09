use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use chrono::Utc;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::model::account::Account;
use crate::model::identity::{
    RunProfile, build_full_env_json, device_profile, process_snapshot, process_snapshot_json,
    run_profile,
};
use crate::service::account::AccountService;
use crate::service::rewriter::ordered_anthropic_headers;
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
const EVENT_BATCH_MAX: usize = 25;
const EVENT_QUEUE_MAX: usize = 200;
const SLOW_FIRST_BYTE_MS: u64 = 10_000;

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
    pending_events: VecDeque<TelemetryEvent>,
    expires_at: Instant,
    expires_at_utc: chrono::DateTime<Utc>,
    last_event_batch_at: Instant,
    last_growthbook_at: Option<Instant>,
    last_metrics_at: Instant,
    send_count: i64,
    running: bool,
}

/// 表示 `/v1/messages` 请求阶段的安全遥测摘要。
///
/// 该结构只保存字段计数、长度、状态等安全信息，不包含 prompt、tool input、响应正文或 token。
#[derive(Debug, Clone)]
pub struct MessageTelemetryContext {
    /// 请求声明的模型名称。为空时事件构造会回退到默认 Claude Code 模型。
    pub model: String,
    /// 请求级 session id，只来自 metadata 派生字段，不来自正文原文。
    pub session_id: Option<String>,
    /// 原始请求体字节数。
    pub request_body_bytes: usize,
    /// 改写后请求体字节数。
    pub rewritten_body_bytes: usize,
    /// 是否为流式请求。
    pub stream: bool,
    /// 请求声明的工具数量。
    pub tool_count: usize,
    /// 请求中附件或文件块数量。
    pub attachment_count: usize,
    /// 改写后 system prompt 文本块数量。
    pub system_prompt_block_count: usize,
    /// 请求来源类型，用于区分 Claude Code 客户端和纯 API 客户端。
    pub client_type: String,
    /// 当前网关重试序号。
    pub attempt: usize,
}

/// 表示 `/v1/messages` 上游响应阶段的安全遥测摘要。
#[derive(Debug, Clone)]
pub struct MessageTelemetryResult {
    /// 上游 HTTP 状态码。网络错误时为空。
    pub status_code: Option<u16>,
    /// 从发送上游请求到收到响应头或错误的耗时。
    pub duration_ms: u64,
    /// 上游请求失败类型。成功响应为空。
    pub error_kind: Option<String>,
}

#[derive(Debug, Clone)]
struct TelemetryEvent {
    event_type: TelemetryEventType,
    name: Option<String>,
    model: String,
    session_id: Option<String>,
    fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TelemetryEventType {
    Internal,
    GrowthbookExperiment,
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
            pending_events: startup_events(account),
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

    /// 将 `/v1/messages` 请求改写前后的安全摘要写入遥测事件队列。
    ///
    /// `context` 只能包含字段数量、字节长度、session id 等派生信息，不能传入请求体原文或 token。
    pub async fn record_message_request(
        &self,
        account: &Account,
        context: MessageTelemetryContext,
    ) {
        if !account.auto_telemetry {
            return;
        }
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get_mut(&account.id) {
            session.expires_at = Instant::now() + SESSION_TTL;
            session.expires_at_utc = Utc::now() + chrono::Duration::from_std(SESSION_TTL).unwrap();
            enqueue_events(
                &mut session.pending_events,
                message_request_events(&context),
            );
        }
    }

    /// 将 `/v1/messages` 上游响应或错误摘要写入遥测事件队列。
    ///
    /// `result` 只描述状态码、耗时和错误类别，用于生成 success、slow first byte 或 error 事件。
    pub async fn record_message_result(
        &self,
        account: &Account,
        context: MessageTelemetryContext,
        result: MessageTelemetryResult,
    ) {
        if !account.auto_telemetry {
            return;
        }
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get_mut(&account.id) {
            session.expires_at = Instant::now() + SESSION_TTL;
            session.expires_at_utc = Utc::now() + chrono::Duration::from_std(SESSION_TTL).unwrap();
            enqueue_events(
                &mut session.pending_events,
                message_result_events(&context, &result),
            );
        }
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
    let client = crate::tlsfp::get_request_client(&proxy_url);

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
        if !session.pending_events.is_empty()
            && now.duration_since(session.last_event_batch_at) >= EVENT_BATCH_INTERVAL
        {
            let uptime_secs = now.duration_since(session.started_at).as_secs_f64();
            let mut events = Vec::new();
            while events.len() < EVENT_BATCH_MAX {
                match session.pending_events.pop_front() {
                    Some(event) => events.push(event),
                    None => break,
                }
            }
            let payload =
                build_event_batch(&session.account, &session.run_profile, uptime_secs, &events);
            let token = session.token.clone();
            let c = client.clone();
            session.last_event_batch_at = now;
            session.send_count += 1;
            let event_count = events.len();
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
            debug!(
                "telemetry: sent {} queued events for account {}",
                event_count, account_id
            );
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
    let path = telemetry_path_from_url(url);
    let headers = telemetry_request_headers(path, token, user_agent, service_header);

    let mut req = client.post(url);
    for (name, value) in ordered_anthropic_headers(path, &headers) {
        req = req.header(name, value);
    }
    let req = req.json(body);

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

fn telemetry_path_from_url(url: &str) -> &str {
    url.strip_prefix(UPSTREAM_BASE).unwrap_or(url)
}

fn telemetry_request_headers(
    path: &str,
    token: &str,
    user_agent: &str,
    service_header: bool,
) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    headers.insert("User-Agent".to_string(), user_agent.to_string());
    headers.insert("anthropic-beta".to_string(), OAUTH_BETA_TOKEN.to_string());
    headers.insert("Authorization".to_string(), format!("Bearer {}", token));

    if path.starts_with("/api/eval/") {
        headers.insert("Accept".to_string(), "*/*".to_string());
        headers.insert(
            "accept-encoding".to_string(),
            "gzip, deflate, br, zstd".to_string(),
        );
    } else if is_event_logging_path(path) {
        headers.insert(
            "Accept".to_string(),
            "application/json, text/plain, */*".to_string(),
        );
        headers.insert(
            "accept-encoding".to_string(),
            "gzip, compress, deflate, br".to_string(),
        );
    }

    if service_header {
        headers.insert("x-service-name".to_string(), "claude-code".to_string());
    }

    headers
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
    events: &[TelemetryEvent],
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

    let events: Vec<serde_json::Value> = events
        .iter()
        .map(|event| build_event_json(event, run_profile, &profile, &env_obj, &auth, &process_b64))
        .collect();

    json!({ "events": events })
}

fn build_event_json(
    event: &TelemetryEvent,
    run_profile: &RunProfile,
    profile: &crate::model::identity::DeviceProfile,
    env_obj: &serde_json::Value,
    auth: &serde_json::Value,
    process_b64: &str,
) -> serde_json::Value {
    match event.event_type {
        TelemetryEventType::Internal => {
            let mut event_data = base_internal_event_data(
                event.name.as_deref().unwrap_or("tengu_feature_ok"),
                event,
                run_profile,
                profile,
                env_obj,
                auth,
                process_b64,
            );
            for (key, value) in &event.fields {
                event_data.insert(key.clone(), value.clone());
            }
            json!({
                "event_type": "ClaudeCodeInternalEvent",
                "event_data": event_data,
            })
        }
        TelemetryEventType::GrowthbookExperiment => {
            let mut event_data = serde_json::Map::new();
            event_data.insert("event_id".into(), json!(uuid::Uuid::new_v4().to_string()));
            event_data.insert("timestamp".into(), json!(js_iso_timestamp()));
            event_data.insert("experiment_id".into(), json!("claude_code_remote_eval"));
            event_data.insert("variation_id".into(), json!(0));
            event_data.insert("environment".into(), json!("production"));
            event_data.insert("user_attributes".into(), json!("{}"));
            event_data.insert("experiment_metadata".into(), json!("{}"));
            event_data.insert("device_id".into(), json!(profile.device_id));
            event_data.insert("auth".into(), auth.clone());
            event_data.insert(
                "session_id".into(),
                json!(
                    event
                        .session_id
                        .clone()
                        .unwrap_or_else(|| run_profile.growthbook_session_id.clone())
                ),
            );
            for (key, value) in &event.fields {
                event_data.insert(key.clone(), value.clone());
            }
            json!({
                "event_type": "GrowthbookExperimentEvent",
                "event_data": event_data,
            })
        }
    }
}

fn base_internal_event_data(
    event_name: &str,
    event: &TelemetryEvent,
    run_profile: &RunProfile,
    profile: &crate::model::identity::DeviceProfile,
    env_obj: &serde_json::Value,
    auth: &serde_json::Value,
    process_b64: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut event_data = serde_json::Map::new();
    event_data.insert("event_id".into(), json!(uuid::Uuid::new_v4().to_string()));
    event_data.insert("event_name".into(), json!(event_name));
    event_data.insert("client_timestamp".into(), json!(js_iso_timestamp()));
    event_data.insert("device_id".into(), json!(profile.device_id));
    event_data.insert("email".into(), json!(profile.email));
    event_data.insert(
        "session_id".into(),
        json!(
            event
                .session_id
                .clone()
                .unwrap_or_else(|| run_profile.session_id.clone())
        ),
    );
    event_data.insert("model".into(), json!(event.model));
    event_data.insert("user_type".into(), json!("external"));
    event_data.insert("is_interactive".into(), json!(true));
    event_data.insert("client_type".into(), json!("cli"));
    event_data.insert("entrypoint".into(), json!("cli"));
    event_data.insert("betas".into(), json!(""));
    event_data.insert("agent_sdk_version".into(), json!(""));
    event_data.insert("swe_bench_run_id".into(), json!(""));
    event_data.insert("swe_bench_instance_id".into(), json!(""));
    event_data.insert("swe_bench_task_id".into(), json!(""));
    event_data.insert("agent_id".into(), json!(""));
    event_data.insert("parent_session_id".into(), json!(""));
    event_data.insert("agent_type".into(), json!(""));
    event_data.insert("team_name".into(), json!(""));
    event_data.insert("skill_name".into(), json!(""));
    event_data.insert("plugin_name".into(), json!(""));
    event_data.insert("marketplace_name".into(), json!(""));
    event_data.insert("additional_metadata".into(), json!(""));
    event_data.insert("auth".into(), auth.clone());
    event_data.insert("env".into(), env_obj.clone());
    event_data.insert("process".into(), json!(process_b64));
    event_data
}

fn enqueue_events(queue: &mut VecDeque<TelemetryEvent>, events: Vec<TelemetryEvent>) {
    for event in events {
        if queue.len() >= EVENT_QUEUE_MAX {
            queue.pop_front();
        }
        queue.push_back(event);
    }
}

fn startup_events(account: &Account) -> VecDeque<TelemetryEvent> {
    let mut events = VecDeque::new();
    let model = "claude-sonnet-4-20250514".to_string();
    let session_id = None;
    for name in [
        "tengu_started",
        "tengu_init",
        "tengu_startup_telemetry",
        "tengu_feature_ok",
    ] {
        events.push_back(internal_event(name, &model, session_id.clone()));
    }
    let mut skill_fields = serde_json::Map::new();
    skill_fields.insert("skill_source".into(), json!("startup_summary"));
    skill_fields.insert("skill_count".into(), json!(0));
    events.push_back(internal_event_with_fields(
        "tengu_skill_loaded",
        &model,
        session_id.clone(),
        skill_fields,
    ));
    events.push_back(growthbook_experiment_event(&model, session_id));
    if !account
        .subscription_type
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        let mut fields = serde_json::Map::new();
        fields.insert(
            "subscription_type".into(),
            json!(account.subscription_type.clone().unwrap_or_default()),
        );
        events.push_back(internal_event_with_fields(
            "tengu_feature_ok",
            &model,
            None,
            fields,
        ));
    }
    events
}

fn message_request_events(context: &MessageTelemetryContext) -> Vec<TelemetryEvent> {
    let mut events = Vec::new();
    let model = normalized_model(&context.model);
    let session_id = context.session_id.clone();

    let mut before = common_message_fields(context);
    before.insert("phase".into(), json!("before_normalize"));
    events.push(internal_event_with_fields(
        "tengu_api_before_normalize",
        &model,
        session_id.clone(),
        before,
    ));

    let mut after = common_message_fields(context);
    after.insert("phase".into(), json!("after_normalize"));
    events.push(internal_event_with_fields(
        "tengu_api_after_normalize",
        &model,
        session_id.clone(),
        after,
    ));

    let mut query = common_message_fields(context);
    query.insert("api_endpoint".into(), json!("/v1/messages"));
    events.push(internal_event_with_fields(
        "tengu_api_query",
        &model,
        session_id.clone(),
        query,
    ));

    events.push(internal_event_with_fields(
        "tengu_sysprompt_boundary_found",
        &model,
        session_id.clone(),
        system_prompt_fields(context),
    ));
    events.push(internal_event_with_fields(
        "tengu_sysprompt_block",
        &model,
        session_id.clone(),
        system_prompt_fields(context),
    ));

    let mut cache_fields = common_message_fields(context);
    cache_fields.insert(
        "breakpoint_count".into(),
        json!(context.system_prompt_block_count),
    );
    events.push(internal_event_with_fields(
        "tengu_api_cache_breakpoints",
        &model,
        session_id.clone(),
        cache_fields,
    ));

    if context.tool_count > 0 {
        let mut tool_fields = common_message_fields(context);
        tool_fields.insert("tool_count".into(), json!(context.tool_count));
        events.push(internal_event_with_fields(
            "tengu_tool_search_mode_decision",
            &model,
            session_id.clone(),
            tool_fields.clone(),
        ));
        events.push(internal_event_with_fields(
            "tengu_tool_use_can_use_tool_allowed",
            &model,
            session_id.clone(),
            tool_fields.clone(),
        ));
        events.push(internal_event_with_fields(
            "tengu_tool_use_granted_in_config",
            &model,
            session_id.clone(),
            tool_fields,
        ));
    }

    if context.attachment_count > 0 {
        let mut attachment_fields = common_message_fields(context);
        attachment_fields.insert("attachment_count".into(), json!(context.attachment_count));
        events.push(internal_event_with_fields(
            "tengu_query_before_attachments",
            &model,
            session_id.clone(),
            attachment_fields.clone(),
        ));
        events.push(internal_event_with_fields(
            "tengu_attachment_compute_duration",
            &model,
            session_id.clone(),
            attachment_fields.clone(),
        ));
        events.push(internal_event_with_fields(
            "tengu_query_after_attachments",
            &model,
            session_id.clone(),
            attachment_fields,
        ));

        let mut file_fields = common_message_fields(context);
        file_fields.insert("operation".into(), json!("attachment_summary"));
        file_fields.insert("file_count".into(), json!(context.attachment_count));
        events.push(internal_event_with_fields(
            "tengu_file_operation",
            &model,
            session_id,
            file_fields,
        ));
    }

    events
}

fn message_result_events(
    context: &MessageTelemetryContext,
    result: &MessageTelemetryResult,
) -> Vec<TelemetryEvent> {
    let mut events = Vec::new();
    let model = normalized_model(&context.model);
    let session_id = context.session_id.clone();
    let mut fields = common_message_fields(context);
    fields.insert("duration_ms".into(), json!(result.duration_ms));
    if let Some(status) = result.status_code {
        fields.insert("status_code".into(), json!(status));
    }
    if let Some(ref error_kind) = result.error_kind {
        fields.insert("error_kind".into(), json!(error_kind));
    }

    let event_name = match (result.status_code, result.error_kind.as_deref()) {
        (Some(status), _) if (200..300).contains(&status) => "tengu_api_success",
        (Some(429), _) => "tengu_api_rate_limit",
        (_, Some(_)) => "tengu_api_error",
        _ => "tengu_api_success",
    };
    events.push(internal_event_with_fields(
        event_name,
        &model,
        session_id.clone(),
        fields.clone(),
    ));

    if result.duration_ms >= SLOW_FIRST_BYTE_MS {
        events.push(internal_event_with_fields(
            "tengu_api_slow_first_byte",
            &model,
            session_id.clone(),
            fields.clone(),
        ));
    }

    if context.tool_count > 0 && result.status_code.is_some_and(|s| (200..300).contains(&s)) {
        let mut tool_fields = fields.clone();
        tool_fields.insert("tool_count".into(), json!(context.tool_count));
        events.push(internal_event_with_fields(
            "tengu_tool_use_success",
            &model,
            session_id,
            tool_fields,
        ));
    }

    events
}

fn internal_event(name: &str, model: &str, session_id: Option<String>) -> TelemetryEvent {
    internal_event_with_fields(name, model, session_id, serde_json::Map::new())
}

fn internal_event_with_fields(
    name: &str,
    model: &str,
    session_id: Option<String>,
    fields: serde_json::Map<String, serde_json::Value>,
) -> TelemetryEvent {
    TelemetryEvent {
        event_type: TelemetryEventType::Internal,
        name: Some(name.to_string()),
        model: normalized_model(model),
        session_id,
        fields,
    }
}

fn growthbook_experiment_event(model: &str, session_id: Option<String>) -> TelemetryEvent {
    TelemetryEvent {
        event_type: TelemetryEventType::GrowthbookExperiment,
        name: None,
        model: normalized_model(model),
        session_id,
        fields: serde_json::Map::new(),
    }
}

fn common_message_fields(
    context: &MessageTelemetryContext,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert("api_endpoint".into(), json!("/v1/messages"));
    fields.insert(
        "request_body_bytes".into(),
        json!(context.request_body_bytes),
    );
    fields.insert(
        "rewritten_body_bytes".into(),
        json!(context.rewritten_body_bytes),
    );
    fields.insert("stream".into(), json!(context.stream));
    fields.insert("tool_count".into(), json!(context.tool_count));
    fields.insert("attachment_count".into(), json!(context.attachment_count));
    fields.insert(
        "system_prompt_block_count".into(),
        json!(context.system_prompt_block_count),
    );
    fields.insert("client_type_source".into(), json!(context.client_type));
    fields.insert("attempt".into(), json!(context.attempt));
    fields
}

fn system_prompt_fields(
    context: &MessageTelemetryContext,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "system_prompt_block_count".into(),
        json!(context.system_prompt_block_count),
    );
    fields.insert(
        "has_system_prompt".into(),
        json!(context.system_prompt_block_count > 0),
    );
    fields.insert("content_strategy".into(), json!("redacted_summary"));
    fields
}

fn normalized_model(model: &str) -> String {
    if model.trim().is_empty() {
        "claude-sonnet-4-20250514".to_string()
    } else {
        model.to_string()
    }
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
    use super::{
        MessageTelemetryContext, MessageTelemetryResult, TelemetryEvent, build_event_batch,
        build_growthbook_eval, build_metrics, enqueue_events, internal_event, is_telemetry_path,
        message_request_events, message_result_events,
    };
    use crate::model::account::{
        Account, AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData,
        CanonicalProcessData, CanonicalPromptEnvData,
    };
    use crate::model::identity::run_profile;
    use crate::service::rewriter::ordered_anthropic_headers;
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
            rpm_limit: 0,
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
    fn telemetry_headers_match_2169_wire_profile() {
        let event_headers = super::telemetry_request_headers(
            "/api/event_logging/v2/batch",
            "redacted",
            "claude-code/2.1.169",
            true,
        );
        assert_eq!(
            event_headers.get("Accept").unwrap(),
            "application/json, text/plain, */*"
        );
        assert_eq!(
            event_headers.get("accept-encoding").unwrap(),
            "gzip, compress, deflate, br"
        );
        assert_eq!(
            ordered_header_names("/api/event_logging/v2/batch", &event_headers),
            vec![
                "Accept",
                "Accept-Encoding",
                "Authorization",
                "Content-Type",
                "User-Agent",
                "anthropic-beta",
                "x-service-name",
                "Connection",
                "Host",
            ]
        );

        let eval_headers = super::telemetry_request_headers(
            "/api/eval/sdk-zAZezfDKGoZuXXKe",
            "redacted",
            "Bun/1.3.14",
            false,
        );
        assert_eq!(eval_headers.get("Accept").unwrap(), "*/*");
        assert_eq!(
            eval_headers.get("accept-encoding").unwrap(),
            "gzip, deflate, br, zstd"
        );
        assert_eq!(
            ordered_header_names("/api/eval/sdk-zAZezfDKGoZuXXKe", &eval_headers),
            vec![
                "Authorization",
                "Content-Type",
                "anthropic-beta",
                "Connection",
                "User-Agent",
                "Accept",
                "Host",
                "Accept-Encoding",
            ]
        );
    }

    #[test]
    fn growthbook_eval_contains_2169_attributes() {
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
        let events = vec![internal_event(
            "tengu_api_success",
            "claude-sonnet-4-20250514",
            Some("session-1".into()),
        )];
        let payload = build_event_batch(&account, &run, 12.5, &events);
        let event = &payload["events"][0]["event_data"];
        assert_eq!(event["device_id"], "device-1");
        assert_eq!(event["email"], "user@example.com");
        assert_eq!(event["session_id"], "session-1");
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
    fn message_lifecycle_events_are_request_driven() {
        let context = MessageTelemetryContext {
            model: "claude-sonnet-4-20250514".into(),
            session_id: Some("session-1".into()),
            request_body_bytes: 123,
            rewritten_body_bytes: 456,
            stream: true,
            tool_count: 2,
            attachment_count: 1,
            system_prompt_block_count: 3,
            client_type: "api".into(),
            attempt: 1,
        };

        let request_events = message_request_events(&context);
        let names: Vec<_> = request_events
            .iter()
            .filter_map(|event| event.name.as_deref())
            .collect();
        assert!(names.contains(&"tengu_api_before_normalize"));
        assert!(names.contains(&"tengu_api_after_normalize"));
        assert!(names.contains(&"tengu_api_query"));
        assert!(names.contains(&"tengu_sysprompt_boundary_found"));
        assert!(names.contains(&"tengu_sysprompt_block"));
        assert!(names.contains(&"tengu_tool_search_mode_decision"));
        assert!(names.contains(&"tengu_attachment_compute_duration"));
        assert!(names.contains(&"tengu_file_operation"));

        let result_events = message_result_events(
            &context,
            &MessageTelemetryResult {
                status_code: Some(200),
                duration_ms: 12_001,
                error_kind: None,
            },
        );
        let result_names: Vec<_> = result_events
            .iter()
            .filter_map(|event| event.name.as_deref())
            .collect();
        assert!(result_names.contains(&"tengu_api_success"));
        assert!(result_names.contains(&"tengu_api_slow_first_byte"));
        assert!(result_names.contains(&"tengu_tool_use_success"));
    }

    #[test]
    fn event_batch_does_not_include_raw_prompt_or_token() {
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap(),
        );
        let context = MessageTelemetryContext {
            model: "claude-sonnet-4-20250514".into(),
            session_id: Some("session-1".into()),
            request_body_bytes: 777,
            rewritten_body_bytes: 888,
            stream: false,
            tool_count: 0,
            attachment_count: 0,
            system_prompt_block_count: 1,
            client_type: "api".into(),
            attempt: 0,
        };
        let events = message_request_events(&context);
        let payload = build_event_batch(&account, &run, 3.0, &events);
        let serialized = serde_json::to_string(&payload).unwrap();

        assert!(!serialized.contains("raw-token-marker"));
        assert!(!serialized.contains("raw-prompt-marker"));
        assert!(!serialized.contains("raw-tool-input-marker"));
        assert!(serialized.contains("redacted_summary"));
        assert!(serialized.contains("request_body_bytes"));
    }

    #[test]
    fn event_queue_enforces_max_size() {
        let mut queue = std::collections::VecDeque::<TelemetryEvent>::new();
        for idx in 0..250 {
            enqueue_events(
                &mut queue,
                vec![internal_event(
                    &format!("event_{}", idx),
                    "claude-sonnet-4-20250514",
                    None,
                )],
            );
        }
        assert_eq!(queue.len(), super::EVENT_QUEUE_MAX);
        assert_eq!(queue.front().unwrap().name.as_deref(), Some("event_50"));
        assert_eq!(queue.back().unwrap().name.as_deref(), Some("event_249"));
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

    fn ordered_header_names(
        path: &str,
        headers: &std::collections::HashMap<String, String>,
    ) -> Vec<String> {
        ordered_anthropic_headers(path, headers)
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }
}
