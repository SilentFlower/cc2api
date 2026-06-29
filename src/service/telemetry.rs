use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use chrono::Utc;
use serde_json::json;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::model::account::Account;
use crate::model::identity::{
    DeviceProfile, RunProfile, build_full_env_json, device_profile, process_snapshot,
    process_snapshot_json, run_profile,
};
use crate::service::account::AccountService;
use crate::service::rewriter::ordered_anthropic_headers;
use crate::service::version_profile::{
    EVENT_LOGGING_V2_PATH, MESSAGE_BETA_TOKENS, OAUTH_BETA_TOKEN, TelemetryShape,
    claude_code_user_agent, is_event_logging_path, normalize_version, profile_for_version,
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
const ADDITIONAL_METADATA_LONG_TEXT_LIMIT: usize = 1024;
const EVENT_DATA_LONG_TEXT_LIMIT: usize = 4096;

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
    correlations: TelemetryCorrelationStore,
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
    /// 网关内部请求键，只用于把 request/result 两个记录点关联起来，不发送到上游。
    pub request_key: String,
    /// 请求声明的模型名称。为空时事件构造会回退到默认 Claude Code 模型。
    pub model: String,
    /// 请求级 session id，只来自 metadata 派生字段，不来自正文原文。
    pub session_id: Option<String>,
    /// 改写前 message 数量。
    pub pre_normalized_message_count: usize,
    /// 改写后 message 数量。
    pub post_normalized_message_count: usize,
    /// 改写后 message content block 总数。
    pub message_content_block_count: usize,
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
    /// 请求中的图片 block 数量。
    pub image_block_count: usize,
    /// 请求中可安全估算的图片 payload 字节数。
    pub image_total_bytes: usize,
    /// 请求中的文档/文件 block 数量。
    pub document_block_count: usize,
    /// 请求中可安全估算的文档/文件 payload 字节数。
    pub document_total_bytes: usize,
    /// 改写后 system prompt 文本块数量。
    pub system_prompt_block_count: usize,
    /// 改写后 system prompt 文本总长度，只记录长度不记录正文。
    pub system_prompt_text_length: usize,
    /// 改写后 system prompt cache breakpoint 数量。
    pub system_cache_breakpoint_count: usize,
    /// 改写后 tool cache breakpoint 数量。
    pub tool_cache_breakpoint_count: usize,
    /// 改写后 message cache breakpoint 数量。
    pub message_cache_breakpoint_count: usize,
    /// 请求 thinking.type。
    pub thinking_type: Option<String>,
    /// 请求 temperature，只保留数值字段。
    pub temperature: Option<f64>,
    /// 请求中首个工具名，只保留工具名不保留 tool input。
    pub primary_tool_name: Option<String>,
    /// 输入 text block 的字符总长度，只记录长度不记录正文。
    pub input_text_char_length: usize,
    /// 请求来源类型，用于区分 Claude Code 客户端和纯 API 客户端。
    pub client_type: String,
    /// 当前网关重试序号。
    pub attempt: usize,
    /// 最终发送到 `/v1/messages` 的 `anthropic-beta` header。
    pub betas: String,
}

/// 表示 `/v1/messages` 上游响应阶段的安全遥测摘要。
#[derive(Debug, Clone)]
pub struct MessageTelemetryResult {
    /// 上游 HTTP 状态码。网络错误时为空。
    pub status_code: Option<u16>,
    /// 从发送上游请求到响应体结束或错误的耗时。
    pub duration_ms: u64,
    /// 从发送上游请求到收到响应头或首个可用结果的耗时。
    pub ttft_ms: Option<u64>,
    /// 上游请求失败类型。成功响应为空。
    pub error_kind: Option<String>,
    /// 上游响应体字节数，只记录长度不记录正文。
    pub response_body_bytes: Option<usize>,
    /// 上游响应 usage 数值摘要。
    pub usage: MessageTelemetryUsage,
    /// 上游 stop_reason，只保留枚举值。
    pub stop_reason: Option<String>,
}

/// 表示 `/v1/messages` 上游响应中的安全 usage 数值摘要。
#[derive(Debug, Clone, Copy, Default)]
pub struct MessageTelemetryUsage {
    /// 输入 token 数。
    pub input_tokens: u64,
    /// 输出 token 数。
    pub output_tokens: u64,
    /// prompt cache 读取 token 数。
    pub cache_read_input_tokens: u64,
    /// prompt cache 写入 token 数。
    pub cache_creation_input_tokens: u64,
    /// 5 分钟 cache 写入 token 数。
    pub cache_creation_ephemeral_5m_input_tokens: u64,
    /// 1 小时 cache 写入 token 数。
    pub cache_creation_ephemeral_1h_input_tokens: u64,
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

#[derive(Debug, Clone, Default)]
struct TelemetryCorrelationStore {
    by_session: HashMap<String, TelemetrySessionCorrelation>,
    by_request: HashMap<String, MessageCorrelation>,
}

#[derive(Debug, Clone)]
struct TelemetrySessionCorrelation {
    query_chain_id: String,
    query_depth: u64,
    last_request_id: Option<String>,
    last_api_call_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct MessageCorrelation {
    query_chain_id: String,
    query_depth: u64,
    request_id: String,
    message_id: String,
    previous_request_id: Option<String>,
    time_since_last_api_call_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct TelemetryPayloadScanSummary {
    event_count: usize,
    event_name_counts: BTreeMap<String, usize>,
    dropped_fields: usize,
    dropped_events: usize,
    reason_counts: BTreeMap<&'static str, usize>,
    field_drops: Vec<TelemetryFieldDrop>,
}

#[derive(Debug, Clone)]
struct TelemetryFieldDrop {
    event_name: String,
    path: String,
    reason: &'static str,
    action: &'static str,
}

impl TelemetryFieldDrop {
    fn as_log_tuple(&self) -> (&str, &str, &'static str, &'static str) {
        (
            self.event_name.as_str(),
            self.path.as_str(),
            self.reason,
            self.action,
        )
    }
}

#[derive(Debug, Clone, Default)]
struct TelemetryShapeSummary {
    event_name_counts: BTreeMap<String, usize>,
    metadata_keys_by_event: BTreeMap<String, BTreeMap<String, usize>>,
    metadata_types_by_event: BTreeMap<String, BTreeMap<String, BTreeMap<String, usize>>>,
    dropped_fields: usize,
    dropped_events: usize,
    reason_counts: BTreeMap<&'static str, usize>,
}

impl TelemetryCorrelationStore {
    fn register_request(
        &mut self,
        context: &MessageTelemetryContext,
        fallback_session_id: &str,
    ) -> MessageCorrelation {
        let now = Instant::now();
        let session_key = correlation_session_key(context, fallback_session_id);
        let session = self
            .by_session
            .entry(session_key)
            .or_insert_with(TelemetrySessionCorrelation::new);
        let time_since_last_api_call_ms = session
            .last_api_call_at
            .map(|last| now.duration_since(last).as_millis() as u64);
        let correlation = MessageCorrelation {
            query_chain_id: session.query_chain_id.clone(),
            query_depth: session.query_depth,
            request_id: prefixed_random_id("req"),
            message_id: prefixed_random_id("msg"),
            previous_request_id: session.last_request_id.clone(),
            time_since_last_api_call_ms,
        };
        session.last_request_id = Some(correlation.request_id.clone());
        session.last_api_call_at = Some(now);
        session.query_depth = session.query_depth.saturating_add(1);
        self.by_request
            .insert(context.request_key.clone(), correlation.clone());
        correlation
    }

    fn resolve_result(
        &mut self,
        context: &MessageTelemetryContext,
        fallback_session_id: &str,
    ) -> MessageCorrelation {
        self.by_request
            .remove(&context.request_key)
            .unwrap_or_else(|| self.register_request(context, fallback_session_id))
    }
}

impl TelemetrySessionCorrelation {
    fn new() -> Self {
        Self {
            query_chain_id: uuid::Uuid::new_v4().to_string(),
            query_depth: 0,
            last_request_id: None,
            last_api_call_at: None,
        }
    }
}

fn correlation_session_key(context: &MessageTelemetryContext, fallback_session_id: &str) -> String {
    context
        .session_id
        .clone()
        .unwrap_or_else(|| format!("run:{}", fallback_session_id))
}

fn prefixed_random_id(prefix: &str) -> String {
    let raw = uuid::Uuid::new_v4().simple().to_string();
    format!("{}_{}", prefix, &raw[..24])
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
            correlations: TelemetryCorrelationStore::default(),
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
            let correlation = session
                .correlations
                .register_request(&context, &session.run_profile.session_id);
            enqueue_events(
                &mut session.pending_events,
                message_request_events(&context, &correlation),
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
            let correlation = session
                .correlations
                .resolve_result(&context, &session.run_profile.session_id);
            enqueue_events(
                &mut session.pending_events,
                message_result_events(&context, &result, &correlation),
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
            let mut payload =
                build_event_batch(&session.account, &session.run_profile, uptime_secs, &events);
            let scan_summary = sanitize_telemetry_payload(&mut payload);
            let token = session.token.clone();
            let c = client.clone();
            session.last_event_batch_at = now;
            session.send_count += 1;
            let event_count = events.len();
            let event_name_counts = scan_summary.event_name_counts.clone();
            drop(map);

            log_simulated_batch_pre_send(account_id, &scan_summary);
            send_telemetry(
                &c,
                &format!("{}{}", UPSTREAM_BASE, EVENT_LOGGING_V2_PATH),
                &token,
                &payload,
                &session_ua(&store, account_id).await,
                true,
                Some(&scan_summary),
            )
            .await;

            let _ = store.increment_telemetry_count(account_id, 1).await;
            debug!(
                event_count,
                account_id,
                event_name_counts = ?event_name_counts,
                "telemetry: simulated batch queued for send"
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
            let user_agent = growthbook_user_agent_for_account(&session.account);
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
                user_agent,
                false,
                None,
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

fn growthbook_user_agent_for_account(account: &Account) -> &'static str {
    let profile = device_profile(account);
    profile_for_version(&profile.env.version)
        .telemetry
        .growthbook_user_agent
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
    scan_summary: Option<&TelemetryPayloadScanSummary>,
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
                if let Some(summary) = scan_summary {
                    info!(
                        url,
                        status = %status,
                        event_count = summary.event_count,
                        dropped_fields = summary.dropped_fields,
                        dropped_events = summary.dropped_events,
                        reason_counts = ?summary.reason_counts,
                        "telemetry: simulated batch send succeeded"
                    );
                } else {
                    debug!("telemetry: {} → {}", url, status);
                }
            } else {
                if let Some(summary) = scan_summary {
                    warn!(
                        url,
                        status = %status,
                        event_count = summary.event_count,
                        dropped_fields = summary.dropped_fields,
                        dropped_events = summary.dropped_events,
                        reason_counts = ?summary.reason_counts,
                        "telemetry: simulated batch send failed"
                    );
                } else {
                    warn!("telemetry: {} → {}", url, status);
                }
            }
        }
        Err(e) => {
            if let Some(summary) = scan_summary {
                warn!(
                    url,
                    error_kind = %e,
                    event_count = summary.event_count,
                    dropped_fields = summary.dropped_fields,
                    dropped_events = summary.dropped_events,
                    reason_counts = ?summary.reason_counts,
                    "telemetry: simulated batch send errored"
                );
            } else {
                warn!("telemetry: {} failed: {}", url, e);
            }
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
    let version_profile = profile_for_version(&profile.env.version);
    let process_b64 = {
        let snapshot = process_snapshot(&profile.process, &profile.device_id, uptime_secs);
        let p = process_snapshot_json(&snapshot);
        let bytes = serde_json::to_vec(&p).unwrap_or_default();
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    };

    let env_obj = telemetry_env_json(&profile, version_profile.telemetry.shape);

    let mut auth = json!({});
    auth["account_uuid"] = json!(profile.account_uuid);
    if let Some(ref org) = profile.organization_uuid {
        auth["organization_uuid"] = json!(org);
    }

    let events: Vec<serde_json::Value> = events
        .iter()
        .map(|event| {
            build_event_json(
                event,
                run_profile,
                &profile,
                version_profile.telemetry.shape,
                &env_obj,
                &auth,
                &process_b64,
            )
        })
        .collect();

    json!({ "events": events })
}

fn sanitize_telemetry_payload(payload: &mut serde_json::Value) -> TelemetryPayloadScanSummary {
    let mut summary = TelemetryPayloadScanSummary::default();
    let Some(events) = payload
        .get_mut("events")
        .and_then(|events| events.as_array_mut())
    else {
        return summary;
    };

    let mut kept_events = Vec::with_capacity(events.len());
    for mut event in std::mem::take(events) {
        let event_name = telemetry_event_name(&event);
        *summary
            .event_name_counts
            .entry(event_name.clone())
            .or_default() += 1;
        summary.event_count += 1;

        if sanitize_event_value(&mut event, &event_name, &mut summary) {
            kept_events.push(event);
        } else {
            summary.dropped_events += 1;
            *summary.reason_counts.entry("invalid_event").or_default() += 1;
        }
    }
    *events = kept_events;
    summary
}

fn sanitize_event_value(
    event: &mut serde_json::Value,
    event_name: &str,
    summary: &mut TelemetryPayloadScanSummary,
) -> bool {
    let Some(event_data) = event
        .get_mut("event_data")
        .and_then(|data| data.as_object_mut())
    else {
        return false;
    };

    sanitize_json_object(
        event_data,
        event_name,
        "event_data",
        EVENT_DATA_LONG_TEXT_LIMIT,
        summary,
    );

    if let Some(metadata_value) = event_data.get_mut("additional_metadata") {
        sanitize_additional_metadata(metadata_value, event_name, summary);
    }

    true
}

fn sanitize_additional_metadata(
    metadata_value: &mut serde_json::Value,
    event_name: &str,
    summary: &mut TelemetryPayloadScanSummary,
) {
    let Some(encoded) = metadata_value.as_str() else {
        return;
    };
    if encoded.is_empty() {
        return;
    }
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        record_field_drop(
            summary,
            event_name,
            "event_data.additional_metadata",
            "invalid_base64_metadata",
        );
        *metadata_value = json!("");
        return;
    };
    let Ok(mut metadata) = serde_json::from_slice::<serde_json::Value>(&decoded) else {
        record_field_drop(
            summary,
            event_name,
            "event_data.additional_metadata",
            "invalid_json_metadata",
        );
        *metadata_value = json!("");
        return;
    };
    if let Some(map) = metadata.as_object_mut() {
        sanitize_json_object(
            map,
            event_name,
            "event_data.additional_metadata",
            ADDITIONAL_METADATA_LONG_TEXT_LIMIT,
            summary,
        );
        let bytes = serde_json::to_vec(&serde_json::Value::Object(map.clone())).unwrap_or_default();
        *metadata_value = json!(base64::engine::general_purpose::STANDARD.encode(bytes));
    } else {
        record_field_drop(
            summary,
            event_name,
            "event_data.additional_metadata",
            "non_object_metadata",
        );
        *metadata_value = json!("");
    }
}

fn sanitize_json_object(
    map: &mut serde_json::Map<String, serde_json::Value>,
    event_name: &str,
    path: &str,
    long_text_limit: usize,
    summary: &mut TelemetryPayloadScanSummary,
) {
    map.retain(|key, value| {
        let field_path = format!("{}.{}", path, key);
        let reason = sensitive_field_reason(key, value, &field_path, long_text_limit);
        if let Some(reason) = reason {
            record_field_drop(summary, event_name, &field_path, reason);
            return false;
        }

        sanitize_nested_value(value, event_name, &field_path, long_text_limit, summary)
    });
}

fn sanitize_nested_value(
    value: &mut serde_json::Value,
    event_name: &str,
    path: &str,
    long_text_limit: usize,
    summary: &mut TelemetryPayloadScanSummary,
) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            sanitize_json_object(map, event_name, path, long_text_limit, summary);
            true
        }
        serde_json::Value::Array(items) => {
            let mut index = 0;
            items.retain_mut(|item| {
                let item_path = format!("{}[{}]", path, index);
                index += 1;
                sanitize_nested_value(item, event_name, &item_path, long_text_limit, summary)
            });
            true
        }
        serde_json::Value::String(_) => {
            if let Some(reason) = sensitive_field_reason("", value, path, long_text_limit) {
                record_field_drop(summary, event_name, path, reason);
                return false;
            }
            true
        }
        _ => true,
    }
}

fn sensitive_field_reason(
    key: &str,
    value: &serde_json::Value,
    path: &str,
    long_text_limit: usize,
) -> Option<&'static str> {
    let key_lower = key.to_ascii_lowercase();
    if sensitive_key(&key_lower) {
        return Some("sensitive_key");
    }
    let Some(text) = value.as_str() else {
        return None;
    };
    if text.len() > long_text_limit && !allowed_long_text_path(path) {
        return Some("long_text");
    }
    if looks_like_email(text) {
        return Some("email_value");
    }
    if looks_like_bearer_or_cookie(text) {
        return Some("credential_value");
    }
    if looks_like_uuid(text) && !allowed_uuid_path(path) {
        return Some("uuid_value");
    }
    None
}

fn sensitive_key(key_lower: &str) -> bool {
    key_lower.contains("authorization")
        || key_lower == "cookie"
        || key_lower == "token"
        || key_lower.contains("access_token")
        || key_lower.contains("refresh_token")
        || key_lower.contains("api_key")
        || key_lower.contains("apikey")
        || key_lower.contains("prompt")
        || key_lower.contains("tool_input")
        || key_lower.contains("toolinput")
        || key_lower.contains("response_body")
        || key_lower.contains("responsebody")
        || key_lower.contains("email")
}

fn allowed_long_text_path(path: &str) -> bool {
    path.ends_with(".process")
        || path.ends_with(".env")
        || path.ends_with(".auth")
        || path.ends_with(".additional_metadata")
}

fn allowed_uuid_path(path: &str) -> bool {
    path.ends_with(".event_id")
        || path.ends_with(".device_id")
        || path.ends_with(".session_id")
        || path.ends_with(".queryChainId")
        || path.ends_with(".requestId")
        || path.ends_with(".previousRequestId")
        || path.ends_with(".messageID")
}

fn looks_like_email(value: &str) -> bool {
    let Some((local, domain)) = value.split_once('@') else {
        return false;
    };
    !local.is_empty() && domain.contains('.') && !domain.ends_with('.')
}

fn looks_like_bearer_or_cookie(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("bearer ")
        || lower.contains("session=")
        || lower.contains("cookie:")
        || lower.contains("sk-ant-")
        || lower.contains("oauth")
}

fn looks_like_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (idx, byte) in bytes.iter().enumerate() {
        if matches!(idx, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return false;
            }
        } else if !byte.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

fn record_field_drop(
    summary: &mut TelemetryPayloadScanSummary,
    event_name: &str,
    path: &str,
    reason: &'static str,
) {
    summary.dropped_fields += 1;
    *summary.reason_counts.entry(reason).or_default() += 1;
    summary.field_drops.push(TelemetryFieldDrop {
        event_name: event_name.to_string(),
        path: path.to_string(),
        reason,
        action: "drop_field",
    });
}

fn log_simulated_batch_pre_send(account_id: i64, summary: &TelemetryPayloadScanSummary) {
    if summary.dropped_fields > 0 || summary.dropped_events > 0 {
        let field_drops: Vec<_> = summary
            .field_drops
            .iter()
            .map(TelemetryFieldDrop::as_log_tuple)
            .collect();
        warn!(
            account_id,
            event_count = summary.event_count,
            event_name_counts = ?summary.event_name_counts,
            dropped_fields = summary.dropped_fields,
            dropped_events = summary.dropped_events,
            reason_counts = ?summary.reason_counts,
            field_drops = ?field_drops,
            "telemetry: simulated batch sanitized before send"
        );
    } else {
        debug!(
            account_id,
            event_count = summary.event_count,
            event_name_counts = ?summary.event_name_counts,
            dropped_fields = summary.dropped_fields,
            dropped_events = summary.dropped_events,
            "telemetry: simulated batch ready"
        );
    }
}

fn telemetry_shape_summary(payload: &serde_json::Value) -> TelemetryShapeSummary {
    let mut payload = payload.clone();
    let scan_summary = sanitize_telemetry_payload(&mut payload);
    let mut summary = TelemetryShapeSummary {
        event_name_counts: scan_summary.event_name_counts.clone(),
        dropped_fields: scan_summary.dropped_fields,
        dropped_events: scan_summary.dropped_events,
        reason_counts: scan_summary.reason_counts.clone(),
        ..TelemetryShapeSummary::default()
    };

    if let Some(events) = payload.get("events").and_then(|events| events.as_array()) {
        for event in events {
            let event_name = telemetry_event_name(event);
            let Some(metadata_b64) = event
                .get("event_data")
                .and_then(|data| data.get("additional_metadata"))
                .and_then(|metadata| metadata.as_str())
            else {
                continue;
            };
            let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(metadata_b64) else {
                continue;
            };
            let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(&decoded) else {
                continue;
            };
            let Some(map) = metadata.as_object() else {
                continue;
            };
            for (key, value) in map {
                *summary
                    .metadata_keys_by_event
                    .entry(event_name.clone())
                    .or_default()
                    .entry(key.clone())
                    .or_default() += 1;
                *summary
                    .metadata_types_by_event
                    .entry(event_name.clone())
                    .or_default()
                    .entry(key.clone())
                    .or_default()
                    .entry(json_type_name(value).to_string())
                    .or_default() += 1;
            }
        }
    }
    summary
}

fn telemetry_event_name(event: &serde_json::Value) -> String {
    event
        .get("event_data")
        .and_then(|data| data.get("event_name"))
        .and_then(|name| name.as_str())
        .unwrap_or_else(|| {
            event
                .get("event_type")
                .and_then(|event_type| event_type.as_str())
                .unwrap_or("unknown")
        })
        .to_string()
}

fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => "int",
        serde_json::Value::Number(_) => "float",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn build_event_json(
    event: &TelemetryEvent,
    run_profile: &RunProfile,
    profile: &crate::model::identity::DeviceProfile,
    shape: TelemetryShape,
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
                shape,
                env_obj,
                auth,
                process_b64,
            );
            apply_internal_top_level_fields(&mut event_data, event);
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

fn apply_internal_top_level_fields(
    event_data: &mut serde_json::Map<String, serde_json::Value>,
    event: &TelemetryEvent,
) {
    for key in ["betas"] {
        if let Some(value) = event.fields.get(key) {
            event_data.insert(key.to_string(), value.clone());
        }
    }
}

fn base_internal_event_data(
    event_name: &str,
    event: &TelemetryEvent,
    run_profile: &RunProfile,
    profile: &crate::model::identity::DeviceProfile,
    shape: TelemetryShape,
    env_obj: &serde_json::Value,
    auth: &serde_json::Value,
    process_b64: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut event_data = serde_json::Map::new();
    event_data.insert("event_id".into(), json!(uuid::Uuid::new_v4().to_string()));
    event_data.insert("event_name".into(), json!(event_name));
    event_data.insert("client_timestamp".into(), json!(js_iso_timestamp()));
    event_data.insert("device_id".into(), json!(profile.device_id));
    if shape == TelemetryShape::ClaudeCode2173 {
        event_data.insert("email".into(), json!(profile.email));
    }
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
    event_data.insert(
        "betas".into(),
        json!(match shape {
            TelemetryShape::ClaudeCode2173 => "",
            TelemetryShape::ClaudeCode2185 => MESSAGE_BETA_TOKENS,
        }),
    );
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
    let additional_metadata = match shape {
        TelemetryShape::ClaudeCode2173 => String::new(),
        TelemetryShape::ClaudeCode2185 => encoded_additional_metadata(event, profile),
    };
    event_data.insert("additional_metadata".into(), json!(additional_metadata));
    event_data.insert("auth".into(), auth.clone());
    event_data.insert("env".into(), env_obj.clone());
    event_data.insert("process".into(), json!(process_b64));
    event_data
}

fn telemetry_env_json(profile: &DeviceProfile, shape: TelemetryShape) -> serde_json::Value {
    let mut env_obj = build_full_env_json(&profile.env);
    if shape == TelemetryShape::ClaudeCode2185 {
        if let Some(map) = env_obj.as_object_mut() {
            map.insert("shell".into(), json!(profile.prompt.shell));
            map.insert(
                "is_running_with_bun".into(),
                json!(profile.env.is_running_with_bun),
            );
        }
    }
    env_obj
}

fn encoded_additional_metadata(event: &TelemetryEvent, profile: &DeviceProfile) -> String {
    let mut metadata = serde_json::Map::new();
    metadata.insert("renderer_mode".into(), json!("fullscreen"));
    metadata.insert("model".into(), json!(event.model));
    metadata.insert(
        "subscription_type".into(),
        json!(profile.subscription_type.clone().unwrap_or_default()),
    );
    for (key, value) in &event.fields {
        if should_skip_additional_metadata_key(key) {
            continue;
        }
        metadata.insert(key.clone(), value.clone());
    }
    let bytes = serde_json::to_vec(&serde_json::Value::Object(metadata)).unwrap_or_default();
    base64::engine::general_purpose::STANDARD.encode(&bytes)
}

fn should_skip_additional_metadata_key(key: &str) -> bool {
    matches!(
        key,
        "api_endpoint" | "request_body_bytes" | "rewritten_body_bytes" | "client_type_source"
    )
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

fn message_request_events(
    context: &MessageTelemetryContext,
    correlation: &MessageCorrelation,
) -> Vec<TelemetryEvent> {
    let mut events = Vec::new();
    let model = normalized_model(&context.model);
    let session_id = context.session_id.clone();

    events.push(internal_event_with_fields(
        "tengu_api_before_normalize",
        &model,
        session_id.clone(),
        before_normalize_fields(context),
    ));

    events.push(internal_event_with_fields(
        "tengu_api_after_normalize",
        &model,
        session_id.clone(),
        after_normalize_fields(context),
    ));

    let query = query_fields(context, correlation);
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

    let mut cache_fields = serde_json::Map::new();
    cache_fields.insert(
        "totalMessageCount".into(),
        json!(context.post_normalized_message_count),
    );
    cache_fields.insert("cachingEnabled".into(), json!(true));
    cache_fields.insert("skipCacheWrite".into(), json!(false));
    cache_fields.insert("forkPointPinned".into(), json!(false));
    cache_fields.insert("markerCount".into(), json!(cache_marker_count(context)));
    events.push(internal_event_with_fields(
        "tengu_api_cache_breakpoints",
        &model,
        session_id.clone(),
        cache_fields,
    ));

    if context.tool_count > 0 {
        let tool_fields = tool_fields(context, correlation);
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
        let mut attachment_fields = serde_json::Map::new();
        attachment_fields.insert("label".into(), json!("attachment_summary"));
        attachment_fields.insert("attachment_count".into(), json!(context.attachment_count));
        if context.image_block_count > 0 {
            attachment_fields.insert("image_block_count".into(), json!(context.image_block_count));
        }
        if context.image_total_bytes > 0 {
            attachment_fields.insert("image_total_bytes".into(), json!(context.image_total_bytes));
        }
        if context.document_block_count > 0 {
            attachment_fields.insert(
                "document_block_count".into(),
                json!(context.document_block_count),
            );
        }
        if context.document_total_bytes > 0 {
            attachment_fields.insert(
                "document_total_bytes".into(),
                json!(context.document_total_bytes),
            );
        }
        let attachment_size_bytes = context
            .image_total_bytes
            .saturating_add(context.document_total_bytes);
        if attachment_size_bytes > 0 {
            attachment_fields.insert("attachment_size_bytes".into(), json!(attachment_size_bytes));
        }
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

        let mut file_fields = serde_json::Map::new();
        file_fields.insert("operation".into(), json!("attachment_summary"));
        file_fields.insert("tool".into(), json!("attachment"));
        file_fields.insert("filePathHash".into(), json!("redacted"));
        file_fields.insert("type".into(), json!("attachment"));
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
    correlation: &MessageCorrelation,
) -> Vec<TelemetryEvent> {
    let mut events = Vec::new();
    let model = normalized_model(&context.model);
    let session_id = context.session_id.clone();
    let mut fields = success_fields(context, result, correlation);
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
        let mut slow_fields = serde_json::Map::new();
        slow_fields.insert("model".into(), json!(model));
        slow_fields.insert("provider".into(), json!("firstParty"));
        slow_fields.insert("attempt".into(), json!(context.attempt));
        slow_fields.insert(
            "elapsed_ms".into(),
            json!(result.ttft_ms.unwrap_or(result.duration_ms)),
        );
        events.push(internal_event_with_fields(
            "tengu_api_slow_first_byte",
            &model,
            session_id.clone(),
            slow_fields,
        ));
    }

    if context.tool_count > 0 && result.status_code.is_some_and(|s| (200..300).contains(&s)) {
        let mut tool_fields = tool_fields(context, correlation);
        tool_fields.insert("durationMs".into(), json!(result.duration_ms));
        if let Some(response_body_bytes) = result.response_body_bytes {
            tool_fields.insert("toolResultSizeBytes".into(), json!(response_body_bytes));
        }
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

fn before_normalize_fields(
    context: &MessageTelemetryContext,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "preNormalizedMessageCount".into(),
        json!(context.pre_normalized_message_count),
    );
    fields
}

fn after_normalize_fields(
    context: &MessageTelemetryContext,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "postNormalizedMessageCount".into(),
        json!(context.post_normalized_message_count),
    );
    fields
}

fn query_fields(
    context: &MessageTelemetryContext,
    correlation: &MessageCorrelation,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert("model".into(), json!(normalized_model(&context.model)));
    fields.insert(
        "messagesLength".into(),
        json!(context.post_normalized_message_count),
    );
    fields.insert(
        "temperature".into(),
        json!(context.temperature.unwrap_or(1.0)),
    );
    fields.insert("provider".into(), json!("firstParty"));
    fields.insert("betas".into(), json!(context.betas));
    fields.insert("permissionMode".into(), json!("default"));
    fields.insert("querySource".into(), json!("repl_main_thread"));
    fields.insert(
        "thinkingType".into(),
        json!(context.thinking_type.as_deref().unwrap_or("disabled")),
    );
    fields.insert("fastMode".into(), json!(false));
    insert_correlation_fields(&mut fields, correlation);
    fields
}

fn success_fields(
    context: &MessageTelemetryContext,
    result: &MessageTelemetryResult,
    correlation: &MessageCorrelation,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert("model".into(), json!(normalized_model(&context.model)));
    fields.insert("betas".into(), json!(context.betas));
    fields.insert(
        "messageCount".into(),
        json!(context.post_normalized_message_count),
    );
    let message_tokens = result
        .usage
        .input_tokens
        .saturating_add(result.usage.output_tokens);
    fields.insert("messageTokens".into(), json!(message_tokens));
    fields.insert("inputTokens".into(), json!(result.usage.input_tokens));
    fields.insert("outputTokens".into(), json!(result.usage.output_tokens));
    fields.insert(
        "cachedInputTokens".into(),
        json!(result.usage.cache_read_input_tokens),
    );
    let uncached_input_tokens = result
        .usage
        .input_tokens
        .saturating_sub(result.usage.cache_read_input_tokens);
    fields.insert("uncachedInputTokens".into(), json!(uncached_input_tokens));
    fields.insert("durationMs".into(), json!(result.duration_ms));
    fields.insert(
        "durationMsIncludingRetries".into(),
        json!(result.duration_ms),
    );
    fields.insert("attempt".into(), json!(context.attempt));
    fields.insert(
        "ttftMs".into(),
        json!(result.ttft_ms.unwrap_or(result.duration_ms)),
    );
    fields.insert("provider".into(), json!("firstParty"));
    fields.insert(
        "stop_reason".into(),
        json!(result.stop_reason.as_deref().unwrap_or("unknown")),
    );
    fields.insert("didFallBackToNonStreaming".into(), json!(false));
    fields.insert("isNonInteractiveSession".into(), json!(false));
    fields.insert("print".into(), json!(false));
    fields.insert("isTTY".into(), json!(true));
    fields.insert("querySource".into(), json!("repl_main_thread"));
    fields.insert("permissionMode".into(), json!("default"));
    fields.insert("globalCacheStrategy".into(), json!("system_prompt"));
    fields.insert(
        "textContentLength".into(),
        json!(context.input_text_char_length),
    );
    if context.image_block_count > 0 {
        fields.insert("imageBlockCount".into(), json!(context.image_block_count));
    }
    if context.image_total_bytes > 0 {
        fields.insert("imageTotalBytes".into(), json!(context.image_total_bytes));
    }
    if context.document_block_count > 0 {
        fields.insert(
            "documentBlockCount".into(),
            json!(context.document_block_count),
        );
    }
    if context.document_total_bytes > 0 {
        fields.insert(
            "documentTotalBytes".into(),
            json!(context.document_total_bytes),
        );
    }
    fields.insert(
        "inputTextCharLength".into(),
        json!(context.input_text_char_length),
    );
    fields.insert(
        "estimatedInputTokens".into(),
        json!(estimated_tokens(context.input_text_char_length)),
    );
    fields.insert("fastMode".into(), json!(false));
    fields.insert(
        "preNormalizedModel".into(),
        json!(normalized_model(&context.model)),
    );
    fields.insert(
        "cacheCreationInputTokens".into(),
        json!(result.usage.cache_creation_input_tokens),
    );
    fields.insert(
        "cacheCreationEphemeral5mInputTokens".into(),
        json!(result.usage.cache_creation_ephemeral_5m_input_tokens),
    );
    fields.insert(
        "cacheCreationEphemeral1hInputTokens".into(),
        json!(result.usage.cache_creation_ephemeral_1h_input_tokens),
    );
    insert_correlation_fields(&mut fields, correlation);
    if let Some(time_since_last_api_call_ms) = correlation.time_since_last_api_call_ms {
        fields.insert(
            "timeSinceLastApiCallMs".into(),
            json!(time_since_last_api_call_ms),
        );
    }
    fields
}

fn tool_fields(
    context: &MessageTelemetryContext,
    correlation: &MessageCorrelation,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert("messageID".into(), json!(correlation.message_id));
    fields.insert(
        "toolName".into(),
        json!(context.primary_tool_name.as_deref().unwrap_or("Tool")),
    );
    fields.insert("isMcp".into(), json!(false));
    insert_correlation_fields(&mut fields, correlation);
    fields
}

fn insert_correlation_fields(
    fields: &mut serde_json::Map<String, serde_json::Value>,
    correlation: &MessageCorrelation,
) {
    fields.insert("queryChainId".into(), json!(correlation.query_chain_id));
    fields.insert("queryDepth".into(), json!(correlation.query_depth));
    fields.insert("requestId".into(), json!(correlation.request_id));
    if let Some(previous_request_id) = &correlation.previous_request_id {
        fields.insert("previousRequestId".into(), json!(previous_request_id));
    }
}

fn cache_marker_count(context: &MessageTelemetryContext) -> usize {
    context
        .system_cache_breakpoint_count
        .saturating_add(context.tool_cache_breakpoint_count)
        .saturating_add(context.message_cache_breakpoint_count)
}

fn estimated_tokens(char_count: usize) -> usize {
    if char_count == 0 {
        0
    } else {
        char_count.saturating_add(3) / 4
    }
}

fn system_prompt_fields(
    context: &MessageTelemetryContext,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert(
        "blockCount".into(),
        json!(context.system_prompt_block_count),
    );
    fields.insert(
        "staticBlockLength".into(),
        json!(context.system_prompt_text_length),
    );
    fields.insert("dynamicBlockLength".into(), json!(0));
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
    let shape = profile_for_version(&profile.env.version).telemetry.shape;
    let mut attrs = json!({
        "id": profile.device_id,
        "sessionId": run_profile.growthbook_session_id,
        "deviceID": profile.device_id,
        "platform": profile.env.platform,
        "appVersion": profile.env.version,
        "accountUUID": profile.account_uuid,
        "userType": "external",
        "rateLimitTier": rate_limit_tier(&profile.subscription_type),
        "entrypoint": "cli",
    });
    if shape == TelemetryShape::ClaudeCode2173 {
        attrs["email"] = json!(profile.email);
    }

    if let Some(ref org) = profile.organization_uuid {
        attrs["organizationUUID"] = json!(org);
    }
    if let Some(ref sub) = profile.subscription_type {
        attrs["subscriptionType"] = json!(sub);
    }

    match shape {
        TelemetryShape::ClaudeCode2173 => json!({
            "attributes": attrs,
            "forcedFeatures": {},
        }),
        TelemetryShape::ClaudeCode2185 => json!({
            "attributes": attrs,
            "forcedVariations": {},
            "forcedFeatures": [],
            "url": "",
        }),
    }
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
        MessageCorrelation, MessageTelemetryContext, MessageTelemetryResult, MessageTelemetryUsage,
        TelemetryCorrelationStore, TelemetryEvent, build_event_batch, build_growthbook_eval,
        build_metrics, enqueue_events, internal_event, is_telemetry_path, message_request_events,
        message_result_events, sanitize_telemetry_payload, telemetry_shape_summary,
    };
    use crate::model::account::{
        Account, AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData,
        CanonicalProcessData, CanonicalPromptEnvData,
    };
    use crate::model::identity::run_profile;
    use crate::service::rewriter::ordered_anthropic_headers;
    use crate::service::version_profile::{
        DEFAULT_CLAUDE_CODE_BUILD_TIME, DEFAULT_CLAUDE_CODE_VERSION,
        DEFAULT_CLAUDE_CODE_VERSION_BASE, apply_identity_to_env_json, claude_code_user_agent,
        profile_for_key,
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
            is_running_with_bun: true,
            is_claude_ai_auth: true,
            version: DEFAULT_CLAUDE_CODE_VERSION.into(),
            version_base: DEFAULT_CLAUDE_CODE_VERSION_BASE.into(),
            build_time: DEFAULT_CLAUDE_CODE_BUILD_TIME.into(),
            deployment_environment: "unknown-linux".into(),
            vcs: "git".into(),
            ..Default::default()
        };
        let prompt = CanonicalPromptEnvData {
            shell: "bash".into(),
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
            canonical_prompt: serde_json::to_value(prompt).unwrap(),
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

    fn test_account_with_profile(key: &str) -> Account {
        let mut account = test_account();
        let profile = profile_for_key(key).expect("profile");
        apply_identity_to_env_json(&mut account.canonical_env, &profile.identity);
        account
    }

    fn test_message_context() -> MessageTelemetryContext {
        MessageTelemetryContext {
            request_key: uuid::Uuid::new_v4().to_string(),
            model: "claude-sonnet-4-20250514".into(),
            session_id: Some("session-1".into()),
            pre_normalized_message_count: 1,
            post_normalized_message_count: 2,
            message_content_block_count: 3,
            request_body_bytes: 123,
            rewritten_body_bytes: 456,
            stream: true,
            tool_count: 2,
            attachment_count: 1,
            image_block_count: 1,
            image_total_bytes: 512,
            document_block_count: 1,
            document_total_bytes: 256,
            system_prompt_block_count: 3,
            system_prompt_text_length: 99,
            system_cache_breakpoint_count: 1,
            tool_cache_breakpoint_count: 1,
            message_cache_breakpoint_count: 1,
            thinking_type: Some("adaptive".into()),
            temperature: Some(1.0),
            primary_tool_name: Some("Bash".into()),
            input_text_char_length: 400,
            client_type: "api".into(),
            attempt: 1,
            betas: "claude-code-20250219".into(),
        }
    }

    fn test_message_correlation() -> MessageCorrelation {
        MessageCorrelation {
            query_chain_id: "11111111-2222-4333-8444-555555555555".into(),
            query_depth: 2,
            request_id: "req_123456789012345678901234".into(),
            message_id: "msg_123456789012345678901234".into(),
            previous_request_id: Some("req_abcdefghijklmnopqrstuvwx".into()),
            time_since_last_api_call_ms: Some(321),
        }
    }

    fn encoded_metadata(value: serde_json::Value) -> String {
        let bytes = serde_json::to_vec(&value).unwrap();
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    fn decoded_metadata(event_data: &serde_json::Value) -> serde_json::Value {
        let metadata_b64 = event_data["additional_metadata"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(metadata_b64)
            .unwrap();
        serde_json::from_slice(&decoded).unwrap()
    }

    fn event_data_by_name<'a>(payload: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
        payload["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_data"]["event_name"] == name)
            .map(|event| &event["event_data"])
            .expect("event data")
    }

    #[test]
    fn telemetry_path_accepts_v2_and_legacy() {
        assert!(is_telemetry_path("/api/event_logging/v2/batch"));
        assert!(is_telemetry_path("/api/event_logging/batch"));
        assert!(is_telemetry_path("/api/eval/sdk-zAZezfDKGoZuXXKe"));
    }

    #[test]
    fn telemetry_headers_match_current_wire_profile() {
        let event_headers = super::telemetry_request_headers(
            "/api/event_logging/v2/batch",
            "redacted",
            claude_code_user_agent(DEFAULT_CLAUDE_CODE_VERSION).as_str(),
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
            "Bun/1.4.0",
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
    fn growthbook_eval_contains_current_attributes() {
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap(),
        );
        let payload = build_growthbook_eval(&account, &run);
        assert!(payload.get("forcedVariations").unwrap().is_object());
        assert!(payload.get("forcedFeatures").unwrap().as_array().is_some());
        assert_eq!(payload.get("url").unwrap(), "");
        let attrs = payload.get("attributes").unwrap();
        assert!(attrs.get("email").is_none());
        assert_eq!(attrs.get("id").unwrap(), "device-1");
        assert_eq!(attrs.get("deviceID").unwrap(), "device-1");
        assert_eq!(attrs.get("platform").unwrap(), "linux");
        assert_eq!(attrs.get("accountUUID").unwrap(), "account-uuid");
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
        assert_eq!(attrs.get("subscriptionType").unwrap(), "max");
    }

    #[test]
    fn growthbook_eval_uses_2173_shape_and_profile_version() {
        let account = test_account_with_profile("2.1.173");
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 11, 12, 0, 0).unwrap(),
        );
        let payload = build_growthbook_eval(&account, &run);
        let attrs = payload.get("attributes").unwrap();

        assert_eq!(
            super::growthbook_user_agent_for_account(&account),
            "Bun/1.3.14"
        );
        assert!(payload.get("forcedVariations").is_none());
        assert!(payload.get("forcedFeatures").unwrap().is_object());
        assert!(payload.get("url").is_none());
        assert_eq!(attrs.get("email").unwrap(), "user@example.com");
        assert_eq!(attrs.get("appVersion").unwrap(), "2.1.173");
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
        assert!(event.get("email").is_none());
        assert_eq!(event["session_id"], "session-1");
        assert_eq!(event["auth"]["account_uuid"], "account-uuid");
        assert_eq!(event["auth"]["organization_uuid"], "org-uuid");
        assert!(
            event["betas"]
                .as_str()
                .is_some_and(|betas| !betas.is_empty())
        );
        assert_eq!(event["env"]["version"], DEFAULT_CLAUDE_CODE_VERSION);
        assert_eq!(
            event["env"]["version_base"],
            DEFAULT_CLAUDE_CODE_VERSION_BASE
        );
        assert_eq!(event["env"]["build_time"], DEFAULT_CLAUDE_CODE_BUILD_TIME);
        assert_eq!(event["env"]["shell"], "bash");
        assert_eq!(event["env"]["is_running_with_bun"], true);
        assert_eq!(event["env"]["linux_distro_id"], "ubuntu");

        let metadata_b64 = event["additional_metadata"].as_str().unwrap();
        let decoded_metadata = base64::engine::general_purpose::STANDARD
            .decode(metadata_b64)
            .unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&decoded_metadata).unwrap();
        assert_eq!(metadata["renderer_mode"], "fullscreen");
        assert_eq!(metadata["subscription_type"], "max");
        assert!(metadata.get("provider").is_none());

        let process_b64 = event["process"].as_str().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(process_b64)
            .unwrap();
        let process: serde_json::Value = serde_json::from_slice(&decoded).unwrap();
        assert!(process["heapUsed"].as_i64().unwrap() <= process["heapTotal"].as_i64().unwrap());
        assert_eq!(process["constrainedMemory"], 0);
    }

    #[test]
    fn event_batch_uses_2173_legacy_telemetry_shape() {
        let account = test_account_with_profile("2.1.173");
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 11, 12, 0, 0).unwrap(),
        );
        let events = vec![internal_event(
            "tengu_api_success",
            "claude-sonnet-4-20250514",
            Some("session-1".into()),
        )];
        let payload = build_event_batch(&account, &run, 12.5, &events);
        let event = &payload["events"][0]["event_data"];

        assert_eq!(event["email"], "user@example.com");
        assert_eq!(event["betas"], "");
        assert_eq!(event["additional_metadata"], "");
        assert_eq!(event["env"]["version"], "2.1.173");
        assert_eq!(event["env"]["version_base"], "2.1.173");
        assert_eq!(event["env"]["build_time"], "2026-06-11T01:23:13Z");
        assert!(event["env"].get("shell").is_none());
    }

    #[test]
    fn message_lifecycle_events_are_request_driven() {
        let context = test_message_context();
        let correlation = test_message_correlation();

        let request_events = message_request_events(&context, &correlation);
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
                ttft_ms: Some(456),
                error_kind: None,
                response_body_bytes: Some(2048),
                usage: MessageTelemetryUsage {
                    input_tokens: 100,
                    output_tokens: 25,
                    cache_read_input_tokens: 40,
                    cache_creation_input_tokens: 60,
                    cache_creation_ephemeral_5m_input_tokens: 10,
                    cache_creation_ephemeral_1h_input_tokens: 20,
                },
                stop_reason: Some("end_turn".into()),
            },
            &correlation,
        );
        let result_names: Vec<_> = result_events
            .iter()
            .filter_map(|event| event.name.as_deref())
            .collect();
        assert!(result_names.contains(&"tengu_api_success"));
        assert!(result_names.contains(&"tengu_api_slow_first_byte"));
        assert!(result_names.contains(&"tengu_tool_use_success"));
        let query_event = request_events
            .iter()
            .find(|event| event.name.as_deref() == Some("tengu_api_query"))
            .expect("api query event");
        assert_eq!(
            query_event.fields.get("betas"),
            Some(&json!("claude-code-20250219"))
        );
        let success_event = result_events
            .iter()
            .find(|event| event.name.as_deref() == Some("tengu_api_success"))
            .expect("api success event");
        assert_eq!(
            success_event.fields.get("betas"),
            Some(&json!("claude-code-20250219"))
        );
        assert_eq!(
            success_event.fields.get("requestId"),
            Some(&json!(correlation.request_id))
        );
        assert_eq!(success_event.fields.get("inputTokens"), Some(&json!(100)));
        assert_eq!(
            success_event.fields.get("cachedInputTokens"),
            Some(&json!(40))
        );
    }

    #[test]
    fn message_events_omit_fixed_fake_metadata_fields() {
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 29, 12, 0, 0).unwrap(),
        );
        let context = test_message_context();
        let correlation = test_message_correlation();
        let request_events = message_request_events(&context, &correlation);
        let result_events = message_result_events(
            &context,
            &MessageTelemetryResult {
                status_code: Some(200),
                duration_ms: 4567,
                ttft_ms: Some(321),
                error_kind: None,
                response_body_bytes: Some(999),
                usage: MessageTelemetryUsage::default(),
                stop_reason: Some("end_turn".into()),
            },
            &correlation,
        );

        let request_payload = build_event_batch(&account, &run, 3.0, &request_events);
        let query_metadata =
            decoded_metadata(event_data_by_name(&request_payload, "tengu_api_query"));
        assert!(query_metadata.get("buildAgeMins").is_none());

        let attachment_metadata = decoded_metadata(event_data_by_name(
            &request_payload,
            "tengu_attachment_compute_duration",
        ));
        assert!(attachment_metadata.get("duration_ms").is_none());
        assert_eq!(attachment_metadata["attachment_count"], 1);
        assert_eq!(attachment_metadata["image_block_count"], 1);
        assert_eq!(attachment_metadata["image_total_bytes"], 512);
        assert_eq!(attachment_metadata["document_block_count"], 1);
        assert_eq!(attachment_metadata["document_total_bytes"], 256);
        assert_eq!(attachment_metadata["attachment_size_bytes"], 768);

        let result_payload = build_event_batch(&account, &run, 3.0, &result_events);
        let success_metadata =
            decoded_metadata(event_data_by_name(&result_payload, "tengu_api_success"));
        for key in [
            "buildAgeMins",
            "costUSD",
            "imageTotalPixels",
            "thinkingContentLength",
        ] {
            assert!(success_metadata.get(key).is_none(), "{key}");
        }
        assert_eq!(success_metadata["imageBlockCount"], 1);
        assert_eq!(success_metadata["imageTotalBytes"], 512);
        assert_eq!(success_metadata["documentBlockCount"], 1);
        assert_eq!(success_metadata["documentTotalBytes"], 256);

        let tool_metadata = decoded_metadata(event_data_by_name(
            &result_payload,
            "tengu_tool_use_success",
        ));
        for key in [
            "rssDeltaBytes",
            "heapUsedDeltaBytes",
            "externalDeltaBytes",
            "preToolHookDurationMs",
            "permissionDurationMs",
            "toolInputSizeBytes",
        ] {
            assert!(tool_metadata.get(key).is_none(), "{key}");
        }
        assert_eq!(tool_metadata["toolResultSizeBytes"], 999);
    }

    #[test]
    fn simulated_payload_scan_removes_sensitive_fields_without_recording_values() {
        let contact = format!("{}@{}", "local", "example.invalid");
        let credential = format!("{} {}", "Bearer", "placeholder");
        let cookie = format!("{}={}", "session", "placeholder");
        let account_uuid = uuid::Uuid::new_v4().to_string();
        let prompt_value = "runtime prompt marker".repeat(2);
        let long_text = "x".repeat(super::ADDITIONAL_METADATA_LONG_TEXT_LIMIT + 1);
        let mut payload = json!({
            "events": [{
                "event_type": "tengu_internal",
                "event_data": {
                    "event_name": "tengu_api_success",
                    "event_id": "evt_safe",
                    "session_id": "session-safe",
                    "prompt": prompt_value,
                    "contact": contact,
                    "authorization_hint": credential,
                    "credential_like": credential,
                    "rawUuid": account_uuid,
                    "nested": {
                        "Cookie": cookie
                    },
                    "values": [
                        contact,
                        "kept"
                    ],
                    "additional_metadata": encoded_metadata(json!({
                        "renderer_mode": "fullscreen",
                        "safe_count": 3,
                        "tool_input": {"command": "redacted"},
                        "responseBody": "redacted",
                        "account_uuid": account_uuid,
                        "long_note": long_text,
                        "safe_array": [
                            contact,
                            "kept"
                        ]
                    }))
                }
            }]
        });

        let summary = sanitize_telemetry_payload(&mut payload);
        assert_eq!(summary.event_count, 1);
        assert_eq!(summary.dropped_events, 0);
        assert!(summary.dropped_fields >= 10);
        for reason in [
            "sensitive_key",
            "email_value",
            "credential_value",
            "uuid_value",
            "long_text",
        ] {
            assert!(summary.reason_counts.contains_key(reason), "{reason}");
        }

        let event_data = &payload["events"][0]["event_data"];
        assert!(event_data.get("prompt").is_none());
        assert!(event_data.get("contact").is_none());
        assert!(event_data.get("authorization_hint").is_none());
        assert!(event_data.get("credential_like").is_none());
        assert!(event_data.get("rawUuid").is_none());
        assert!(event_data["nested"].as_object().unwrap().is_empty());
        assert_eq!(event_data["values"], json!(["kept"]));

        let metadata = decoded_metadata(event_data);
        assert_eq!(metadata["renderer_mode"], "fullscreen");
        assert_eq!(metadata["safe_count"], 3);
        assert_eq!(metadata["safe_array"], json!(["kept"]));
        assert!(metadata.get("tool_input").is_none());
        assert!(metadata.get("responseBody").is_none());
        assert!(metadata.get("account_uuid").is_none());
        assert!(metadata.get("long_note").is_none());

        let serialized_payload = serde_json::to_string(&payload).unwrap();
        let logged_drops = format!("{:?}", summary.field_drops);
        for value in [contact, credential, cookie, account_uuid, prompt_value] {
            assert!(!serialized_payload.contains(&value));
            assert!(!logged_drops.contains(&value));
        }
    }

    #[test]
    fn telemetry_shape_summary_contains_only_keys_types_and_drop_counts() {
        let contact = format!("{}@{}", "shape", "example.invalid");
        let mut payload = json!({
            "events": [{
                "event_type": "tengu_internal",
                "event_data": {
                    "event_name": "tengu_api_success",
                    "additional_metadata": encoded_metadata(json!({
                        "safe_count": 3,
                        "safe_flag": true,
                        "contact": contact
                    }))
                }
            }]
        });
        let summary = telemetry_shape_summary(&payload);

        assert_eq!(summary.event_name_counts["tengu_api_success"], 1);
        assert_eq!(
            summary.metadata_keys_by_event["tengu_api_success"]["safe_count"],
            1
        );
        assert_eq!(
            summary.metadata_types_by_event["tengu_api_success"]["safe_count"]["int"],
            1
        );
        assert_eq!(
            summary.metadata_types_by_event["tengu_api_success"]["safe_flag"]["bool"],
            1
        );
        assert!(
            summary.metadata_keys_by_event["tengu_api_success"]
                .get("contact")
                .is_none()
        );
        assert_eq!(summary.dropped_fields, 1);
        assert_eq!(summary.reason_counts["email_value"], 1);
        assert!(format!("{:?}", summary).contains("safe_count"));
        assert!(!format!("{:?}", summary).contains(&contact));

        let scan_summary = sanitize_telemetry_payload(&mut payload);
        assert_eq!(scan_summary.dropped_fields, summary.dropped_fields);
    }

    #[test]
    fn correlation_fallback_uses_stable_run_session_key() {
        let mut store = TelemetryCorrelationStore::default();
        let mut first_context = test_message_context();
        first_context.session_id = None;
        first_context.request_key = "request-one".into();
        let mut second_context = first_context.clone();
        second_context.request_key = "request-two".into();

        let first = store.register_request(&first_context, "run-session-fixed");
        let second = store.register_request(&second_context, "run-session-fixed");

        assert_eq!(first.query_chain_id, second.query_chain_id);
        assert_eq!(second.query_depth, first.query_depth + 1);
        assert_eq!(
            second.previous_request_id.as_deref(),
            Some(first.request_id.as_str())
        );
    }

    #[test]
    fn event_batch_does_not_include_raw_prompt_or_token() {
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 4, 12, 0, 0).unwrap(),
        );
        let mut context = test_message_context();
        context.tool_count = 0;
        context.attachment_count = 0;
        context.betas = "server-side-fallback-2026-06-01,fallback-credit-2026-06-01".into();
        let events = message_request_events(&context, &test_message_correlation());
        let payload = build_event_batch(&account, &run, 3.0, &events);
        let serialized = serde_json::to_string(&payload).unwrap();

        assert!(!serialized.contains("raw-token-marker"));
        assert!(!serialized.contains("raw-prompt-marker"));
        assert!(!serialized.contains("raw-tool-input-marker"));
        assert!(!serialized.contains("request_body_bytes"));
    }

    #[test]
    fn event_batch_places_request_metadata_inside_additional_metadata() {
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 29, 12, 0, 0).unwrap(),
        );
        let context = test_message_context();
        let correlation = test_message_correlation();
        let events = message_result_events(
            &context,
            &MessageTelemetryResult {
                status_code: Some(200),
                duration_ms: 4567,
                ttft_ms: Some(321),
                error_kind: None,
                response_body_bytes: Some(999),
                usage: MessageTelemetryUsage {
                    input_tokens: 120,
                    output_tokens: 30,
                    cache_read_input_tokens: 50,
                    cache_creation_input_tokens: 70,
                    cache_creation_ephemeral_5m_input_tokens: 11,
                    cache_creation_ephemeral_1h_input_tokens: 22,
                },
                stop_reason: Some("end_turn".into()),
            },
            &correlation,
        );
        let payload = build_event_batch(&account, &run, 3.0, &events);
        let event = &payload["events"][0]["event_data"];
        let metadata_b64 = event["additional_metadata"].as_str().unwrap();
        let decoded_metadata = base64::engine::general_purpose::STANDARD
            .decode(metadata_b64)
            .unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(&decoded_metadata).unwrap();

        assert_eq!(metadata["renderer_mode"], "fullscreen");
        assert_eq!(metadata["provider"], "firstParty");
        assert_eq!(metadata["requestId"], correlation.request_id);
        assert_eq!(metadata["messageTokens"], 150);
        assert_eq!(metadata["cachedInputTokens"], 50);
        assert_eq!(metadata["stop_reason"], "end_turn");
        assert!(event.get("requestId").is_none());
        assert!(event.get("inputTokens").is_none());
        assert!(event.get("durationMs").is_none());
    }

    #[test]
    fn fable_message_telemetry_uses_request_model_and_betas_without_cli_flag() {
        let account = test_account();
        let run = run_profile(
            &account,
            Utc.with_ymd_and_hms(2026, 6, 10, 16, 30, 37).unwrap(),
        );
        let mut context = test_message_context();
        context.model = "claude-fable-5".into();
        context.tool_count = 0;
        context.attachment_count = 0;
        context.betas = "server-side-fallback-2026-06-01,fallback-credit-2026-06-01".into();
        let events = message_request_events(&context, &test_message_correlation());
        let payload = build_event_batch(&account, &run, 3.0, &events);
        let event = payload["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["event_data"]["event_name"] == "tengu_api_query")
            .map(|event| &event["event_data"])
            .expect("query event");
        let serialized = serde_json::to_string(&payload).unwrap();

        assert_eq!(event["model"], json!("claude-fable-5"));
        assert_eq!(
            event["betas"],
            json!("server-side-fallback-2026-06-01,fallback-credit-2026-06-01")
        );
        assert!(!serialized.contains(r#""flags":"model""#));
        assert!(!serialized.contains(r#""flag_count":1"#));
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
