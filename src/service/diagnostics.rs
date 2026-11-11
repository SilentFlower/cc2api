//! 请求诊断旁路：使用请求作用域关联重试，响应观察不改变转发字节。

use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::Response;
use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use http_body::{Body as HttpBody, Frame, SizeHint};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::error::AppError;
use crate::model::account::Account;
use crate::model::identity::device_profile;
use crate::model::request_diagnostics::{
    AttemptDiagnostic, DiagnosticParameters, DiagnosticSessions, DiagnosticTokens,
    NativeRequestObservation, RequestDiagnostic, RequestLogMode, normalize_correlation_id,
    safe_model,
};
use crate::service::rewriter::extract_session_id_from_body;
use crate::service::usage::UsageCapture;
use crate::service::v3_scheduling::SchedulingRequest;
use crate::service::version_profile::profile_for_version;
use crate::store::cache::UpstreamSessionPoolResolve;
use crate::store::diagnostics_store::{ActiveDiagnostic, DiagnosticEvent, DiagnosticsStore};

tokio::task_local! {
    /// 独立于诊断存储的请求模式，保证旁路故障时内容策略仍生效。
    pub(crate) static CURRENT_LOG_MODE: RequestLogMode;
    /// 只在当前请求的异步调用链中可见，不向后台遥测任务传播。
    pub(crate) static CURRENT_TRACE: TraceContext;
}

/// 获取入口冻结模式；独立后台任务默认沿摘要策略。
/// @return 当前请求模式。
pub(crate) fn current_log_mode() -> RequestLogMode {
    CURRENT_LOG_MODE.try_with(|mode| *mode).unwrap_or_default()
}

/// 返回当前消息请求的根标识，不创建新的诊断请求。
/// @return 无请求作用域时为空。
pub(crate) fn current_request_id() -> Option<String> {
    CURRENT_TRACE
        .try_with(|trace| trace.lock().request.request_id.clone())
        .ok()
}

struct TraceState {
    retention: Option<ActiveDiagnostic>,
    request: RequestDiagnostic,
    profile_selection: String,
    sessions: DiagnosticSessions,
    local_outcome: Option<&'static str>,
    next_retry: Option<&'static str>,
    last_attempt: Option<(u32, String, Option<String>)>,
}

/// 可跨响应生命周期持有的请求上下文，只存白名单摘要。
#[derive(Clone)]
pub(crate) struct TraceContext {
    store: Arc<DiagnosticsStore>,
    state: Arc<Mutex<TraceState>>,
}

impl TraceContext {
    /// 创建内部请求 UUID 并记录采集开始。
    /// @param store 独立诊断存储。
    /// @return 当前请求的观测上下文。
    pub(crate) fn new(store: Arc<DiagnosticsStore>) -> Self {
        let request_id = Uuid::new_v4().to_string();
        let retention = store.track_request(&request_id);
        let trace = Self {
            store,
            state: Arc::new(Mutex::new(TraceState {
                retention: Some(retention),
                request: RequestDiagnostic {
                    log_mode: Some(current_log_mode()),
                    request_id,
                    started_at: now(),
                    finished_at: None,
                    status_code: None,
                    account_id: None,
                    response_request_id: None,
                    outcome: "in_progress".into(),
                    error_stage: None,
                    attempt_count: 0,
                },
                profile_selection: "unresolved".into(),
                sessions: DiagnosticSessions::default(),
                local_outcome: None,
                next_retry: None,
                last_attempt: None,
            })),
        };
        trace.emit(None);
        trace
    }

    fn lock(&self) -> MutexGuard<'_, TraceState> {
        // 观测锁不包裹 await；即使异常展开污染锁，也不能中断业务转发。
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn emit(&self, attempt: Option<AttemptDiagnostic>) {
        self.store.submit(DiagnosticEvent {
            request: self.lock().request.clone(),
            attempt,
        });
    }

    fn finish(&self, end: &'static str) {
        {
            let mut state = self.lock();
            if state.request.finished_at.is_some() {
                return;
            }
            let success = state
                .request
                .status_code
                .is_some_and(|status| (200..300).contains(&status));
            state.request.outcome = if end != "complete" {
                end.into()
            } else if !success && state.request.attempt_count == 0 {
                "local_rejected".into()
            } else if let Some(local) = state.local_outcome {
                local.into()
            } else if let Some((_, result, _)) = &state.last_attempt {
                result.clone()
            } else if state.request.attempt_count == 0 {
                "local_response".into()
            } else {
                "incomplete".into()
            };
            if !success && state.request.outcome == "success" {
                state.request.outcome = "local_rejected".into();
            }
            if ["success", "mock", "cache_hit"].contains(&state.request.outcome.as_str()) {
                state.request.error_stage = None;
            }
            if state.request.error_stage.is_none()
                && !["success", "mock", "cache_hit"].contains(&state.request.outcome.as_str())
            {
                state.request.error_stage = if end != "complete" {
                    if end == "body_error" {
                        state
                            .last_attempt
                            .as_ref()
                            .and_then(|(_, _, stage)| stage.clone())
                            .or_else(|| Some("downstream_body".into()))
                    } else {
                        Some("downstream_body".into())
                    }
                } else {
                    state
                        .last_attempt
                        .as_ref()
                        .and_then(|(_, _, stage)| stage.clone())
                };
            }
            if state.request.outcome == "local_rejected" && state.request.error_stage.is_none() {
                state.request.error_stage = Some("admission".into());
            }
            state.request.finished_at = Some(now());
            state.retention.take();
        }
        self.emit(None);
    }
}

/// 设置请求的固定画像选择方式，不在结束时重读配置。
/// @param client_version 是否按客户端版本选择画像。
/// @return 无返回值。
pub(crate) fn profile_selection(client_version: bool) {
    let _ = CURRENT_TRACE.try_with(|trace| {
        trace.lock().profile_selection = if client_version {
            "client_version"
        } else {
            "account"
        }
        .into()
    });
}

/// 记录当前选择的账号，包含选中后由本地返回的请求。
/// @param account_id 本轮调度实际选中的账号。
/// @return 无返回值。
pub(crate) fn selected_account(account_id: i64) {
    let _ = CURRENT_TRACE.try_with(|trace| trace.lock().request.account_id = Some(account_id));
}

/// 标记本地响应的来源，避免把模拟或缓存命中记为上游推理。
/// @param outcome 代码内固定的结果枚举。
/// @return 无返回值。
pub(crate) fn local_outcome(outcome: &'static str) {
    let _ = CURRENT_TRACE.try_with(|trace| trace.lock().local_outcome = Some(outcome));
}

/// 记录即将执行的重试原因或错误阶段。
/// @param reason 代码内固定原因，不接受上游任意错误文本。
/// @return 无返回值。
pub(crate) fn retry_reason(reason: &'static str) {
    let _ = CURRENT_TRACE.try_with(|trace| trace.lock().next_retry = Some(reason));
}

/// 记录本地准备或授权恢复错误阶段。
/// @param stage 代码内固定阶段。
/// @return 无返回值。
pub(crate) fn error_stage(stage: &'static str) {
    let _ = CURRENT_TRACE.try_with(|trace| trace.lock().request.error_stage = Some(stage.into()));
}

/// 记录既有 session 解析结果，绝不额外解析或刷新池 TTL。
/// @param account_id 已选定账号。
/// @param original 原始消息体中的真实 session。
/// @param resolved 已有上游池解析结果。
/// @return 无返回值。
pub(crate) fn session_context(
    account_id: i64,
    original: Option<&str>,
    resolved: Option<&UpstreamSessionPoolResolve>,
) {
    let _ = CURRENT_TRACE.try_with(|trace| {
        trace.lock().sessions = DiagnosticSessions {
            real_session_hash: original.map(|value| identity_hash(account_id, "session", value)),
            upstream_session_hash: resolved
                .and_then(|value| value.upstream_session_id.as_deref())
                .or(original)
                .map(|value| identity_hash(account_id, "session", value)),
            mapping_action: Some(
                resolved
                    .map(|value| value.action.as_str())
                    .unwrap_or("passthrough")
                    .into(),
            ),
            previous_message_hash: None,
        };
    });
}

/// 一次真实发送的观察器，离开作用域时也会记录取消。
pub(crate) struct AttemptTrace {
    trace: TraceContext,
    record: Option<AttemptDiagnostic>,
    capture: Option<UsageCapture>,
    start: Instant,
    committed: bool,
    feedback: Option<SchedulingRequest>,
}

/// 预处理白名单观察参数；尚不记录 attempt，丢弃也不记取消。
/// @param account 实际用于协议改写的账号副本。
/// @param body 最终上游请求字节，只临时解析，不持久化。
/// @return 有请求作用域时返回未提交的观察器。
pub(crate) fn prepare_attempt(account: &Account, body: &[u8]) -> Option<AttemptTrace> {
    CURRENT_TRACE.try_with(|_| ()).ok()?;
    let body: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    prepare_observation(account, body)
}

// 观察器只读取明确来源的临时参数，完整业务正文不进入诊断存储。
fn prepare_observation(account: &Account, body: Value) -> Option<AttemptTrace> {
    CURRENT_TRACE
        .try_with(|trace| {
            let state = trace.lock();
            let mut sessions = state.sessions.clone();
            sessions.upstream_session_hash = extract_session_id_from_body(&body)
                .map(|value| identity_hash(account.id, "session", &value));
            sessions.previous_message_hash = body
                .pointer("/thread/previous_message_id")
                .and_then(Value::as_str)
                .map(|value| identity_hash(account.id, "message", value));
            let record = AttemptDiagnostic {
                runtime: None,
                log_mode: state.request.log_mode,
                capture_status: None,
                attempt_id: Uuid::new_v4().to_string(),
                request_id: state.request.request_id.clone(),
                ordinal: 0,
                account_id: account.id,
                profile_version: profile_for_version(&device_profile(account).env.version)
                    .key
                    .into(),
                profile_selection: state.profile_selection.clone(),
                parameters: DiagnosticParameters::from_request(&body),
                sessions,
                started_at: now(),
                finished_at: None,
                ttft_ms: None,
                first_output_ms: None,
                duration_ms: None,
                status_code: None,
                upstream_request_id: None,
                response_model: None,
                response_message_hash: None,
                stop_reason: None,
                tokens: DiagnosticTokens::default(),
                partial: true,
                outcome: "in_progress".into(),
                error_stage: None,
                error_type: None,
                retry_reason: None,
            };
            drop(state);
            AttemptTrace {
                trace: trace.clone(),
                record: Some(record),
                capture: Some(UsageCapture::observer(&body)),
                start: Instant::now(),
                committed: false,
                feedback: None,
            }
        })
        .ok()
}

/// 从固定运行时组装阶段的实际白名单摘要建立诊断，来源不冒充最终写出。
/// @param account 实际承载账号。
/// @param observation sender 从 cc_prepared 提取的有界摘要。
/// @param attempt_id 私有作业及 sender 证据共用的 UUID。
/// @return 未提交的原生观察器，摘要损坏时保持缺口可见。
pub(crate) fn prepare_native_attempt(
    account: &Account,
    observation: NativeRequestObservation,
    attempt_id: &str,
) -> Option<AttemptTrace> {
    if Uuid::parse_str(attempt_id).is_err()
        || observation.runtime.as_ref().is_some_and(|runtime| {
            Uuid::parse_str(&runtime.worker_id).is_err()
                || runtime.worker_pid == 0
                || runtime
                    .kernel_generation
                    .as_ref()
                    .is_some_and(|id| Uuid::parse_str(id).is_err())
                || runtime
                    .preparation_wait_ms
                    .saturating_add(runtime.checkout_ms)
                    > runtime.prepared_ms.saturating_add(2)
                || runtime
                    .worker_start_ms
                    .is_some_and(|elapsed| elapsed > runtime.checkout_ms.saturating_add(2))
        })
        || observation.schema_version != 1
        || observation.body_sha256.len() != 64
        || !observation
            .body_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || observation
            .session_id
            .as_ref()
            .is_some_and(|id| id.len() > 512)
        || observation
            .previous_message_id
            .as_ref()
            .is_some_and(|id| id.len() > 512)
    {
        return None;
    }
    let session_mismatch = CURRENT_TRACE
        .try_with(|trace| {
            let expected = trace.lock().sessions.upstream_session_hash.clone();
            let actual = observation
                .session_id
                .as_deref()
                .map(|session| identity_hash(account.id, "session", session));
            expected.is_some() && expected != actual
        })
        .unwrap_or(false);
    if session_mismatch {
        error_stage("native_session_observation");
        return None;
    }
    let context = serde_json::json!({"model":observation.parameters.model,"stream":observation.parameters.stream,
        "metadata":{"user_id":serde_json::json!({"session_id":observation.session_id}).to_string()},
        "thread":{"previous_message_id":observation.previous_message_id}});
    let mut attempt = prepare_observation(account, context)?;
    let record = attempt.record.as_mut()?;
    record.attempt_id = attempt_id.to_owned();
    record.profile_version = "2.1.293".into();
    record.profile_selection = "native293_cc_prepared".into();
    record.parameters = observation.parameters;
    record.capture_status = observation.capture_status;
    record.runtime = observation.runtime;
    Some(attempt)
}

/// 保留旧观察器测试的即时开始入口。
/// @param account 合成账号。
/// @param body 合成请求正文。
/// @return 已提交的测试观察器。
#[cfg(test)]
fn begin_attempt(account: &Account, body: &[u8]) -> Option<AttemptTrace> {
    let mut attempt = prepare_attempt(account, body)?;
    attempt.commit();
    Some(attempt)
}

impl AttemptTrace {
    /// 在真正提交边界发布一次 attempt；大正文解析已在准入前完成。
    /// @return 无，重复调用不重复计数。
    pub(crate) fn commit(&mut self) {
        self.commit_started(Instant::now(), now());
    }

    /// 原生摘要在响应头时回传，保留实际提交起点并只增加一次尝试。
    /// @param started 网关开始该次原生提交的单调时间。
    /// @return 无。
    pub(crate) fn commit_native(&mut self, started: Instant) {
        let timestamp = (Utc::now()
            - ChronoDuration::from_std(started.elapsed()).unwrap_or_default())
        .to_rfc3339_opts(SecondsFormat::Millis, true);
        self.commit_started(started, timestamp);
    }

    fn commit_started(&mut self, started: Instant, timestamp: String) {
        if self.committed {
            return;
        }
        let Some(record) = self.record.as_mut() else {
            return;
        };
        let mut state = self.trace.lock();
        state.request.attempt_count = state.request.attempt_count.saturating_add(1);
        state.request.error_stage = None;
        record.ordinal = state.request.attempt_count;
        record.retry_reason = state.next_retry.take().map(str::to_owned);
        record.started_at = timestamp;
        self.start = started;
        self.committed = true;
        drop(state);
        self.trace.emit(Some(record.clone()));
    }

    /// 让现有响应观察器在完整结束后回灌反馈，不新增 SSE 解析路径。
    /// @param request 当前调度租约。
    /// @return 无。
    pub(crate) fn with_feedback(&mut self, request: SchedulingRequest) {
        self.feedback = Some(request);
    }

    /// 返回供 sender 证据复用的内部 UUID。
    /// @return 当前尝试 UUID。
    pub(crate) fn id(&self) -> &str {
        self.record
            .as_ref()
            .map(|record| record.attempt_id.as_str())
            .unwrap_or_default()
    }

    /// 记录实际收到的响应头摘要，不复制任何完整 headers。
    /// @param status 上游状态码。
    /// @param headers 原始响应头。
    /// @param codings 既有传输路径解析的编码顺序。
    /// @return 无返回值。
    pub(crate) fn headers(&mut self, status: u16, headers: &HeaderMap, codings: Vec<String>) {
        if let Some(record) = self.record.as_mut() {
            record.status_code = Some(status);
            record.ttft_ms = Some(self.start.elapsed().as_millis() as u64);
            record.upstream_request_id = ["request-id", "x-request-id"]
                .iter()
                .find_map(|name| headers.get(*name).and_then(|v| v.to_str().ok()))
                .map(normalize_correlation_id);
        }
        if let Some(capture) = self.capture.as_mut() {
            capture.configure_codings(&codings);
        }
        if let Some(record) = self.record.as_ref() {
            self.trace.emit(Some(record.clone()));
        }
    }

    fn observe(&mut self, bytes: &[u8]) {
        if let Some(capture) = self.capture.as_mut() {
            capture.observe(bytes);
        }
    }

    /// 观察既有分支已缓冲的响应，不额外读取网络或修改编码。
    /// @param bytes 上游原始字节。
    /// @param complete 原有读取是否正常完成。
    /// @return 无返回值。
    pub(crate) fn buffered(&mut self, bytes: &[u8], complete: bool) {
        self.observe(bytes);
        self.finish(if complete { "complete" } else { "body_error" });
    }

    /// 记录传输错误，提交状态未知时不伪造上游 HTTP 状态。
    /// @return 无返回值。
    pub(crate) fn transport_error(&mut self) {
        self.finish("transport_error");
    }

    /// 把观察器绑定到原始响应体生命周期。
    /// @param response 保持字节和传输头不变的响应。
    /// @return 具有相同响应内容的旁路包装。
    pub(crate) fn wrap(self, response: Response) -> Response {
        let (parts, body) = response.into_parts();
        Response::from_parts(
            parts,
            Body::new(DiagnosticBody {
                inner: body,
                request: None,
                attempt: Some(self),
            }),
        )
    }

    fn finish(&mut self, end: &'static str) {
        if !self.committed {
            self.record.take();
            self.capture.take();
            return;
        }
        let Some(mut record) = self.record.take() else {
            return;
        };
        if let Some(capture) = self.capture.take() {
            let observed = capture.observation(end == "complete");
            record.first_output_ms = observed.first_output_at.map(|at| {
                at.saturating_duration_since(self.start)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64
            });
            record.tokens = observed.tokens.into();
            record.partial = observed.partial;
            record.response_model = observed.response_model.as_deref().and_then(safe_model);
            record.response_message_hash = observed
                .message_id
                .as_deref()
                .map(|id| identity_hash(record.account_id, "message", id));
            record.stop_reason = observed.stop_reason;
            if observed.observation_error.is_some() {
                self.trace.store.observation_failed();
            }
            record.error_type = observed
                .error_type
                .or_else(|| observed.observation_error.map(str::to_owned));
        }
        record.outcome = if end != "complete" {
            end.into()
        } else if record.status_code.is_some_and(|value| value >= 400) {
            "upstream_rejected".into()
        } else if record.error_type.as_deref().is_some_and(|kind| {
            !["sampling_limit", "decode_error", "invalid_payload"].contains(&kind)
        }) {
            "upstream_error".into()
        } else if record.partial {
            "incomplete".into()
        } else {
            "success".into()
        };
        record.error_stage = match record.outcome.as_str() {
            "success" => None,
            "transport_error" => Some("transport".into()),
            "upstream_rejected" => Some("upstream_headers".into()),
            _ => Some("upstream_body".into()),
        };
        if let Some(feedback) = self.feedback.take() {
            feedback.finish_observed_attempt(
                record.account_id,
                record.status_code,
                &record.outcome,
                record.error_type.as_deref(),
                record.first_output_ms,
            );
        }
        record.finished_at = Some(now());
        record.duration_ms = Some(self.start.elapsed().as_millis() as u64);
        {
            let mut state = self.trace.lock();
            if state
                .last_attempt
                .as_ref()
                .is_none_or(|(ordinal, _, _)| *ordinal <= record.ordinal)
            {
                state.last_attempt = Some((
                    record.ordinal,
                    record.outcome.clone(),
                    record.error_stage.clone(),
                ));
            }
        }
        self.trace.emit(Some(record));
    }
}

impl Drop for AttemptTrace {
    fn drop(&mut self) {
        self.finish("cancelled");
    }
}

/// 请求准备期和下游响应期共用的终结保护，覆盖客户端提前取消。
pub(crate) struct RequestTraceGuard {
    trace: TraceContext,
    active: bool,
}

impl RequestTraceGuard {
    /// 持有请求终结责任。
    /// @param trace 当前请求上下文。
    /// @return 生命周期保护器。
    pub(crate) fn new(trace: TraceContext) -> Self {
        Self {
            trace,
            active: true,
        }
    }

    /// 记录受控本地错误类别，不读取错误中的任意文本。
    /// @param error 网关返回的原始错误。
    /// @return 无返回值。
    pub(crate) fn error(&self, error: &AppError) {
        let stage = match error {
            AppError::AdmissionLimited { .. } | AppError::SchedulingConflict => "admission",
            AppError::BadRequest(_) => "request",
            AppError::Unauthorized | AppError::PermanentCredential(_) => "credentials",
            AppError::TooManyRequests(_) => "admission",
            AppError::BadGateway(_) => "transport",
            AppError::ServiceUnavailable(_) => "account_selection",
            AppError::Internal(_) | AppError::NotFound => "gateway",
        };
        let mut state = self.trace.lock();
        if state.request.error_stage.is_none() {
            state.request.error_stage = Some(stage.into());
        }
    }

    /// 附加下游关联头并把终结责任移交给响应体。
    /// @param response 原有网关响应。
    /// @return 保留已有上游请求 ID 的响应。
    pub(crate) fn wrap(self, mut response: Response) -> Response {
        let id = self.trace.lock().request.request_id.clone();
        if let Ok(value) = HeaderValue::from_str(&id) {
            response
                .headers_mut()
                .insert("x-cc2api-request-id", value.clone());
            if !response.headers().contains_key("x-request-id") {
                response.headers_mut().insert("x-request-id", value);
            }
        }
        {
            let mut state = self.trace.lock();
            state.request.status_code = Some(response.status().as_u16());
            state.request.response_request_id = response
                .headers()
                .get("x-request-id")
                .and_then(|v| v.to_str().ok())
                .map(normalize_correlation_id);
        }
        let (parts, body) = response.into_parts();
        Response::from_parts(
            parts,
            Body::new(DiagnosticBody {
                inner: body,
                request: Some(self),
                attempt: None,
            }),
        )
    }

    fn finish(&mut self, end: &'static str) {
        if self.active {
            self.active = false;
            self.trace.finish(end);
        }
    }
}

impl Drop for RequestTraceGuard {
    fn drop(&mut self) {
        self.finish("cancelled");
    }
}

struct DiagnosticBody {
    inner: Body,
    request: Option<RequestTraceGuard>,
    attempt: Option<AttemptTrace>,
}

impl DiagnosticBody {
    fn finish(&mut self, end: &'static str) {
        if let Some(mut attempt) = self.attempt.take() {
            attempt.finish(end);
        }
        if let Some(mut request) = self.request.take() {
            request.finish(end);
        }
    }
}

impl HttpBody for DiagnosticBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                if let (Some(attempt), Some(bytes)) = (self.attempt.as_mut(), frame.data_ref()) {
                    attempt.observe(bytes);
                }
                if self.inner.is_end_stream() {
                    self.finish("complete");
                }
            }
            Poll::Ready(None) => self.finish("complete"),
            Poll::Ready(Some(Err(_))) => self.finish("body_error"),
            Poll::Pending => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for DiagnosticBody {
    fn drop(&mut self) {
        self.finish(if self.inner.is_end_stream() {
            "complete"
        } else {
            "cancelled"
        });
    }
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn identity_hash(account: i64, kind: &str, value: &str) -> String {
    let digest =
        Sha256::digest(format!("cc2api-diagnostics-v1:{account}:{kind}:{value}").as_bytes());
    hex::encode(&digest[..16])
}

#[cfg(test)]
mod tests {
    use super::{
        CURRENT_TRACE, RequestTraceGuard, TraceContext, begin_attempt, now, profile_selection,
        selected_account, session_context,
    };
    use crate::model::account::Account;
    use crate::model::request_diagnostics::{AttemptDiagnostic, DiagnosticQuery};
    use crate::service::usage::USAGE_BUFFER_LIMIT;
    use crate::service::v3_scheduling::{SchedulingCandidate, V3Scheduler, now_ms};
    use crate::store::cache::{UpstreamSessionPoolAction, UpstreamSessionPoolResolve};
    use crate::store::diagnostics_store::DiagnosticsStore;
    use crate::store::scheduling_store::SchedulingStore;
    use axum::body::{Body, Bytes, to_bytes};
    use axum::response::Response;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use http_body_util::BodyExt;
    use serde_json::json;
    use sqlx::any::AnyPoolOptions;
    use std::convert::Infallible;
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_stream::StreamExt;

    async fn store() -> Arc<DiagnosticsStore> {
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        DiagnosticsStore::migrate(&pool).await.unwrap();
        Arc::new(DiagnosticsStore::new(pool))
    }

    fn account() -> Account {
        serde_json::from_value(json!({
            "id":7,"name":"合成测试","email":"fixture@example.invalid","status":"active","device_id":"synthetic-device",
            "canonical_env":{"version":"2.1.293"},"canonical_prompt_env":{},"canonical_process":{},"billing_mode":"strip",
            "created_at":now(),"updated_at":now()
        })).unwrap()
    }

    /// 头部立即到达但首内容延迟时，两项时间分开记录，响应字节保持不变且旧记录可读。
    #[tokio::test]
    async fn first_output_diagnostics_measure_content_after_headers_and_read_old_records() {
        let store = store().await;
        let trace = TraceContext::new(store.clone());
        let guard = RequestTraceGuard::new(trace.clone());
        let scheduler = Arc::new(V3Scheduler::new(Arc::new(SchedulingStore::new(
            AnyPoolOptions::new()
                .connect_lazy("sqlite::memory:")
                .unwrap(),
        ))));
        let first = b"data: {\"type\":\"ping\"}\n\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n";
        let output = b"data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"synthetic\"}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":1}}\n\ndata: {\"type\":\"message_stop\"}\n\n";
        let response = CURRENT_TRACE
            .scope(trace, async {
                let mut attempt = begin_attempt(&account(), br#"{"stream":true}"#).unwrap();
                attempt.with_feedback(scheduler.begin(0, None));
                let stream = tokio_stream::once(Ok::<_, Infallible>(Bytes::from_static(first)))
                    .chain(tokio_stream::once(Ok(Bytes::from_static(output))).then(
                        |part| async move {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            part
                        },
                    ));
                let response = Response::new(Body::from_stream(stream));
                attempt.headers(200, response.headers(), vec![]);
                guard.wrap(attempt.wrap(response))
            })
            .await;
        let id = response.headers()["x-cc2api-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            to_bytes(response.into_body(), 65536)
                .await
                .unwrap()
                .as_ref(),
            [first.as_slice(), output.as_slice()].concat()
        );
        store.flush().await.unwrap();
        let detail = store.detail(&id).await.unwrap();
        let record = &detail.attempts[0];
        assert_eq!(record.outcome, "success");
        assert!(record.first_output_ms.unwrap() >= record.ttft_ms.unwrap() + 20);
        let candidate = SchedulingCandidate {
            account: account(),
            active_units: 0,
            max_units: 8,
            queued: 0,
            queued_units: 0,
            legacy_score: 0.0,
            next_send_ms: 0,
        };
        let factor = scheduler
            .factors(&candidate, &scheduler.config(), now_ms())
            .latency;
        assert!(
            (factor - 1.0 / (1.0 + record.first_output_ms.unwrap() as f64 / 10000.0)).abs() < 1e-9
        );
        let mut old = serde_json::to_value(record).unwrap();
        old.as_object_mut().unwrap().remove("first_output_ms");
        let old: AttemptDiagnostic = serde_json::from_value(old).unwrap();
        assert_eq!(old.first_output_ms, None);
        assert_eq!(old.ttft_ms, record.ttft_ms);
    }

    /// 响应包装在请求作用域外仍可收口，原始字节与头值保持不变，记录仅含白名单。
    #[tokio::test]
    async fn diagnostics_preserve_bytes_and_correlate_anonymous_sessions() {
        let store = store().await;
        let trace = TraceContext::new(store.clone());
        let guard = RequestTraceGuard::new(trace.clone());
        let response = CURRENT_TRACE.scope(trace, async {
            profile_selection(true);
            selected_account(7);
            let resolved = UpstreamSessionPoolResolve {real_session_id:"source-session-secret".into(),upstream_session_id:Some("shared-session-secret".into()),
                action:UpstreamSessionPoolAction::Mapped,active_count:4,oldest_last_seen_ms:None,newest_last_seen_ms:None};
            session_context(7, Some("source-session-secret"), Some(&resolved));
            let request = json!({"model":"claude-sonnet-5-5","max_tokens":4096,"thinking":{"type":"enabled","budget_tokens":1024},
                "output_config":{"effort":"high"},"stream":false,"tools":[{"input_schema":{"secret":"tool-input-secret"}}],
                "system":"prompt-secret","messages":[{"role":"user","content":"prompt-secret"}],
                "metadata":{"user_id":json!({"session_id":"shared-session-secret"}).to_string()},"thread":{"previous_message_id":"msg_parent_secret"}});
            let mut attempt = begin_attempt(&account(), &serde_json::to_vec(&request).unwrap()).unwrap();
            let response = Response::builder().header("x-request-id","header-secret").header("request-id","req_fixture123")
                .body(Body::from(r#"{"id":"msg_response_secret","type":"message","model":"claude-sonnet-5-5","stop_reason":"end_turn","content":[{"text":"response-secret"}],"usage":{"input_tokens":123,"output_tokens":8}}"#)).unwrap();
            attempt.headers(200, response.headers(), vec![]);
            guard.wrap(attempt.wrap(response))
        }).await;
        let id = response.headers()["x-cc2api-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(response.headers()["x-request-id"], "header-secret");
        let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("response-secret"));
        store.flush().await.unwrap();
        let detail = store.detail(&id).await.unwrap();
        assert_eq!(detail.request.outcome, "success");
        assert_eq!(detail.attempts.len(), 1);
        let attempt = &detail.attempts[0];
        assert_eq!(attempt.profile_version, "2.1.293");
        assert_eq!(attempt.profile_selection, "client_version");
        assert_eq!(attempt.parameters.max_tokens, Some(4096));
        assert_eq!(attempt.parameters.thinking_budget, Some(1024));
        assert_eq!(attempt.parameters.effort.as_deref(), Some("high"));
        assert_eq!(attempt.parameters.tools_count, Some(1));
        assert_eq!(attempt.tokens.input_tokens, Some(123));
        assert_eq!(attempt.tokens.output_tokens, Some(8));
        assert_eq!(attempt.tokens.cache_read_tokens, None);
        assert_eq!(attempt.stop_reason.as_deref(), Some("end_turn"));
        assert_eq!(attempt.sessions.mapping_action.as_deref(), Some("mapped"));
        assert_ne!(
            attempt.sessions.real_session_hash,
            attempt.sessions.upstream_session_hash
        );
        assert!(attempt.response_message_hash.is_some());
        let serialized = serde_json::to_string(&detail).unwrap();
        for secret in [
            "source-session-secret",
            "shared-session-secret",
            "msg_parent_secret",
            "msg_response_secret",
            "tool-input-secret",
            "prompt-secret",
            "response-secret",
            "header-secret",
            "synthetic-device",
            "fixture@example.invalid",
        ] {
            assert!(!serialized.contains(secret), "诊断不应含合成秘密：{secret}");
        }
        let page = store
            .list(DiagnosticQuery {
                correlation_id: Some("header-secret".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.items[0].request_id, id);
    }

    /// SSE 内错误、读取异常及客户端取消必须独立于 HTTP 200 判定。
    #[tokio::test]
    async fn diagnostics_record_stream_errors_and_cancellation_once() {
        let store = store().await;
        for end in ["sse_error", "read_error", "cancelled"] {
            let trace = TraceContext::new(store.clone());
            let guard = RequestTraceGuard::new(trace.clone());
            let response=CURRENT_TRACE.scope(trace,async {
                let mut attempt=begin_attempt(&account(),br#"{"model":"claude-sonnet-5","stream":true}"#).unwrap();
                let first=Bytes::from_static(b"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n");
                let body=if end=="sse_error" {
                    let tail=Bytes::from_static(b"data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"arbitrary-error-secret\"}}\n\n");
                    Body::from_stream(tokio_stream::iter(vec![Ok::<_,Infallible>(first),Ok(tail)]))
                } else if end=="read_error" {
                    Body::from_stream(tokio_stream::iter(vec![Ok(first),Err(std::io::Error::other("arbitrary-error-secret"))]))
                } else {
                    Body::from_stream(tokio_stream::iter(vec![Ok::<_,Infallible>(first)]).chain(tokio_stream::pending()))
                };
                let response=Response::new(body);
                attempt.headers(200,response.headers(),vec![]);
                guard.wrap(attempt.wrap(response))
            }).await;
            let id = response.headers()["x-cc2api-request-id"]
                .to_str()
                .unwrap()
                .to_owned();
            let mut body = response.into_body();
            if end == "cancelled" {
                assert!(body.frame().await.unwrap().is_ok());
                drop(body);
            } else {
                assert_eq!(to_bytes(body, 65536).await.is_err(), end == "read_error");
            }
            store.flush().await.unwrap();
            let detail = store.detail(&id).await.unwrap();
            assert_eq!(detail.attempts.len(), 1);
            assert_eq!(detail.attempts[0].tokens.input_tokens, Some(10));
            assert!(detail.attempts[0].partial);
            assert_eq!(
                detail.request.outcome,
                match end {
                    "sse_error" => "upstream_error",
                    "read_error" => "body_error",
                    _ => "cancelled",
                }
            );
            if end == "read_error" {
                assert_eq!(detail.request.error_stage.as_deref(), Some("upstream_body"));
            }
            assert!(
                !serde_json::to_string(&detail)
                    .unwrap()
                    .contains("arbitrary-error-secret")
            );
        }
    }

    /// 请求准备阶段取消和后台异步写入故障不改变业务响应。
    #[tokio::test]
    async fn diagnostics_cover_early_cancellation_and_unavailable_storage() {
        let store = store().await;
        let trace = TraceContext::new(store.clone());
        let id = trace.lock().request.request_id.clone();
        drop(RequestTraceGuard::new(trace));
        store.flush().await.unwrap();
        assert_eq!(
            store.detail(&id).await.unwrap().request.outcome,
            "cancelled"
        );
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        DiagnosticsStore::migrate(&pool).await.unwrap();
        let unavailable = Arc::new(DiagnosticsStore::new(pool.clone()));
        let trace = TraceContext::new(unavailable.clone());
        let response =
            RequestTraceGuard::new(trace).wrap(Response::new(Body::from("正常业务结果")));
        // 关闭连接池后，诊断写入失败也不能阻断已经构造的业务响应。
        pool.close().await;
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap(),
            "正常业务结果"
        );
        unavailable.flush().await.unwrap();
        assert!(unavailable.list(DiagnosticQuery::default()).await.is_err());
    }

    /// 既有缓冲分支复用相同解码与事件解析，传输未完成时仍保守终结。
    /// @return 无；重复终结、丢用量或根状态错误时失败。
    #[tokio::test]
    async fn diagnostics_buffered_compression_uses_the_same_event_observer() {
        let source = b"data: {\"type\":\"message_start\",\rdata: \"message\":{\"model\":\"claude-sonnet-5\",\"usage\":{\"input_tokens\":123,\"output_tokens\":0}}}\r\rdata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":8}}\r\rdata: {\"type\":\"message_stop\"}\r\r";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(source).unwrap();
        let wire = encoder.finish().unwrap();
        let store = store().await;
        for complete in [true, false] {
            let trace = TraceContext::new(store.clone());
            let guard = RequestTraceGuard::new(trace.clone());
            let response = CURRENT_TRACE
                .scope(trace, async {
                    let mut attempt =
                        begin_attempt(&account(), br#"{"model":"claude-sonnet-5"}"#).unwrap();
                    let response = Response::builder()
                        .header("content-encoding", "gzip")
                        .body(Body::from(wire.clone()))
                        .unwrap();
                    attempt.headers(200, response.headers(), vec!["gzip".into()]);
                    attempt.buffered(&wire, complete);
                    drop(attempt);
                    guard.wrap(response)
                })
                .await;
            let id = response.headers()["x-cc2api-request-id"]
                .to_str()
                .unwrap()
                .to_owned();
            assert_eq!(to_bytes(response.into_body(), 4096).await.unwrap(), wire);
            store.flush().await.unwrap();
            let detail = store.detail(&id).await.unwrap();
            assert_eq!(detail.attempts.len(), 1);
            let attempt = &detail.attempts[0];
            assert_eq!(attempt.tokens.input_tokens, Some(123));
            assert_eq!(attempt.tokens.output_tokens, Some(8));
            assert_eq!(attempt.partial, !complete);
            assert_eq!(
                attempt.outcome,
                if complete { "success" } else { "body_error" }
            );
            assert_eq!(detail.request.outcome, attempt.outcome);
        }
        assert_eq!(
            store.list(DiagnosticQuery::default()).await.unwrap().total,
            2
        );
    }

    /// 损坏压缩、采样超限和非法 JSON 应报告观察缺口，同时保留原始响应。
    #[tokio::test]
    async fn diagnostics_report_observation_failures_without_changing_bytes() {
        let store = store().await;
        for reason in ["decode_error", "sampling_limit", "invalid_payload"] {
            let raw = match reason {
                "decode_error" => b"bad-gzip".to_vec(),
                "sampling_limit" => vec![b'{'; USAGE_BUFFER_LIMIT + 1],
                _ => b"{bad-json".to_vec(),
            };
            let trace = TraceContext::new(store.clone());
            let guard = RequestTraceGuard::new(trace.clone());
            let response = CURRENT_TRACE
                .scope(trace, async {
                    let mut attempt =
                        begin_attempt(&account(), br#"{"model":"claude-sonnet-5"}"#).unwrap();
                    let response = Response::new(Body::from(raw.clone()));
                    attempt.headers(
                        200,
                        response.headers(),
                        if reason == "decode_error" {
                            vec!["gzip".into()]
                        } else {
                            vec![]
                        },
                    );
                    guard.wrap(attempt.wrap(response))
                })
                .await;
            let id = response.headers()["x-cc2api-request-id"]
                .to_str()
                .unwrap()
                .to_owned();
            assert_eq!(
                to_bytes(response.into_body(), USAGE_BUFFER_LIMIT + 2)
                    .await
                    .unwrap(),
                raw
            );
            store.flush().await.unwrap();
            let detail = store.detail(&id).await.unwrap();
            assert_eq!(detail.attempts.len(), 1);
            assert_eq!(detail.attempts[0].error_type.as_deref(), Some(reason));
            assert!(detail.attempts[0].partial);
            assert_eq!(detail.request.outcome, "incomplete");
        }
        assert_eq!(
            store
                .list(DiagnosticQuery::default())
                .await
                .unwrap()
                .health
                .observation_failures,
            3
        );
    }
}
