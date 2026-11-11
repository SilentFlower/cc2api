use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// 请求入口冻结的内容记录级别；历史私有作业默认只保存摘要。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestLogMode {
    #[default]
    Info,
    Debug,
}

impl RequestLogMode {
    /// 返回管理设置使用的稳定字符串。
    /// @return info 或 debug。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }

    /// 解析管理端字符串，未知值由调用边界明确处理。
    /// @param value 设置中的原始字符串。
    /// @return 已知模式或 None。
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            _ => None,
        }
    }
}

/// 一次已鉴权 messages 请求的诊断摘要；不包含请求或响应正文。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestDiagnostic {
    /// 本次实际模式；旧记录保持未知。
    #[serde(default)]
    pub log_mode: Option<RequestLogMode>,
    pub request_id: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub status_code: Option<u16>,
    pub account_id: Option<i64>,
    pub response_request_id: Option<String>,
    pub outcome: String,
    pub error_stage: Option<String>,
    pub attempt_count: u32,
}

/// 实际提交给上游的白名单参数，未知字段保留为空。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticParameters {
    pub model: Option<String>,
    pub max_tokens: Option<u64>,
    pub thinking_type: Option<String>,
    pub thinking_budget: Option<u64>,
    pub effort: Option<String>,
    pub stream: Option<bool>,
    pub tools_count: Option<usize>,
}

/// 受信任 sender 从原生组装快照提取的白名单观察；不包含业务正文或凭据。
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRequestObservation {
    pub schema_version: u8,
    pub parameters: DiagnosticParameters,
    pub session_id: Option<String>,
    pub previous_message_id: Option<String>,
    pub body_sha256: String,
    #[serde(default)]
    pub capture_status: Option<String>,
    #[serde(default)]
    pub runtime: Option<NativeRuntimeObservation>,
}

/// 原生请求实际使用的进程与本地阶段计时，所有耗时来自单调时钟。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRuntimeObservation {
    pub worker_id: String,
    pub worker_pid: u32,
    pub kernel_generation: Option<String>,
    pub warm_hit: bool,
    pub preparation_wait_ms: u64,
    pub checkout_ms: u64,
    pub worker_start_ms: Option<u64>,
    pub prepared_ms: u64,
}

/// 仅在网关与 sender 之间传递的原生观察头，消费后移除。
pub(crate) const NATIVE_OBSERVATION_HEADER: &str = "x-v3-native-observation";

impl DiagnosticParameters {
    /// 从请求中提取可用于诊断和延迟分组的既有白名单参数。
    /// @param body 已解析的请求，不保留正文或任意扩展字段。
    /// @return 缺失或非法字段保持未知的参数摘要。
    pub fn from_request(body: &Value) -> Self {
        Self {
            model: body
                .get("model")
                .and_then(Value::as_str)
                .and_then(safe_model),
            max_tokens: body.get("max_tokens").and_then(Value::as_u64),
            thinking_type: body
                .pointer("/thinking/type")
                .and_then(Value::as_str)
                .filter(|value| ["enabled", "disabled", "adaptive"].contains(value))
                .map(str::to_owned),
            thinking_budget: body
                .pointer("/thinking/budget_tokens")
                .and_then(Value::as_u64),
            effort: body
                .pointer("/output_config/effort")
                .and_then(Value::as_str)
                .filter(|value| {
                    ["low", "medium", "high", "xhigh", "max", "default", "none"].contains(value)
                })
                .map(str::to_owned),
            stream: body.get("stream").and_then(Value::as_bool),
            tools_count: body.get("tools").and_then(Value::as_array).map(Vec::len),
        }
    }
}

/// 只保留安全模型标识，不能把请求正文或凭据片段带入诊断和统计键。
/// @param value 外部模型值。
/// @return 符合既有白名单时的模型名。
pub(crate) fn safe_model(value: &str) -> Option<String> {
    (value.len() <= 128
        && value.starts_with("claude-")
        && !value.contains("sk-ant-")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)))
    .then(|| value.to_owned())
}

/// 同一账号内可关联的会话和线程摘要，所有身份均为散列。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticSessions {
    pub real_session_hash: Option<String>,
    pub upstream_session_hash: Option<String>,
    pub mapping_action: Option<String>,
    pub previous_message_hash: Option<String>,
}

/// 响应实际出现的各类 token；未观察到的值不伪装为零。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiagnosticTokens {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
    pub cache_write_5m_tokens: Option<u64>,
    pub cache_write_1h_tokens: Option<u64>,
}

impl From<[Option<u64>; 6]> for DiagnosticTokens {
    /// 将共享观察器的六类累计值转换为具名查询字段。
    /// @param values 输入、输出、缓存读、缓存写及两种缓存写 TTL 明细。
    /// @return 保留未知值的 token 摘要。
    fn from(values: [Option<u64>; 6]) -> Self {
        let [
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            cache_write_5m_tokens,
            cache_write_1h_tokens,
        ] = values;
        Self {
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            cache_write_5m_tokens,
            cache_write_1h_tokens,
        }
    }
}

/// 一次真实发送尝试，包括重试和未完整返回的流。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptDiagnostic {
    /// 历史缺失表示未观测，不能使用当前运行时状态补写。
    #[serde(default)]
    pub runtime: Option<NativeRuntimeObservation>,
    /// 与根请求同次冻结的模式；历史缺失保持未知。
    #[serde(default)]
    pub log_mode: Option<RequestLogMode>,
    /// 私有正文采集状态；旧记录为空。
    #[serde(default)]
    pub capture_status: Option<String>,
    pub attempt_id: String,
    pub request_id: String,
    pub ordinal: u32,
    pub account_id: i64,
    pub profile_version: String,
    pub profile_selection: String,
    pub parameters: DiagnosticParameters,
    pub sessions: DiagnosticSessions,
    pub started_at: String,
    pub finished_at: Option<String>,
    /// 历史字段保留响应头耗时语义，不作为首输出延迟回填。
    pub ttft_ms: Option<u64>,
    /// 首次有效内容被观察到的耗时；旧记录和未观察到内容时为空。
    #[serde(default)]
    pub first_output_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub status_code: Option<u16>,
    pub upstream_request_id: Option<String>,
    pub response_model: Option<String>,
    pub response_message_hash: Option<String>,
    pub stop_reason: Option<String>,
    pub tokens: DiagnosticTokens,
    pub partial: bool,
    pub outcome: String,
    pub error_stage: Option<String>,
    pub error_type: Option<String>,
    pub retry_reason: Option<String>,
}

/// 管理员列表查询；原始时间在入口规范化为 UTC 毫秒。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DiagnosticQuery {
    pub page: Option<u32>,
    pub page_size: Option<u32>,
    pub account_id: Option<i64>,
    pub correlation_id: Option<String>,
    pub outcome: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
}

/// 单个请求及按提交顺序排列的尝试。
#[derive(Debug, Serialize)]
pub struct DiagnosticDetail {
    pub request: RequestDiagnostic,
    pub attempts: Vec<AttemptDiagnostic>,
}

/// 进程级采集失败计数，避免把缺失日志误认为没有请求。
#[derive(Debug, Default, Serialize)]
pub struct DiagnosticHealth {
    pub dropped_records: u64,
    pub write_failures: u64,
    pub observation_failures: u64,
}

/// 管理员分页结果与采集健康状态。
#[derive(Debug, Serialize)]
pub struct DiagnosticPage {
    pub items: Vec<RequestDiagnostic>,
    pub total: i64,
    pub page: u32,
    pub page_size: u32,
    pub health: DiagnosticHealth,
}

/// 规范化响应关联 ID，非标准头值只保存散列。
/// @param value 外部响应标识或管理员筛选条件。
/// @return 可持久化、可反查的受控标识。
pub(crate) fn normalize_correlation_id(value: &str) -> String {
    if let Ok(uuid) = Uuid::parse_str(value) {
        return uuid.to_string();
    }
    if value.len() <= 128
        && value
            .strip_prefix("req_")
            .is_some_and(|rest| !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_alphanumeric()))
    {
        return value.to_owned();
    }
    if value
        .strip_prefix("sha256:")
        .is_some_and(|rest| rest.len() == 64 && rest.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        return value.to_ascii_lowercase();
    }
    format!("sha256:{}", hex::encode(Sha256::digest(value.as_bytes())))
}
