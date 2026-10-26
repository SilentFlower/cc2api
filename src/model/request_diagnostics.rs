use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// 一次已鉴权 messages 请求的诊断摘要；不包含请求或响应正文。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestDiagnostic {
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
    pub ttft_ms: Option<u64>,
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
