use serde_json::Value;

use crate::error::AppError;

/// 默认 Claude Code 版本画像 key。
pub const DEFAULT_CLAUDE_CODE_VERSION_PROFILE: &str = "2.1.257";
/// Claude Code 默认兼容版本。
pub const DEFAULT_CLAUDE_CODE_VERSION: &str = PROFILE_2_1_257.identity.version;
/// Claude Code 默认基础版本。
pub const DEFAULT_CLAUDE_CODE_VERSION_BASE: &str = PROFILE_2_1_257.identity.version_base;
/// 当前默认 Claude Code 抓包对应的构建时间。
pub const DEFAULT_CLAUDE_CODE_BUILD_TIME: &str = PROFILE_2_1_257.identity.build_time;
/// 默认画像对应的 Claude Code / Claude CLI 允许版本范围。
pub const DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS: &str =
    PROFILE_2_1_257.access_policy.allowed_claude_code_versions;
/// 当前默认 Claude Code 使用的 Stainless SDK 版本。
pub const STAINLESS_PACKAGE_VERSION: &str = "0.112.1";
/// Claude Code 2.1.220 及旧回滚画像使用的 Stainless SDK 版本。
const STAINLESS_PACKAGE_VERSION_2_1_220: &str = "0.94.0";
/// 当前默认 Claude Code 抓包中的 Node runtime 版本。
pub const STAINLESS_RUNTIME_VERSION: &str = "v26.3.0";
/// 旧版回滚画像使用的 Node runtime 版本。
const STAINLESS_RUNTIME_VERSION_2_1_187: &str = "v24.3.0";
/// Claude Code 2.1.197 抓包中的通用 message beta token 集合。
///
/// `context-1m-2025-08-07` 仍由账号白名单单独控制,不能放进必需集合。
const MESSAGE_BETA_TOKENS_2_1_197: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.220 抓包中的通用 message beta token 集合。
///
/// `fallback-credit-2026-06-01` 位于 effort 与 extended-cache-ttl 之间。
pub const MESSAGE_BETA_TOKENS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,fallback-credit-2026-06-01,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.195 Haiku 非流式标题/探测请求使用的窄 beta token 集合。
pub const HAIKU_PROBE_BETA_TOKENS: &str = "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05";
/// Claude Code 2.1.195 Haiku 流式标题请求使用的窄 beta token 集合。
pub const HAIKU_STREAMING_TITLE_BETA_TOKENS: &str = "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,advisor-tool-2026-03-01,structured-outputs-2025-12-15,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.195 Fable 主请求额外启用的 fallback beta token 集合。
pub const FABLE_FALLBACK_BETA_TOKENS: &str =
    "server-side-fallback-2026-06-01,fallback-credit-2026-06-01";
/// Claude Code 2.1.195 Fable 主请求使用的完整 message beta token 集合。
pub const FABLE_MESSAGE_BETA_TOKENS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,server-side-fallback-2026-06-01,fallback-credit-2026-06-01,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.257 Fable 5.1 主请求使用的完整 message beta token 集合。
pub const FABLE_5_1_MESSAGE_BETA_TOKENS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,server-side-fallback-2026-07-01,fallback-credit-2026-06-01,thinking-display-updates-2026-08-18,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.257 Haiku 主请求使用的 beta token 集合。
pub const HAIKU_MAIN_BETA_TOKENS_2_1_257: &str = "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,claude-code-20250219,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.257 无 diagnostics 的 Haiku 主请求 beta token 集合。
pub const HAIKU_MAIN_NO_DIAGNOSTICS_BETA_TOKENS_2_1_257: &str = "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.257 Haiku 非流式 1024 token 辅助请求 beta token 集合。
pub const HAIKU_NON_STREAM_AUX_BETA_TOKENS_2_1_257: &str = "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,context-management-2025-06-27,prompt-caching-scope-2026-01-05,extended-cache-ttl-2025-04-11";
/// Claude Code OAuth 相关端点使用的 beta token。
pub const OAUTH_BETA_TOKEN: &str = "oauth-2025-04-20";
/// Claude 原生 count_tokens 端点需要的 beta token。
pub const COUNT_TOKENS_BETA_TOKEN: &str = "token-counting-2024-11-01";
/// Claude 原生 count_tokens 端点缺省使用的 beta token 集合。
pub const COUNT_TOKENS_BETA_TOKENS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,context-management-2025-06-27,token-counting-2024-11-01";
/// Claude Code triggers 端点使用的 beta token。
pub const CODE_TRIGGERS_BETA_TOKEN: &str = "ccr-triggers-2026-01-30";
/// Claude Code MCP servers 端点使用的 beta token。
pub const MCP_SERVERS_BETA_TOKEN: &str = "mcp-servers-2025-12-04";
/// Claude Code 2.1.195 MCP servers 请求声明的客户端能力。
pub const MCP_CLIENT_CAPABILITIES: &str = "eyJyb290cyI6e30sImVsaWNpdGF0aW9uIjp7fX0=";
/// Claude Code 2.1.195 MCP servers 请求声明的协议版本。
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
/// Claude Code 2.1.195 的 event logging v2 路径。
pub const EVENT_LOGGING_V2_PATH: &str = "/api/event_logging/v2/batch";
/// 旧版 event logging 路径，保留用于客户端请求兼容。
pub const EVENT_LOGGING_LEGACY_PATH: &str = "/api/event_logging/batch";

/// Claude Code 版本画像，集中声明一个版本的所有协议子画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaudeCodeProfile {
    pub key: &'static str,
    pub identity: IdentityProfile,
    pub access_policy: AccessPolicyProfile,
    pub request: RequestProfile,
    pub billing: BillingProfile,
    pub telemetry: TelemetryProfile,
    pub endpoints: EndpointProfile,
}

/// 账号 canonical env 中需要与版本同步的身份字段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityProfile {
    pub version: &'static str,
    pub version_base: &'static str,
    pub build_time: &'static str,
    pub stainless_package_version: &'static str,
    pub stainless_runtime_version: &'static str,
}

/// 版本画像对应的默认访问策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccessPolicyProfile {
    pub allowed_claude_code_versions: &'static str,
}

/// 请求 beta token 子画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestProfile {
    pub message_beta_tokens: &'static str,
    pub fable_models: &'static [FableRequestProfile],
    pub haiku_probe_beta_tokens: &'static str,
    pub haiku_streaming_title_beta_tokens: &'static str,
    pub haiku_main_beta_tokens: &'static str,
    pub haiku_main_no_diagnostics_beta_tokens: &'static str,
    pub haiku_non_stream_aux_beta_tokens: &'static str,
    pub count_tokens_beta_tokens: &'static str,
    pub oauth_beta_token: &'static str,
    pub code_triggers_beta_token: &'static str,
    pub mcp_servers_beta_token: &'static str,
    pub opus_default_max_tokens_model: &'static str,
    pub message_body_order: MessageBodyOrderProfile,
}

impl RequestProfile {
    /// 按精确模型 ID 返回当前版本声明的 Fable 子画像。
    ///
    /// @param model_id 最终发送上游的模型 ID。
    /// @return 当前版本支持该 Fable 模型时返回子画像，否则返回 `None`。
    pub fn fable_model(&self, model_id: &str) -> Option<&FableRequestProfile> {
        self.fable_models
            .iter()
            .find(|profile| profile.model_id == model_id)
    }
}

/// 单个精确 Fable 模型的请求画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FableRequestProfile {
    pub model_id: &'static str,
    pub message_beta_tokens: &'static str,
    pub fallback: FableFallbackProfile,
    pub default_max_tokens: u64,
    pub thinking_display: Option<&'static str>,
}

/// Fable 顶层 `fallbacks` 字段的 JSON 形状。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FableFallbackProfile {
    Model(&'static str),
    Default,
}

/// `/v1/messages` 顶层字段顺序画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBodyOrderProfile {
    Legacy,
    ClaudeCode21220,
}

/// billing header 和 CCH 子画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BillingProfile {
    pub cc_version_algorithm: CcVersionAlgorithm,
    pub cch_profile: CchProfile,
}

/// `cc_version` 后缀算法枚举。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CcVersionAlgorithm {
    Sha256TextPositions,
}

/// CCH attestation 输入与 seed 画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CchProfile {
    ClaudeCode2172DropFallbacks,
    ClaudeCode21257ModelAware,
}

/// telemetry 和 GrowthBook 子画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryProfile {
    pub shape: TelemetryShape,
    pub growthbook_user_agent: &'static str,
    pub default_model: &'static str,
    pub base_beta_tokens: &'static str,
}

/// 自动 telemetry payload 结构版本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryShape {
    ClaudeCode2173,
    ClaudeCode2185,
}

impl TelemetryShape {
    /// 返回日志和前端展示使用的稳定 shape 名称。
    ///
    /// @return telemetry shape 的稳定字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCode2173 => "claude_code_2_1_173",
            Self::ClaudeCode2185 => "claude_code_2_1_185",
        }
    }
}

/// endpoint 子画像入口，后续新增 endpoint 差异时在此扩展。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointProfile {
    pub event_logging_path: &'static str,
    pub event_logging_legacy_path: &'static str,
    pub bootstrap_cedar_basin: Option<&'static str>,
    pub bootstrap_fable_cwk_cfg_key: Option<&'static str>,
    pub bootstrap_opus_cwk_cfg_key: Option<&'static str>,
}

const FABLE_MODELS_2_1_257: [FableRequestProfile; 2] = [
    FableRequestProfile {
        model_id: "claude-fable-5",
        message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
        fallback: FableFallbackProfile::Model("claude-opus-5"),
        default_max_tokens: 64_000,
        thinking_display: None,
    },
    FableRequestProfile {
        model_id: "claude-fable-5-1",
        message_beta_tokens: FABLE_5_1_MESSAGE_BETA_TOKENS,
        fallback: FableFallbackProfile::Default,
        default_max_tokens: 64_000,
        thinking_display: Some("updates"),
    },
];

const FABLE_MODELS_2_1_220: [FableRequestProfile; 1] = [FableRequestProfile {
    model_id: "claude-fable-5",
    message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
    fallback: FableFallbackProfile::Model("claude-opus-5"),
    default_max_tokens: 64_000,
    thinking_display: None,
}];

const FABLE_MODELS_ROLLBACK: [FableRequestProfile; 1] = [FableRequestProfile {
    model_id: "claude-fable-5",
    message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
    fallback: FableFallbackProfile::Model("claude-opus-4-8"),
    default_max_tokens: 64_000,
    thinking_display: None,
}];

const ROLLBACK_REQUEST_PROFILE: RequestProfile = RequestProfile {
    message_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    fable_models: &FABLE_MODELS_ROLLBACK,
    haiku_probe_beta_tokens: HAIKU_PROBE_BETA_TOKENS,
    haiku_streaming_title_beta_tokens: HAIKU_STREAMING_TITLE_BETA_TOKENS,
    haiku_main_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    haiku_main_no_diagnostics_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    haiku_non_stream_aux_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
    oauth_beta_token: OAUTH_BETA_TOKEN,
    code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
    mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
    opus_default_max_tokens_model: "claude-opus-4-8",
    message_body_order: MessageBodyOrderProfile::Legacy,
};

const PROFILE_2_1_257: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.257",
    identity: IdentityProfile {
        version: "2.1.257",
        version_base: "2.1.257",
        build_time: "2026-09-01T05:28:54Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.257",
    },
    request: RequestProfile {
        message_beta_tokens: MESSAGE_BETA_TOKENS,
        fable_models: &FABLE_MODELS_2_1_257,
        haiku_probe_beta_tokens: HAIKU_PROBE_BETA_TOKENS,
        haiku_streaming_title_beta_tokens: HAIKU_STREAMING_TITLE_BETA_TOKENS,
        haiku_main_beta_tokens: HAIKU_MAIN_BETA_TOKENS_2_1_257,
        haiku_main_no_diagnostics_beta_tokens: HAIKU_MAIN_NO_DIAGNOSTICS_BETA_TOKENS_2_1_257,
        haiku_non_stream_aux_beta_tokens: HAIKU_NON_STREAM_AUX_BETA_TOKENS_2_1_257,
        count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
        oauth_beta_token: OAUTH_BETA_TOKEN,
        code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
        mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
        opus_default_max_tokens_model: "claude-opus-5",
        message_body_order: MessageBodyOrderProfile::ClaudeCode21220,
    },
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode21257ModelAware,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.1",
        default_model: "claude-opus-5",
        base_beta_tokens: MESSAGE_BETA_TOKENS,
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
        bootstrap_cedar_basin: Some("2027-08-31"),
        bootstrap_fable_cwk_cfg_key: Some("sorrel"),
        bootstrap_opus_cwk_cfg_key: Some("belladonna"),
    },
};

const PROFILE_2_1_220: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.220",
    identity: IdentityProfile {
        version: "2.1.220",
        version_base: "2.1.220",
        build_time: "2026-07-24T22:17:45Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION_2_1_220,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.220",
    },
    request: RequestProfile {
        message_beta_tokens: MESSAGE_BETA_TOKENS,
        fable_models: &FABLE_MODELS_2_1_220,
        haiku_probe_beta_tokens: HAIKU_PROBE_BETA_TOKENS,
        haiku_streaming_title_beta_tokens: HAIKU_STREAMING_TITLE_BETA_TOKENS,
        haiku_main_beta_tokens: MESSAGE_BETA_TOKENS,
        haiku_main_no_diagnostics_beta_tokens: MESSAGE_BETA_TOKENS,
        haiku_non_stream_aux_beta_tokens: MESSAGE_BETA_TOKENS,
        count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
        oauth_beta_token: OAUTH_BETA_TOKEN,
        code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
        mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
        opus_default_max_tokens_model: "claude-opus-5",
        message_body_order: MessageBodyOrderProfile::ClaudeCode21220,
    },
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172DropFallbacks,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
        default_model: "claude-opus-5",
        base_beta_tokens: MESSAGE_BETA_TOKENS,
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
        bootstrap_cedar_basin: Some("2026-08-31"),
        bootstrap_fable_cwk_cfg_key: Some("marigold"),
        bootstrap_opus_cwk_cfg_key: Some("belladonna"),
    },
};

const PROFILE_2_1_185: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.185",
    identity: IdentityProfile {
        version: "2.1.185",
        version_base: "2.1.185",
        build_time: "2026-06-20T06:38:30Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION_2_1_220,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION_2_1_187,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.185",
    },
    request: ROLLBACK_REQUEST_PROFILE,
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172DropFallbacks,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
        default_model: "claude-sonnet-4-20250514",
        base_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
        bootstrap_cedar_basin: None,
        bootstrap_fable_cwk_cfg_key: Some("marigold"),
        bootstrap_opus_cwk_cfg_key: None,
    },
};

const PROFILE_2_1_195: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.195",
    identity: IdentityProfile {
        version: "2.1.195",
        version_base: "2.1.195",
        build_time: "2026-06-26T01:00:56Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION_2_1_220,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.195",
    },
    request: ROLLBACK_REQUEST_PROFILE,
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172DropFallbacks,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
        default_model: "claude-sonnet-4-20250514",
        base_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
        bootstrap_cedar_basin: None,
        bootstrap_fable_cwk_cfg_key: Some("marigold"),
        bootstrap_opus_cwk_cfg_key: None,
    },
};

const PROFILE_2_1_187: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.187",
    identity: IdentityProfile {
        version: "2.1.187",
        version_base: "2.1.187",
        build_time: "2026-06-23T16:59:46Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION_2_1_220,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION_2_1_187,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.187",
    },
    request: ROLLBACK_REQUEST_PROFILE,
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172DropFallbacks,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
        default_model: "claude-sonnet-4-20250514",
        base_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
        bootstrap_cedar_basin: None,
        bootstrap_fable_cwk_cfg_key: Some("marigold"),
        bootstrap_opus_cwk_cfg_key: None,
    },
};

const PROFILE_2_1_173: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.173",
    identity: IdentityProfile {
        version: "2.1.173",
        version_base: "2.1.173",
        build_time: "2026-06-11T01:23:13Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION_2_1_220,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION_2_1_187,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.173",
    },
    request: ROLLBACK_REQUEST_PROFILE,
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172DropFallbacks,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2173,
        growthbook_user_agent: "Bun/1.3.14",
        default_model: "claude-sonnet-4-20250514",
        base_beta_tokens: "",
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
        bootstrap_cedar_basin: None,
        bootstrap_fable_cwk_cfg_key: Some("marigold"),
        bootstrap_opus_cwk_cfg_key: None,
    },
};

const PROFILE_2_1_197: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.197",
    identity: IdentityProfile {
        version: "2.1.197",
        version_base: "2.1.197",
        build_time: "2026-06-29T19:08:42Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION_2_1_220,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.197",
    },
    request: ROLLBACK_REQUEST_PROFILE,
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172DropFallbacks,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
        default_model: "claude-sonnet-4-20250514",
        base_beta_tokens: MESSAGE_BETA_TOKENS_2_1_197,
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
        bootstrap_cedar_basin: None,
        bootstrap_fable_cwk_cfg_key: Some("marigold"),
        bootstrap_opus_cwk_cfg_key: None,
    },
};

static CLAUDE_CODE_PROFILES: [&ClaudeCodeProfile; 7] = [
    &PROFILE_2_1_257,
    &PROFILE_2_1_220,
    &PROFILE_2_1_197,
    &PROFILE_2_1_195,
    &PROFILE_2_1_187,
    &PROFILE_2_1_185,
    &PROFILE_2_1_173,
];

/// 返回默认 Claude Code 版本画像。
///
/// @return 默认版本画像。
pub fn default_profile() -> &'static ClaudeCodeProfile {
    &PROFILE_2_1_257
}

/// 返回所有内置 Claude Code 版本画像。
///
/// @return 只读的内置画像列表。
pub fn all_profiles() -> &'static [&'static ClaudeCodeProfile] {
    &CLAUDE_CODE_PROFILES
}

/// 按 settings key 查找内置版本画像。
///
/// @param key settings 中保存的版本画像 key。
/// @return 找到时返回画像，未知 key 返回业务错误。
pub fn profile_for_key(key: &str) -> Result<&'static ClaudeCodeProfile, AppError> {
    let key = key.trim();
    all_profiles()
        .iter()
        .copied()
        .find(|profile| profile.key == key)
        .ok_or_else(|| AppError::BadRequest(format!("未知 Claude Code 版本画像: {}", key)))
}

/// 校验 settings 中提交的版本画像 key。
///
/// @param key settings 中保存的版本画像 key。
/// @return key 已内置时返回 `Ok(())`。
pub fn validate_profile_key(key: &str) -> Result<(), AppError> {
    profile_for_key(key).map(|_| ())
}

/// 按账号 env.version 查找版本画像。
///
/// @param version 账号 canonical env 中的版本号。
/// @return 找不到内置画像时回退到默认画像，避免热路径拼出未验证组合。
pub fn profile_for_version(version: &str) -> &'static ClaudeCodeProfile {
    let version = normalize_version(version);
    all_profiles()
        .iter()
        .copied()
        .find(|profile| profile.identity.version == version)
        .unwrap_or_else(default_profile)
}

/// 按账号 env.version 精确查找版本画像，不对未知版本做默认回退。
///
/// @param version 账号 canonical env 中的版本号。
/// @return 命中内置画像时返回画像，否则返回 `None`。
pub fn exact_profile_for_version(version: &str) -> Option<&'static ClaudeCodeProfile> {
    let version = normalize_version(version);
    all_profiles()
        .iter()
        .copied()
        .find(|profile| profile.identity.version == version)
}

/// 将版本画像身份字段覆盖到 canonical_env JSON。
///
/// @param env 需要修改的 canonical_env JSON。
/// @param identity 目标版本身份画像。
pub fn apply_identity_to_env_json(env: &mut Value, identity: &IdentityProfile) {
    if !env.is_object() {
        *env = serde_json::json!({});
    }
    if let Some(map) = env.as_object_mut() {
        map.insert(
            "version".into(),
            Value::String(identity.version.to_string()),
        );
        map.insert(
            "version_base".into(),
            Value::String(identity.version_base.to_string()),
        );
        map.insert(
            "build_time".into(),
            Value::String(identity.build_time.to_string()),
        );
        map.insert(
            "node_version".into(),
            Value::String(identity.stainless_runtime_version.to_string()),
        );
    }
}

/// 返回 Claude CLI 请求使用的 User-Agent。
pub fn claude_cli_user_agent(version: &str) -> String {
    format!("claude-cli/{} (external, cli)", normalize_version(version))
}

/// 返回 Claude Code 服务请求使用的 User-Agent。
pub fn claude_code_user_agent(version: &str) -> String {
    format!("claude-code/{}", normalize_version(version))
}

/// 返回抓包中 GrowthBook remote eval 使用的 Bun User-Agent。
pub fn growthbook_user_agent() -> &'static str {
    default_profile().telemetry.growthbook_user_agent
}

/// 将空版本归一化为当前默认 Claude Code 版本。
pub fn normalize_version(version: &str) -> &str {
    if version.is_empty() {
        DEFAULT_CLAUDE_CODE_VERSION
    } else {
        version
    }
}

/// 判断路径是否为 event logging v2 或旧版 batch 端点。
pub fn is_event_logging_path(path: &str) -> bool {
    path.contains(EVENT_LOGGING_V2_PATH) || path.contains(EVENT_LOGGING_LEGACY_PATH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn default_profile_matches_compat_constants() {
        let profile = default_profile();
        assert_eq!(profile.key, DEFAULT_CLAUDE_CODE_VERSION_PROFILE);
        assert_eq!(profile.identity.version, DEFAULT_CLAUDE_CODE_VERSION);
        assert_eq!(
            profile.identity.version_base,
            DEFAULT_CLAUDE_CODE_VERSION_BASE
        );
        assert_eq!(profile.identity.build_time, DEFAULT_CLAUDE_CODE_BUILD_TIME);
        assert_eq!(
            profile.access_policy.allowed_claude_code_versions,
            DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS
        );
        assert_eq!(growthbook_user_agent(), "Bun/1.4.1");
    }

    #[test]
    fn profiles_are_complete_and_unique() {
        let mut keys = HashSet::new();
        let mut versions = HashSet::new();
        for profile in all_profiles() {
            assert!(keys.insert(profile.key));
            assert!(versions.insert(profile.identity.version));
            assert!(!profile.identity.version_base.is_empty());
            assert!(!profile.identity.build_time.is_empty());
            assert!(!profile.identity.stainless_package_version.is_empty());
            assert!(!profile.identity.stainless_runtime_version.is_empty());
            assert!(
                !profile
                    .access_policy
                    .allowed_claude_code_versions
                    .is_empty()
            );
            assert!(!profile.request.message_beta_tokens.is_empty());
            assert!(!profile.request.fable_models.is_empty());
            assert!(
                profile
                    .request
                    .fable_models
                    .iter()
                    .all(|fable| !fable.message_beta_tokens.is_empty())
            );
            assert!(!profile.request.haiku_probe_beta_tokens.is_empty());
            assert!(!profile.request.haiku_streaming_title_beta_tokens.is_empty());
            assert!(!profile.request.haiku_main_beta_tokens.is_empty());
            assert!(
                !profile
                    .request
                    .haiku_main_no_diagnostics_beta_tokens
                    .is_empty()
            );
            assert!(!profile.request.haiku_non_stream_aux_beta_tokens.is_empty());
            assert!(!profile.request.count_tokens_beta_tokens.is_empty());
            assert!(!profile.request.oauth_beta_token.is_empty());
            assert!(!profile.request.opus_default_max_tokens_model.is_empty());
            assert!(!profile.telemetry.growthbook_user_agent.is_empty());
            assert!(!profile.telemetry.default_model.is_empty());
            assert!(!profile.endpoints.event_logging_path.is_empty());
        }
    }

    #[test]
    fn profile_lookup_rejects_unknown_key_and_falls_back_for_unknown_version() {
        assert_eq!(
            profile_for_key("2.1.173").unwrap().identity.version,
            "2.1.173"
        );
        assert!(profile_for_key("2.1.999").is_err());
        assert_eq!(
            profile_for_version("2.1.999").identity.version,
            DEFAULT_CLAUDE_CODE_VERSION
        );
    }

    #[test]
    fn profile_declares_known_telemetry_differences() {
        let current = profile_for_key("2.1.257").unwrap();
        assert_eq!(current.telemetry.shape, TelemetryShape::ClaudeCode2185);
        assert_eq!(current.telemetry.growthbook_user_agent, "Bun/1.4.1");
        assert_eq!(
            current.access_policy.allowed_claude_code_versions,
            "2.1.89-2.1.257"
        );
        assert_eq!(
            current.identity.stainless_runtime_version,
            STAINLESS_RUNTIME_VERSION
        );
        assert_eq!(current.identity.stainless_package_version, "0.112.1");
        assert_eq!(
            current
                .request
                .fable_model("claude-fable-5-1")
                .unwrap()
                .fallback,
            FableFallbackProfile::Default
        );
        assert_eq!(current.telemetry.default_model, "claude-opus-5");

        let previous = profile_for_key("2.1.220").unwrap();
        assert_eq!(previous.telemetry.growthbook_user_agent, "Bun/1.4.0");
        assert_eq!(previous.identity.stainless_package_version, "0.94.0");
        assert!(previous.request.fable_model("claude-fable-5-1").is_none());

        let rollback = profile_for_key("2.1.197").unwrap();
        assert_eq!(
            rollback.request.message_beta_tokens,
            MESSAGE_BETA_TOKENS_2_1_197
        );
        assert_eq!(
            rollback
                .request
                .fable_model("claude-fable-5")
                .unwrap()
                .fallback,
            FableFallbackProfile::Model("claude-opus-4-8")
        );
        assert_eq!(rollback.telemetry.default_model, "claude-sonnet-4-20250514");

        let latest_rollback = profile_for_key("2.1.195").unwrap();
        assert_eq!(
            latest_rollback.access_policy.allowed_claude_code_versions,
            "2.1.89-2.1.195"
        );
        assert_eq!(latest_rollback.identity.build_time, "2026-06-26T01:00:56Z");

        let rollback = profile_for_key("2.1.187").unwrap();
        assert_eq!(rollback.telemetry.shape, TelemetryShape::ClaudeCode2185);
        assert_eq!(rollback.telemetry.growthbook_user_agent, "Bun/1.4.0");
        assert_eq!(
            rollback.access_policy.allowed_claude_code_versions,
            "2.1.89-2.1.187"
        );

        let previous = profile_for_key("2.1.185").unwrap();
        assert_eq!(previous.telemetry.shape, TelemetryShape::ClaudeCode2185);
        assert_eq!(previous.telemetry.growthbook_user_agent, "Bun/1.4.0");
        assert_eq!(
            previous.access_policy.allowed_claude_code_versions,
            "2.1.89-2.1.185"
        );

        let old = profile_for_key("2.1.173").unwrap();
        assert_eq!(old.telemetry.shape, TelemetryShape::ClaudeCode2173);
        assert_eq!(old.telemetry.growthbook_user_agent, "Bun/1.3.14");
        assert_eq!(
            old.access_policy.allowed_claude_code_versions,
            "2.1.89-2.1.173"
        );
    }

    #[test]
    fn apply_identity_to_env_json_preserves_other_fields() {
        let profile = profile_for_key("2.1.173").unwrap();
        let mut env = serde_json::json!({
            "version": "2.1.185",
            "platform": "linux",
        });
        apply_identity_to_env_json(&mut env, &profile.identity);
        assert_eq!(env["version"], "2.1.173");
        assert_eq!(env["version_base"], "2.1.173");
        assert_eq!(env["build_time"], "2026-06-11T01:23:13Z");
        assert_eq!(
            env["node_version"],
            profile.identity.stainless_runtime_version
        );
        assert_eq!(env["platform"], "linux");
    }
}
