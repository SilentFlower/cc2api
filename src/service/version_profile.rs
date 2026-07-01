use serde_json::Value;

use crate::error::AppError;

/// 默认 Claude Code 版本画像 key。
pub const DEFAULT_CLAUDE_CODE_VERSION_PROFILE: &str = "2.1.197";
/// Claude Code 默认兼容版本。
pub const DEFAULT_CLAUDE_CODE_VERSION: &str = PROFILE_2_1_197.identity.version;
/// Claude Code 默认基础版本。
pub const DEFAULT_CLAUDE_CODE_VERSION_BASE: &str = PROFILE_2_1_197.identity.version_base;
/// 当前默认 Claude Code 抓包对应的构建时间。
pub const DEFAULT_CLAUDE_CODE_BUILD_TIME: &str = PROFILE_2_1_197.identity.build_time;
/// 默认画像对应的 Claude Code / Claude CLI 允许版本范围。
pub const DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS: &str =
    PROFILE_2_1_197.access_policy.allowed_claude_code_versions;
/// 当前默认 Claude Code 使用的 Stainless SDK 版本。
pub const STAINLESS_PACKAGE_VERSION: &str = "0.94.0";
/// 当前默认 Claude Code 抓包中的 Node runtime 版本。
pub const STAINLESS_RUNTIME_VERSION: &str = "v26.3.0";
/// 旧版回滚画像使用的 Node runtime 版本。
const STAINLESS_RUNTIME_VERSION_2_1_187: &str = "v24.3.0";
/// Claude Code 2.1.195 抓包中的通用 message beta token 集合。
///
/// `context-1m-2025-08-07` 仍由账号白名单单独控制,不能放进必需集合。
pub const MESSAGE_BETA_TOKENS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.195 Haiku 非流式标题/探测请求使用的窄 beta token 集合。
pub const HAIKU_PROBE_BETA_TOKENS: &str = "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05";
/// Claude Code 2.1.195 Haiku 流式标题请求使用的窄 beta token 集合。
pub const HAIKU_STREAMING_TITLE_BETA_TOKENS: &str = "oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,advisor-tool-2026-03-01,structured-outputs-2025-12-15,cache-diagnosis-2026-04-07";
/// Claude Code 2.1.195 Fable 主请求额外启用的 fallback beta token 集合。
pub const FABLE_FALLBACK_BETA_TOKENS: &str =
    "server-side-fallback-2026-06-01,fallback-credit-2026-06-01";
/// Claude Code 2.1.195 Fable 主请求使用的完整 message beta token 集合。
pub const FABLE_MESSAGE_BETA_TOKENS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,server-side-fallback-2026-06-01,fallback-credit-2026-06-01,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
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
    pub fable_message_beta_tokens: &'static str,
    pub count_tokens_beta_tokens: &'static str,
    pub oauth_beta_token: &'static str,
    pub code_triggers_beta_token: &'static str,
    pub mcp_servers_beta_token: &'static str,
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
    ClaudeCode2172Plus,
}

/// telemetry 和 GrowthBook 子画像。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryProfile {
    pub shape: TelemetryShape,
    pub growthbook_user_agent: &'static str,
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
}

const PROFILE_2_1_185: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.185",
    identity: IdentityProfile {
        version: "2.1.185",
        version_base: "2.1.185",
        build_time: "2026-06-20T06:38:30Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION_2_1_187,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.185",
    },
    request: RequestProfile {
        message_beta_tokens: MESSAGE_BETA_TOKENS,
        fable_message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
        count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
        oauth_beta_token: OAUTH_BETA_TOKEN,
        code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
        mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
    },
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172Plus,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
    },
};

const PROFILE_2_1_195: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.195",
    identity: IdentityProfile {
        version: "2.1.195",
        version_base: "2.1.195",
        build_time: "2026-06-26T01:00:56Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.195",
    },
    request: RequestProfile {
        message_beta_tokens: MESSAGE_BETA_TOKENS,
        fable_message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
        count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
        oauth_beta_token: OAUTH_BETA_TOKEN,
        code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
        mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
    },
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172Plus,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
    },
};

const PROFILE_2_1_187: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.187",
    identity: IdentityProfile {
        version: "2.1.187",
        version_base: "2.1.187",
        build_time: "2026-06-23T16:59:46Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION_2_1_187,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.187",
    },
    request: RequestProfile {
        message_beta_tokens: MESSAGE_BETA_TOKENS,
        fable_message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
        count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
        oauth_beta_token: OAUTH_BETA_TOKEN,
        code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
        mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
    },
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172Plus,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
    },
};

const PROFILE_2_1_173: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.173",
    identity: IdentityProfile {
        version: "2.1.173",
        version_base: "2.1.173",
        build_time: "2026-06-11T01:23:13Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION_2_1_187,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.173",
    },
    request: RequestProfile {
        message_beta_tokens: MESSAGE_BETA_TOKENS,
        fable_message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
        count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
        oauth_beta_token: OAUTH_BETA_TOKEN,
        code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
        mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
    },
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172Plus,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2173,
        growthbook_user_agent: "Bun/1.3.14",
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
    },
};

const PROFILE_2_1_197: ClaudeCodeProfile = ClaudeCodeProfile {
    key: "2.1.197",
    identity: IdentityProfile {
        version: "2.1.197",
        version_base: "2.1.197",
        build_time: "2026-06-29T19:08:42Z",
        stainless_package_version: STAINLESS_PACKAGE_VERSION,
        stainless_runtime_version: STAINLESS_RUNTIME_VERSION,
    },
    access_policy: AccessPolicyProfile {
        allowed_claude_code_versions: "2.1.89-2.1.197",
    },
    request: RequestProfile {
        message_beta_tokens: MESSAGE_BETA_TOKENS,
        fable_message_beta_tokens: FABLE_MESSAGE_BETA_TOKENS,
        count_tokens_beta_tokens: COUNT_TOKENS_BETA_TOKENS,
        oauth_beta_token: OAUTH_BETA_TOKEN,
        code_triggers_beta_token: CODE_TRIGGERS_BETA_TOKEN,
        mcp_servers_beta_token: MCP_SERVERS_BETA_TOKEN,
    },
    billing: BillingProfile {
        cc_version_algorithm: CcVersionAlgorithm::Sha256TextPositions,
        cch_profile: CchProfile::ClaudeCode2172Plus,
    },
    telemetry: TelemetryProfile {
        shape: TelemetryShape::ClaudeCode2185,
        growthbook_user_agent: "Bun/1.4.0",
    },
    endpoints: EndpointProfile {
        event_logging_path: EVENT_LOGGING_V2_PATH,
        event_logging_legacy_path: EVENT_LOGGING_LEGACY_PATH,
    },
};

static CLAUDE_CODE_PROFILES: [&ClaudeCodeProfile; 5] = [
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
    &PROFILE_2_1_197
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
        assert_eq!(growthbook_user_agent(), "Bun/1.4.0");
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
            assert!(!profile.request.fable_message_beta_tokens.is_empty());
            assert!(!profile.request.count_tokens_beta_tokens.is_empty());
            assert!(!profile.request.oauth_beta_token.is_empty());
            assert!(!profile.telemetry.growthbook_user_agent.is_empty());
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
        let current = profile_for_key("2.1.197").unwrap();
        assert_eq!(current.telemetry.shape, TelemetryShape::ClaudeCode2185);
        assert_eq!(current.telemetry.growthbook_user_agent, "Bun/1.4.0");
        assert_eq!(
            current.access_policy.allowed_claude_code_versions,
            "2.1.89-2.1.197"
        );
        assert_eq!(
            current.identity.stainless_runtime_version,
            STAINLESS_RUNTIME_VERSION
        );

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
