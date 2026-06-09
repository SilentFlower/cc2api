/// Claude Code 默认兼容版本。
pub const DEFAULT_CLAUDE_CODE_VERSION: &str = "2.1.169";
/// Claude Code 默认基础版本。
pub const DEFAULT_CLAUDE_CODE_VERSION_BASE: &str = "2.1.169";
/// Claude Code 2.1.169 抓包对应的构建时间。
pub const DEFAULT_CLAUDE_CODE_BUILD_TIME: &str = "2026-06-08T03:22:12Z";
/// Claude Code 2.1.169 使用的 Stainless SDK 版本。
pub const STAINLESS_PACKAGE_VERSION: &str = "0.94.0";
/// Claude Code 2.1.169 抓包中的 Node runtime 版本。
pub const STAINLESS_RUNTIME_VERSION: &str = "v24.3.0";
/// Claude Code 2.1.169 抓包中的 message beta token 集合。
///
/// `context-1m-2025-08-07` 仍由账号白名单单独控制,不能放进必需集合。
pub const MESSAGE_BETA_TOKENS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07";
/// Claude Code OAuth 相关端点使用的 beta token。
pub const OAUTH_BETA_TOKEN: &str = "oauth-2025-04-20";
/// Claude Code triggers 端点使用的 beta token。
pub const CODE_TRIGGERS_BETA_TOKEN: &str = "ccr-triggers-2026-01-30";
/// Claude Code MCP servers 端点使用的 beta token。
pub const MCP_SERVERS_BETA_TOKEN: &str = "mcp-servers-2025-12-04";
/// Claude Code 2.1.169 MCP servers 请求声明的客户端能力。
pub const MCP_CLIENT_CAPABILITIES: &str = "eyJyb290cyI6e30sImVsaWNpdGF0aW9uIjp7fX0=";
/// Claude Code 2.1.169 MCP servers 请求声明的协议版本。
pub const MCP_PROTOCOL_VERSION: &str = "2025-11-25";
/// Claude Code 2.1.169 的 event logging v2 路径。
pub const EVENT_LOGGING_V2_PATH: &str = "/api/event_logging/v2/batch";
/// 旧版 event logging 路径，保留用于客户端请求兼容。
pub const EVENT_LOGGING_LEGACY_PATH: &str = "/api/event_logging/batch";

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
    "Bun/1.3.14"
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
