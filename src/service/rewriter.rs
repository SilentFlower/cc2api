use base64::Engine;
use once_cell::sync::Lazy;
use rand::Rng;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tracing::info;

use crate::error::AppError;
use crate::model::account::{Account, BillingMode, CanonicalEnvData, CanonicalPromptEnvData};
use crate::model::identity::{
    DeviceProfile, device_profile, process_snapshot, process_snapshot_json, request_profile,
};
use crate::service::version_profile::{
    CODE_TRIGGERS_BETA_TOKEN, COUNT_TOKENS_BETA_TOKENS, FABLE_MESSAGE_BETA_TOKENS,
    MCP_CLIENT_CAPABILITIES, MCP_PROTOCOL_VERSION, MCP_SERVERS_BETA_TOKEN, MESSAGE_BETA_TOKENS,
    OAUTH_BETA_TOKEN, STAINLESS_PACKAGE_VERSION, STAINLESS_RUNTIME_VERSION, claude_cli_user_agent,
    claude_code_user_agent, growthbook_user_agent, is_event_logging_path, normalize_version,
};

/// header wire 大小写映射。
/// Go 的 HTTP 服务器规范化 header，此映射还原 Claude CLI 抓包原始大小写。
static HEADER_WIRE_CASING: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert("accept", "Accept");
    m.insert("user-agent", "User-Agent");
    m.insert("x-stainless-retry-count", "X-Stainless-Retry-Count");
    m.insert("x-stainless-timeout", "X-Stainless-Timeout");
    m.insert("x-stainless-lang", "X-Stainless-Lang");
    m.insert("x-stainless-package-version", "X-Stainless-Package-Version");
    m.insert("x-stainless-os", "X-Stainless-OS");
    m.insert("x-stainless-arch", "X-Stainless-Arch");
    m.insert("x-stainless-runtime", "X-Stainless-Runtime");
    m.insert("x-stainless-runtime-version", "X-Stainless-Runtime-Version");
    m.insert("x-stainless-helper-method", "x-stainless-helper-method");
    m.insert(
        "anthropic-dangerous-direct-browser-access",
        "anthropic-dangerous-direct-browser-access",
    );
    m.insert("anthropic-version", "anthropic-version");
    m.insert("anthropic-beta", "anthropic-beta");
    m.insert(
        "anthropic-mcp-client-capabilities",
        "anthropic-mcp-client-capabilities",
    );
    m.insert("mcp-protocol-version", "mcp-protocol-version");
    m.insert("x-app", "x-app");
    m.insert("x-service-name", "x-service-name");
    m.insert("anthropic-client-platform", "anthropic-client-platform");
    m.insert("x-organization-uuid", "x-organization-uuid");
    m.insert("content-type", "content-type");
    m.insert("accept-language", "accept-language");
    m.insert("sec-fetch-mode", "sec-fetch-mode");
    m.insert("accept-encoding", "accept-encoding");
    m.insert("authorization", "authorization");
    m.insert("x-claude-code-session-id", "X-Claude-Code-Session-Id");
    m.insert("x-client-request-id", "x-client-request-id");
    m.insert("content-length", "content-length");
    m
});

/// 将规范化 key 转换为真实 wire 大小写。
fn resolve_wire_casing(key: &str) -> String {
    let lower = key.to_lowercase();
    if let Some(wk) = HEADER_WIRE_CASING.get(lower.as_str()) {
        wk.to_string()
    } else {
        key.to_string()
    }
}

/// 请求来源类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientType {
    ClaudeCode,
    API,
}

impl ClientType {
    /// 返回日志中使用的稳定客户端类型名。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCode => "claude_code",
            Self::API => "api",
        }
    }
}

const API_MAX_TOKENS_LIMIT: u64 = 64000;
const API_DEFAULT_MAX_TOKENS: u64 = 32000;
const CONTEXT_1M_BETA_TOKEN: &str = "context-1m-2025-08-07";
const FABLE_MODEL_ID: &str = "claude-fable-5";
const FABLE_FALLBACK_MODEL_ID: &str = "claude-opus-4-8";

/// 从逗号分隔的 anthropic-beta header 中剥离指定 token，保留其它 token 和相对顺序。
///
/// 对应 sub2api 的 `stripBetaTokensWithSet`：对非白名单模型默认过滤掉
/// `context-1m-2025-08-07` 这类受控 beta，防止 Sonnet/Haiku 误开 1M 档计费。
pub(crate) fn strip_beta_token(beta: &str, token: &str) -> String {
    beta.split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty() && *t != token)
        .collect::<Vec<_>>()
        .join(",")
}

/// 判断 model_id 是否命中账号配置的 1M 上下文白名单。
///
/// `allow_1m_models` 是逗号分隔的子串列表（大小写不敏感，空段被忽略）。
/// 任一子串在 model_id 中出现即视为命中。全空白名单 → 全部 filter。
pub(crate) fn matches_1m_whitelist(model_id: &str, allow_1m_models: &str) -> bool {
    let m = model_id.to_lowercase();
    allow_1m_models
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .any(|pat| m.contains(pat.to_lowercase().as_str()))
}

/// 合并必需的 beta 令牌与客户端传入的 beta 令牌。
pub(crate) fn merge_anthropic_beta(required: &str, incoming: &str) -> String {
    let mut seen = std::collections::HashSet::new();
    let mut tokens = Vec::new();
    for t in required.split(',') {
        let t = t.trim();
        if !t.is_empty() && seen.insert(t.to_string()) {
            tokens.push(t.to_string());
        }
    }
    for t in incoming.split(',') {
        let t = t.trim();
        if !t.is_empty() && seen.insert(t.to_string()) {
            tokens.push(t.to_string());
        }
    }
    tokens.join(",")
}

/// 按抓包顺序把 `context-1m` 插入到 `oauth` 后面。
pub(crate) fn order_context_1m_after_oauth(beta: String) -> String {
    let tokens = beta
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>();
    if !tokens.contains(&CONTEXT_1M_BETA_TOKEN) {
        return tokens.join(",");
    }

    let mut ordered = Vec::with_capacity(tokens.len());
    let mut inserted_context = false;
    for token in tokens
        .iter()
        .copied()
        .filter(|token| *token != CONTEXT_1M_BETA_TOKEN)
    {
        ordered.push(token);
        if token == OAUTH_BETA_TOKEN {
            ordered.push(CONTEXT_1M_BETA_TOKEN);
            inserted_context = true;
        }
    }
    if !inserted_context {
        ordered.push(CONTEXT_1M_BETA_TOKEN);
    }
    ordered.join(",")
}

/// 根据模型返回正确的 anthropic-beta 值。
fn beta_header_for_model(model_id: &str) -> String {
    if is_fable_model(model_id) {
        FABLE_MESSAGE_BETA_TOKENS.to_string()
    } else {
        MESSAGE_BETA_TOKENS.to_string()
    }
}

/// 根据 endpoint 返回当前 Claude Code 画像的必需 beta token。
fn beta_header_for_path(path: &str, model_id: &str) -> String {
    if path == "/v1/messages/count_tokens" {
        COUNT_TOKENS_BETA_TOKENS.to_string()
    } else if is_event_logging_path(path)
        || path.starts_with("/api/eval/")
        || path.starts_with("/api/oauth/")
        || path.starts_with("/api/claude_cli/bootstrap")
        || path.starts_with("/api/claude_code_grove")
        || path.starts_with("/api/claude_code_penguin_mode")
    {
        OAUTH_BETA_TOKEN.to_string()
    } else if path.starts_with("/v1/code/triggers") {
        CODE_TRIGGERS_BETA_TOKEN.to_string()
    } else if path.starts_with("/v1/mcp_servers") {
        MCP_SERVERS_BETA_TOKEN.to_string()
    } else {
        beta_header_for_model(model_id)
    }
}

fn is_fable_model(model_id: &str) -> bool {
    // 2.1.173 抓包显示 Fable `[1m]` 不会改变主请求画像，不能用后缀裁剪推断 beta。
    model_id == FABLE_MODEL_ID
}

/// 判断该 endpoint 是否应该发送 anthropic-beta。
fn requires_anthropic_beta(path: &str) -> bool {
    !path.starts_with("/mcp-registry/") && path != "/"
}

/// 判断该 endpoint 是否应该主动发送 JSON content-type。
///
/// 当前抓包中部分 GET 配置类端点不带 content-type；保留这些差异可以避免
/// “值正确但 header 集合不像真实客户端”的 wire 指纹偏差。
fn requires_json_content_type(path: &str) -> bool {
    !(path == "/"
        || path.starts_with("/mcp-registry/")
        || path.starts_with("/api/oauth/")
        || path.starts_with("/api/claude_code_grove")
        || path.starts_with("/api/claude_code_penguin_mode"))
}

/// 按当前 Claude Code 抓包中的 endpoint wire 顺序组织上游 header。
///
/// reqwest/hyper 是否保留大小写仍取决于底层 HTTP 实现；这里至少保证应用层
/// profile 的集合和插入顺序稳定，避免继续依赖 HashMap 的随机遍历顺序。
pub fn ordered_anthropic_headers(
    path: &str,
    headers: &HashMap<String, String>,
) -> Vec<(String, String)> {
    let mut ordered = Vec::new();
    let mut used = std::collections::HashSet::<String>::new();

    for name in wire_header_order(path) {
        let lower = name.to_ascii_lowercase();
        if lower == "host" {
            ordered.push(((*name).to_string(), "api.anthropic.com".to_string()));
            used.insert(lower);
            continue;
        }
        if lower == "connection" {
            let value = find_header_value(headers, name)
                .cloned()
                .unwrap_or_else(|| "keep-alive".to_string());
            ordered.push(((*name).to_string(), value));
            used.insert(lower);
            continue;
        }
        if let Some(value) = find_header_value(headers, name) {
            ordered.push(((*name).to_string(), value.clone()));
            used.insert(lower);
        }
    }

    let mut rest = headers
        .iter()
        .filter(|(k, _)| !used.contains(k.to_ascii_lowercase().as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    rest.sort_by(|a, b| a.0.to_ascii_lowercase().cmp(&b.0.to_ascii_lowercase()));
    ordered.extend(rest);
    ordered
}

fn find_header_value<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v)
}

fn wire_header_order(path: &str) -> &'static [&'static str] {
    if path.starts_with("/v1/messages") {
        &[
            "Accept",
            "Authorization",
            "Content-Type",
            "User-Agent",
            "X-Claude-Code-Session-Id",
            "X-Stainless-Arch",
            "X-Stainless-Lang",
            "X-Stainless-OS",
            "X-Stainless-Package-Version",
            "X-Stainless-Retry-Count",
            "X-Stainless-Runtime",
            "X-Stainless-Runtime-Version",
            "X-Stainless-Timeout",
            "anthropic-beta",
            "anthropic-dangerous-direct-browser-access",
            "anthropic-version",
            "x-app",
            "x-client-request-id",
            "Connection",
            "Host",
            "Accept-Encoding",
        ]
    } else if path.starts_with("/api/eval/") {
        &[
            "Authorization",
            "Content-Type",
            "anthropic-beta",
            "Connection",
            "User-Agent",
            "Accept",
            "Host",
            "Accept-Encoding",
        ]
    } else if path.starts_with("/api/event_logging/") {
        &[
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
    } else if path.starts_with("/api/claude_cli/bootstrap") {
        &[
            "Accept",
            "Accept-Encoding",
            "Authorization",
            "Content-Type",
            "User-Agent",
            "anthropic-beta",
            "Connection",
            "Host",
        ]
    } else if path.starts_with("/mcp-registry/") {
        &[
            "Accept",
            "Accept-Encoding",
            "User-Agent",
            "Connection",
            "Host",
        ]
    } else if path.starts_with("/api/oauth/")
        || path.starts_with("/api/claude_code_grove")
        || path.starts_with("/api/claude_code_penguin_mode")
    {
        &[
            "Accept",
            "Accept-Encoding",
            "Authorization",
            "User-Agent",
            "anthropic-beta",
            "Connection",
            "Host",
        ]
    } else if path.starts_with("/v1/code/triggers") {
        &[
            "Accept",
            "Accept-Encoding",
            "Authorization",
            "Content-Type",
            "User-Agent",
            "anthropic-beta",
            "anthropic-client-platform",
            "anthropic-version",
            "x-organization-uuid",
            "Connection",
            "Host",
        ]
    } else if path.starts_with("/v1/mcp_servers") {
        &[
            "Accept",
            "Accept-Encoding",
            "Authorization",
            "Content-Type",
            "User-Agent",
            "anthropic-beta",
            "anthropic-mcp-client-capabilities",
            "anthropic-version",
            "mcp-protocol-version",
            "Connection",
            "Host",
        ]
    } else {
        &[
            "Accept",
            "Accept-Encoding",
            "Authorization",
            "Content-Type",
            "User-Agent",
            "anthropic-beta",
            "Connection",
            "Host",
        ]
    }
}

/// 控制系统提示词 `<env>` 块中各环境字段是否「真值透传」(跳过改写)。
///
/// 这三项仅存在于请求体的系统提示词中,不进请求头/遥测,放开改写无跨通道连带风险。
/// `platform` 不在此列:它是请求头/遥测/提示词三处共享的跨通道字段,必须保持一致。
#[derive(Clone, Copy, Default)]
pub struct EnvPassthrough {
    /// 透传真实 `Shell:` 行。
    pub shell: bool,
    /// 透传真实 `OS Version:` 行。
    pub os_version: bool,
    /// 透传真实 `Working directory:` 行(同时跳过 home 路径前缀改写)。
    pub working_dir: bool,
}

/// `thinking.type=disabled` 的兼容改写配置。
///
/// 该配置只处理请求体顶层 `thinking` 参数,不处理历史 assistant thinking block。
/// 历史 block 带 Anthropic 签名,修改内容会造成另一类签名错误,仍交给网关的签名降级重试。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DisabledThinkingRewrite {
    /// 是否启用 `disabled` 到 `adaptive` 的自动改写。
    pub enabled: bool,
    /// 需要改写的精确模型 ID 列表。
    pub models: Vec<String>,
}

impl DisabledThinkingRewrite {
    /// 构造关闭状态的配置。
    ///
    /// @return 不改写任何请求的配置。
    pub fn off() -> Self {
        Self::default()
    }
}

/// Anthropic ephemeral cache_control TTL 改写模式。
///
/// 该设置只决定是否覆盖已有 `cache_control.type == "ephemeral"` 的 `ttl`;
/// 不创建新的 `cache_control`,避免无意增加缓存断点。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheControlTtlRewrite {
    /// 不改写请求体中的 cache_control ttl。
    #[default]
    Off,
    /// 将已有 ephemeral cache_control 的 ttl 改写为 5m。
    FiveMinutes,
    /// 将已有 ephemeral cache_control 的 ttl 改写为 1h。
    OneHour,
}

impl CacheControlTtlRewrite {
    /// 从 settings 字符串解析 TTL 改写模式。
    ///
    /// @param raw settings 中保存的原始字符串。
    /// @return 解析成功返回枚举值,非法值返回业务错误。
    pub fn parse(raw: &str) -> Result<Self, AppError> {
        match raw.trim() {
            "off" => Ok(Self::Off),
            "5m" => Ok(Self::FiveMinutes),
            "1h" => Ok(Self::OneHour),
            other => Err(AppError::BadRequest(format!(
                "'cache_control_ttl_rewrite' 必须是 off、5m 或 1h,当前值: {}",
                other
            ))),
        }
    }

    /// 返回目标 TTL 字符串;关闭改写时返回 `None`。
    ///
    /// @return 目标 TTL,或关闭改写时的 `None`。
    pub fn target_ttl(self) -> Option<&'static str> {
        match self {
            Self::Off => None,
            Self::FiveMinutes => Some("5m"),
            Self::OneHour => Some("1h"),
        }
    }

    /// 返回日志中使用的稳定设置名。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::FiveMinutes => "5m",
            Self::OneHour => "1h",
        }
    }
}

/// Claude Code messages 缓存断点改写模式。
///
/// `Auto` 会使用无状态保守滚动策略,稳定 Claude Code cache prefix 后只在相对稳定的
/// message 边界放置断点。`Rolling` 保留更积极的滚动策略,方便线上对照。
/// `Stateful` 在 `Auto` 基础上按会话保留上一轮实际发送的断点指纹,并避免异常暴涨
/// 请求污染正常主线。`Sub2api` 使用最后一条 message + 倒数第二个 user message
/// 的通用稳定断点策略,用于 API 模式默认策略和 Claude Code 对照实验。旧设置值
/// `stable` / `anchored` 会兼容解析到 `Auto`,避免历史配置继续进入已废弃的实验路径。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MessageCacheControlRewrite {
    /// 保持客户端原始 message cache_control。
    #[default]
    Off,
    /// 自动缓存修复策略。
    Auto,
    /// 更积极的滚动断点策略。
    Rolling,
    /// 会话级防污染缓存断点策略。
    Stateful,
    /// sub2api 风格的通用稳定尾部断点策略。
    Sub2api,
}

impl MessageCacheControlRewrite {
    /// 返回日志中使用的稳定设置名。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Auto => "auto",
            Self::Rolling => "rolling",
            Self::Stateful => "stateful",
            Self::Sub2api => "sub2api",
        }
    }

    /// 从 settings 字符串解析 message 缓存断点改写模式。
    ///
    /// @param raw settings 中保存的原始字符串。
    /// @return 解析成功返回枚举值,非法值返回业务错误。
    pub fn parse(raw: &str) -> Result<Self, AppError> {
        match raw.trim() {
            "off" => Ok(Self::Off),
            "auto" | "stable" | "anchored" => Ok(Self::Auto),
            "rolling" => Ok(Self::Rolling),
            "stateful" => Ok(Self::Stateful),
            "sub2api" => Ok(Self::Sub2api),
            other => Err(AppError::BadRequest(format!(
                "'message_cache_control_rewrite' 必须是 off、auto、rolling、stateful 或 sub2api,当前值: {}",
                other
            ))),
        }
    }
}

/// 处理所有请求的反检测改写。
pub struct Rewriter {
    stateful_cache: Mutex<StatefulCacheStore>,
}

/// stateful 缓存断点的延迟提交句柄。
///
/// 网关流式响应只有在上游 body 真正读完后才提交该句柄,避免并行 tool 请求提前复用
/// 一个 Anthropic 还没完成写入的缓存断点集合。
pub struct StatefulCacheCompletion {
    key: String,
    profile: StatefulRequestProfile,
    anchors: Vec<AnchorRecord>,
    snapshot_generation: Option<u64>,
    snapshot_cache_read_tokens: Option<u64>,
    has_reused_anchor: bool,
}

/// Anthropic 响应返回的 prompt cache 使用量。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatefulCacheUsage {
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_creation_ephemeral_5m_input_tokens: u64,
    pub cache_creation_ephemeral_1h_input_tokens: u64,
}

impl Rewriter {
    /// 创建请求体与请求头改写器。
    ///
    /// @return 请求体与请求头改写器实例。
    pub fn new() -> Self {
        Self {
            stateful_cache: Mutex::new(StatefulCacheStore::default()),
        }
    }

    // --- Header 改写 ---

    /// 处理出站 header 的反检测改写。
    pub fn rewrite_headers(
        &self,
        headers: &HashMap<String, String>,
        path: &str,
        account: &Account,
        client_type: ClientType,
        model_id: &str,
        body_map: &serde_json::Value,
    ) -> HashMap<String, String> {
        let profile = device_profile(account);
        let env = &profile.env;
        let version = normalize_version(&env.version);

        let mut out = HashMap::new();

        if client_type == ClientType::API {
            // API 模式：使用按 endpoint 拆分的当前 Claude Code header profile。
            out.insert("Accept".into(), "application/json".into());
            if requires_anthropic_beta(path) {
                out.insert(
                    "anthropic-beta".into(),
                    beta_header_for_path(path, model_id),
                );
            }
            if requires_json_content_type(path) {
                out.insert("content-type".into(), "application/json".into());
            }
            if path.starts_with("/api/eval/") {
                out.insert("Accept".into(), "*/*".into());
                out.insert("User-Agent".into(), growthbook_user_agent().into());
                out.insert("accept-encoding".into(), "gzip, deflate, br, zstd".into());
            } else if is_event_logging_path(path) {
                out.insert("Accept".into(), "application/json, text/plain, */*".into());
                out.insert("User-Agent".into(), claude_code_user_agent(version));
                out.insert(
                    "accept-encoding".into(),
                    "gzip, compress, deflate, br".into(),
                );
                out.insert("x-service-name".into(), "claude-code".into());
            } else if path.starts_with("/api/claude_code_penguin_mode") {
                out.insert("Accept".into(), "application/json, text/plain, */*".into());
                out.insert("User-Agent".into(), "axios/1.15.2".into());
                out.insert(
                    "accept-encoding".into(),
                    "gzip, compress, deflate, br".into(),
                );
            } else if path.starts_with("/api/claude_cli/bootstrap") {
                out.insert("Accept".into(), "application/json, text/plain, */*".into());
                out.insert("User-Agent".into(), claude_code_user_agent(version));
                out.insert(
                    "accept-encoding".into(),
                    "gzip, compress, deflate, br".into(),
                );
            } else if path.starts_with("/v1/code/triggers") {
                out.insert("Accept".into(), "application/json, text/plain, */*".into());
                out.insert("User-Agent".into(), claude_cli_user_agent(version));
                out.insert("anthropic-version".into(), "2023-06-01".into());
                out.insert("anthropic-client-platform".into(), "claude_code_cli".into());
                if let Some(ref org) = account.organization_uuid {
                    out.insert("x-organization-uuid".into(), org.clone());
                }
                out.insert(
                    "accept-encoding".into(),
                    "gzip, compress, deflate, br".into(),
                );
            } else if path.starts_with("/v1/mcp_servers") {
                out.insert("Accept".into(), "application/json, text/plain, */*".into());
                out.insert("User-Agent".into(), "axios/1.15.2".into());
                out.insert(
                    "anthropic-mcp-client-capabilities".into(),
                    MCP_CLIENT_CAPABILITIES.into(),
                );
                out.insert("anthropic-version".into(), "2023-06-01".into());
                out.insert("mcp-protocol-version".into(), MCP_PROTOCOL_VERSION.into());
                out.insert(
                    "accept-encoding".into(),
                    "gzip, compress, deflate, br".into(),
                );
            } else if path.starts_with("/api/")
                || path.starts_with("/mcp-registry/")
                || path.starts_with("/")
            {
                out.insert("Accept".into(), "application/json, text/plain, */*".into());
                out.insert("User-Agent".into(), claude_cli_user_agent(version));
                out.insert(
                    "accept-encoding".into(),
                    "gzip, compress, deflate, br".into(),
                );
            }

            if path.starts_with("/v1/messages") {
                out.insert("Accept".into(), "application/json".into());
                out.insert("User-Agent".into(), claude_cli_user_agent(version));
                out.insert("anthropic-version".into(), "2023-06-01".into());
                out.insert(
                    "anthropic-dangerous-direct-browser-access".into(),
                    "true".into(),
                );
                out.insert("x-app".into(), "cli".into());
                out.insert("accept-encoding".into(), "gzip, deflate, br, zstd".into());
                let stainless_os = stainless_os_from_platform(&env.platform);
                out.insert("X-Stainless-Lang".into(), "js".into());
                out.insert(
                    "X-Stainless-Package-Version".into(),
                    STAINLESS_PACKAGE_VERSION.into(),
                );
                out.insert("X-Stainless-OS".into(), stainless_os.into());
                out.insert("X-Stainless-Arch".into(), env.arch.clone());
                out.insert("X-Stainless-Runtime".into(), "node".into());
                out.insert(
                    "X-Stainless-Runtime-Version".into(),
                    STAINLESS_RUNTIME_VERSION.into(),
                );
                out.insert("X-Stainless-Retry-Count".into(), "0".into());
                out.insert("X-Stainless-Timeout".into(), "600".into());

                let request = request_profile(account, extract_session_id_from_body(body_map));
                out.insert("X-Claude-Code-Session-Id".into(), request.session_id);
                out.insert("x-client-request-id".into(), request.client_request_id);
            }
        } else {
            // CC 客户端模式：白名单 + 改写
            let allowed: std::collections::HashSet<&str> = [
                "accept",
                "user-agent",
                "content-type",
                "accept-encoding",
                "accept-language",
                "anthropic-beta",
                "anthropic-version",
                "anthropic-mcp-client-capabilities",
                "mcp-protocol-version",
                "anthropic-dangerous-direct-browser-access",
                "x-app",
                "sec-fetch-mode",
                "x-stainless-retry-count",
                "x-stainless-timeout",
                "x-stainless-lang",
                "x-stainless-package-version",
                "x-stainless-os",
                "x-stainless-arch",
                "x-stainless-runtime",
                "x-stainless-runtime-version",
                "x-stainless-helper-method",
                "x-claude-code-session-id",
                "x-client-request-id",
                "x-service-name",
                "anthropic-client-platform",
                "x-organization-uuid",
            ]
            .into_iter()
            .collect();

            let stainless_os = stainless_os_from_platform(&env.platform);
            for (k, v) in headers {
                let lower = k.to_lowercase();
                if !allowed.contains(lower.as_str()) {
                    continue;
                }
                let wire_key = resolve_wire_casing(k);
                match lower.as_str() {
                    "user-agent" => {
                        let user_agent = if path.starts_with("/api/eval/") {
                            growthbook_user_agent().to_string()
                        } else if is_event_logging_path(path)
                            || path.starts_with("/api/claude_cli/bootstrap")
                        {
                            claude_code_user_agent(version)
                        } else if path.starts_with("/v1/mcp_servers")
                            || path.starts_with("/api/claude_code_penguin_mode")
                        {
                            "axios/1.15.2".to_string()
                        } else {
                            claude_cli_user_agent(version)
                        };
                        out.insert(wire_key, user_agent);
                    }
                    "x-stainless-package-version" => {
                        out.insert(wire_key, STAINLESS_PACKAGE_VERSION.to_string());
                    }
                    "x-stainless-os" => {
                        out.insert(wire_key, stainless_os.to_string());
                    }
                    "x-stainless-arch" => {
                        out.insert(wire_key, env.arch.clone());
                    }
                    "x-stainless-runtime-version" => {
                        out.insert(wire_key, STAINLESS_RUNTIME_VERSION.to_string());
                    }
                    "x-organization-uuid" => {
                        if let Some(ref org) = account.organization_uuid {
                            out.insert(wire_key, org.clone());
                        }
                    }
                    _ => {
                        out.insert(wire_key, v.clone());
                    }
                }
            }

            if path.starts_with("/v1/messages") {
                out.entry("anthropic-dangerous-direct-browser-access".into())
                    .or_insert_with(|| "true".into());
            }

            // 合并客户端 beta 与必需 beta；对于不在账号 1M 白名单内的模型，
            // 强制剥掉 context-1m-2025-08-07（即便客户端传了）。
            // 对应 sub2api BetaPolicy(action=filter, model_whitelist=..., fallback=filter)：
            // 默认放行 "opus" 家族，运维可在账号设置里改 allow_1m_models 字段。
            let existing_beta = out.get("anthropic-beta").cloned().unwrap_or_default();
            let filtered_existing = if matches_1m_whitelist(model_id, &account.allow_1m_models) {
                existing_beta
            } else {
                strip_beta_token(&existing_beta, CONTEXT_1M_BETA_TOKEN)
            };
            if requires_anthropic_beta(path) {
                out.insert(
                    "anthropic-beta".into(),
                    order_context_1m_after_oauth(merge_anthropic_beta(
                        beta_header_for_path(path, model_id).as_str(),
                        &filtered_existing,
                    )),
                );
            }
            if is_event_logging_path(path) {
                out.insert("x-service-name".into(), "claude-code".into());
            }
            if path.starts_with("/v1/code/triggers") {
                out.insert("anthropic-client-platform".into(), "claude_code_cli".into());
                if let Some(ref org) = account.organization_uuid {
                    out.insert("x-organization-uuid".into(), org.clone());
                }
            }
            if path.starts_with("/v1/mcp_servers") || path.starts_with("/v1/code/triggers") {
                out.insert("anthropic-version".into(), "2023-06-01".into());
            }
            if path.starts_with("/v1/mcp_servers") {
                out.insert(
                    "anthropic-mcp-client-capabilities".into(),
                    MCP_CLIENT_CAPABILITIES.into(),
                );
                out.insert("mcp-protocol-version".into(), MCP_PROTOCOL_VERSION.into());
            }
        }

        out
    }

    // --- Body 改写 ---

    /// 根据端点和客户端类型改写请求体。
    pub fn rewrite_body(
        &self,
        body: &[u8],
        path: &str,
        account: &Account,
        client_type: ClientType,
        env_pt: EnvPassthrough,
        cache_ttl_rewrite: CacheControlTtlRewrite,
        message_cache_rewrite: MessageCacheControlRewrite,
    ) -> Vec<u8> {
        let (output, completion) = self.rewrite_body_inner(
            body,
            path,
            account,
            client_type,
            env_pt,
            cache_ttl_rewrite,
            message_cache_rewrite,
            &DisabledThinkingRewrite::off(),
        );
        self.complete_stateful_cache(completion);
        output
    }

    /// 根据端点和客户端类型改写请求体,并返回需要在上游成功完成后提交的缓存状态。
    ///
    /// @param body 原始下游请求体。
    /// @param path 请求路径。
    /// @param account 当前账号。
    /// @param client_type 客户端类型。
    /// @param env_pt 环境透传配置。
    /// @param cache_ttl_rewrite cache_control TTL 改写配置。
    /// @param message_cache_rewrite message cache_control 断点改写配置。
    /// @param disabled_thinking_rewrite `thinking.type=disabled` 兼容改写配置。
    /// @return 改写后的 body 与可选 stateful 延迟提交句柄。
    pub fn rewrite_body_with_stateful_completion(
        &self,
        body: &[u8],
        path: &str,
        account: &Account,
        client_type: ClientType,
        env_pt: EnvPassthrough,
        cache_ttl_rewrite: CacheControlTtlRewrite,
        message_cache_rewrite: MessageCacheControlRewrite,
        disabled_thinking_rewrite: &DisabledThinkingRewrite,
    ) -> (Vec<u8>, Option<StatefulCacheCompletion>) {
        self.rewrite_body_inner(
            body,
            path,
            account,
            client_type,
            env_pt,
            cache_ttl_rewrite,
            message_cache_rewrite,
            disabled_thinking_rewrite,
        )
    }

    /// 提交已经被上游成功消费完成的 stateful 缓存状态。
    ///
    /// @param completion `rewrite_body_with_stateful_completion` 返回的延迟提交句柄。
    pub fn complete_stateful_cache(&self, completion: Option<StatefulCacheCompletion>) {
        self.complete_stateful_cache_with_usage(completion, None);
    }

    /// 根据上游响应 usage 提交已经被确认的 stateful 缓存状态。
    ///
    /// @param completion `rewrite_body_with_stateful_completion` 返回的延迟提交句柄。
    /// @param usage 上游响应中的 cache usage；无 usage 时按完成即确认的旧行为处理。
    pub fn complete_stateful_cache_with_usage(
        &self,
        completion: Option<StatefulCacheCompletion>,
        usage: Option<StatefulCacheUsage>,
    ) {
        let Some(completion) = completion else {
            return;
        };
        if !stateful_completion_usage_is_trustworthy(&completion, usage) {
            info!(
                target: "cc2api::cache",
                session = completion.key.as_str(),
                snapshot_cache_read_tokens = completion.snapshot_cache_read_tokens.unwrap_or(0),
                response_cache_read_tokens = usage.map(|u| u.cache_read_input_tokens).unwrap_or(0),
                response_cache_creation_tokens = usage.map(|u| u.cache_creation_input_tokens).unwrap_or(0),
                "stateful cache completion rejected by usage feedback"
            );
            return;
        }
        if let Ok(mut cache) = self.stateful_cache.lock() {
            info!(
                target: "cc2api::cache",
                session = completion.key.as_str(),
                snapshot_cache_read_tokens = completion.snapshot_cache_read_tokens.unwrap_or(0),
                response_cache_read_tokens = usage.map(|u| u.cache_read_input_tokens).unwrap_or(0),
                response_cache_creation_tokens = usage.map(|u| u.cache_creation_input_tokens).unwrap_or(0),
                anchor_count = completion.anchors.len(),
                "stateful cache completion accepted"
            );
            cache.promote_if_current(
                completion.key,
                completion.profile,
                completion.anchors,
                completion.snapshot_generation,
                usage.map(|usage| usage.cache_read_input_tokens),
                completion.has_reused_anchor,
            );
        }
    }

    fn rewrite_body_inner(
        &self,
        body: &[u8],
        path: &str,
        account: &Account,
        client_type: ClientType,
        env_pt: EnvPassthrough,
        cache_ttl_rewrite: CacheControlTtlRewrite,
        message_cache_rewrite: MessageCacheControlRewrite,
        disabled_thinking_rewrite: &DisabledThinkingRewrite,
    ) -> (Vec<u8>, Option<StatefulCacheCompletion>) {
        if body.is_empty() {
            return (body.to_vec(), None);
        }

        let mut parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => return (body.to_vec(), None), // 非 JSON，直接透传
        };

        let mut stateful_completion = None;
        if path.starts_with("/v1/messages") {
            strip_empty_text_blocks(&mut parsed);
            self.rewrite_messages(&mut parsed, account, client_type, env_pt);
            stateful_completion = self.rewrite_message_cache_control(
                &mut parsed,
                account,
                client_type,
                message_cache_rewrite,
            );
            rewrite_existing_ephemeral_cache_control_ttl(&mut parsed, cache_ttl_rewrite);
            rewrite_disabled_thinking_to_adaptive(&mut parsed, disabled_thinking_rewrite);
        } else if is_event_logging_path(path) {
            self.rewrite_event_batch(&mut parsed, account);
        } else if path.starts_with("/api/eval/") {
            self.rewrite_growthbook_eval(&mut parsed, account);
        } else {
            self.rewrite_generic_identity(&mut parsed, account);
        }
        if client_type == ClientType::API {
            clean_session_id_from_body(&mut parsed);
        }

        let mut output = serde_json::to_vec(&parsed).unwrap_or_else(|_| body.to_vec());

        let profile = device_profile(account);
        let version = normalize_version(&profile.env.version);
        if path.starts_with("/v1/messages")
            && (account.billing_mode == BillingMode::Rewrite || client_type == ClientType::API)
        {
            output = compute_cch_attestation(output, version);
        }

        (output, stateful_completion)
    }

    /// 在请求体二次变更后刷新 `cch` attestation。
    ///
    /// @param body 已改写但后续又被签名整流修改的请求体。
    /// @param account 当前账号,用于判断 billing 模式和版本 seed。
    /// @param client_type 当前请求客户端类型,API mimicry 即使账号非 Rewrite 也会生成 CCH。
    /// @return 不需要 CCH 或没有 `cch=` 时原样返回;否则返回重新计算后的 body。
    pub fn refresh_cch_attestation(
        &self,
        body: Vec<u8>,
        account: &Account,
        client_type: ClientType,
    ) -> Vec<u8> {
        if account.billing_mode != BillingMode::Rewrite && client_type != ClientType::API {
            return body;
        }
        let profile = device_profile(account);
        let version = normalize_version(&profile.env.version);
        refresh_cch_attestation(body, version)
    }

    /// 处理 /v1/messages 请求体。
    fn rewrite_messages(
        &self,
        body: &mut serde_json::Value,
        account: &Account,
        client_type: ClientType,
        env_pt: EnvPassthrough,
    ) {
        let profile = device_profile(account);

        if client_type == ClientType::ClaudeCode {
            // 替换模式
            self.rewrite_metadata_user_id(body, &profile);
            self.rewrite_system_prompt(
                body,
                &profile.prompt,
                &profile.env.version,
                &account.billing_mode,
                env_pt,
            );
            scrub_git_user_in_reminders(body, &account.name);
        } else {
            // 注入模式
            self.inject_metadata_user_id(body, &profile, account);

            // 剥离 Claude Code 不会发送的字段
            if let Some(obj) = body.as_object_mut() {
                obj.remove("temperature");
                obj.remove("top_k");
                obj.remove("top_p");
                obj.remove("stop_sequences");

                // 确保 tools 字段存在
                obj.entry("tools")
                    .or_insert(serde_json::Value::Array(vec![]));

                // 确保 stream 为 true
                obj.insert("stream".into(), serde_json::Value::Bool(true));
            }

            // API mimicry 自己接管 system 断点；客户端 message 断点稍后按设置重打。
            strip_message_cache_control(body);

            normalize_api_max_tokens(body);
            ensure_fable_fallbacks(body);

            // 注入 Claude Code-like system 画像，并把原始 system 迁移到 messages。
            self.rewrite_api_system_prompt(body, &profile.env.version);
        }
    }

    /// 替换已有 metadata.user_id 中的 device_id（CC 客户端模式）。
    fn rewrite_metadata_user_id(&self, body: &mut serde_json::Value, profile: &DeviceProfile) {
        let user_id_str = {
            let metadata = match body.get("metadata").and_then(|m| m.as_object()) {
                Some(m) => m,
                None => return,
            };
            match metadata.get("user_id").and_then(|u| u.as_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => return,
            }
        };

        // 尝试 JSON 格式
        if let Ok(mut uid) = serde_json::from_str::<serde_json::Value>(&user_id_str) {
            if let Some(obj) = uid.as_object_mut() {
                obj.insert(
                    "device_id".into(),
                    serde_json::Value::String(profile.device_id.clone()),
                );
                let new_str = serde_json::to_string(&uid).unwrap_or_default();
                if let Some(metadata) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                    metadata.insert("user_id".into(), serde_json::Value::String(new_str));
                }
                return;
            }
        }

        // 旧格式：user_{device}_account_{uuid}_session_{uuid}
        if let Some(idx) = user_id_str.find("_account_") {
            let new_val = format!(
                "user_{}_account_{}",
                profile.device_id,
                &user_id_str[idx + 9..]
            );
            if let Some(metadata) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                metadata.insert("user_id".into(), serde_json::Value::String(new_val));
            }
        }
    }

    /// 为纯 API 调用创建 metadata.user_id。返回使用的 session_id。
    fn inject_metadata_user_id(
        &self,
        body: &mut serde_json::Value,
        profile: &DeviceProfile,
        account: &Account,
    ) -> Option<String> {
        let Some(obj) = body.as_object_mut() else {
            return None;
        };
        obj.entry("metadata")
            .or_insert_with(|| serde_json::json!({}));

        // API mimicry 不能信任下游自带的 user_id；否则 body/header 会话可能不一致。
        let session_id = stable_api_session_id(account, body);
        let request = request_profile(account, Some(session_id));
        let uid = serde_json::json!({
            "device_id": profile.device_id,
            "account_uuid": profile.account_uuid,
            "session_id": request.session_id,
        });
        let uid_str = serde_json::to_string(&uid).unwrap_or_default();
        if let Some(metadata) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
            metadata.insert("user_id".into(), serde_json::Value::String(uid_str));
        }
        Some(request.session_id)
    }

    /// 将 API 模式请求改写为 Claude Code-like system 画像。
    fn rewrite_api_system_prompt(&self, body: &mut serde_json::Value, version: &str) {
        let original_system_text = extract_api_original_system_text(body);
        let version = normalize_version(version);
        let first_msg = extract_first_user_message(body);
        let cc_suffix = compute_cc_version_suffix(&first_msg, version);
        let billing_block = serde_json::json!({
            "type": "text",
            "text": format!(
                "x-anthropic-billing-header: cc_version={version}.{cc_suffix}; cc_entrypoint=cli; cch=00000;"
            )
        });
        let banner_block = serde_json::json!({
            "type": "text",
            "text": CLAUDE_CODE_SYSTEM_PROMPT
        });
        let expansion_block = serde_json::json!({
            "type": "text",
            "text": CLAUDE_CODE_SYSTEM_PROMPT_EXPANSION,
            "cache_control": default_message_cache_control()
        });

        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "system".into(),
                serde_json::Value::Array(vec![billing_block, banner_block, expansion_block]),
            );
        }
        inject_original_system_as_messages(body, original_system_text);
    }

    // --- 系统提示词改写（仅 CC 客户端模式）---

    fn rewrite_system_prompt(
        &self,
        body: &mut serde_json::Value,
        pe: &CanonicalPromptEnvData,
        version: &str,
        billing_mode: &BillingMode,
        env_pt: EnvPassthrough,
    ) {
        let version = normalize_version(version);
        let rewrite_billing = *billing_mode == BillingMode::Rewrite;

        // CCH hash 计算
        let cch_hash = if rewrite_billing {
            let first_msg = extract_first_user_message(body);
            if !first_msg.is_empty() {
                compute_cc_version_suffix(&first_msg, version)
            } else {
                let mut bytes = [0u8; 2];
                rand::thread_rng().fill(&mut bytes);
                random_cc_version_suffix(bytes)
            }
        } else {
            String::new()
        };

        let rewrite = |text: &str| -> String {
            let mut text = text.to_string();
            if rewrite_billing {
                text = BILLING_VERSION_REGEX
                    .replace_all(&text, &format!("cc_version={}.{}", version, cch_hash))
                    .to_string();
                // 将已有的 cch 值重置为占位符，后续在序列化后通过 xxhash64 重新计算
                text = CCH_VALUE_REGEX.replace_all(&text, "cch=00000").to_string();
            } else if *billing_mode == BillingMode::Strip {
                text = BILLING_LINE_REGEX.replace_all(&text, "").to_string();
                text = BILLING_REGEX.replace_all(&text, "").to_string();
            }
            text = PLATFORM_REGEX
                .replace_all(&text, &format!("Platform: {}", pe.platform))
                .to_string();
            // Shell / OS Version / Working directory 仅在未开启「真值透传」时改写。
            // 这三项只存在于请求体提示词中,不进请求头/遥测,放开不影响指纹一致性。
            if !env_pt.shell {
                text = SHELL_REGEX
                    .replace_all(&text, &format!("Shell: {}", pe.shell))
                    .to_string();
            }
            if !env_pt.os_version {
                text = OS_VERSION_REGEX
                    .replace_all(&text, &format!("OS Version: {}", pe.os_version))
                    .to_string();
            }
            if !env_pt.working_dir {
                text = WORKING_DIR_REGEX
                    .replace_all(&text, &format!("${{1}}{}", pe.working_dir))
                    .to_string();
                // home 前缀改写与 working_dir 绑定:透传真实工作目录时一并跳过,
                // 否则真实路径仍会被 home 前缀改写污染。
                let home_prefix = if let Some(idx) = nth_index(&pe.working_dir, '/', 3) {
                    &pe.working_dir[..idx + 1]
                } else {
                    &pe.working_dir
                };
                text = HOME_PATH_REGEX.replace_all(&text, home_prefix).to_string();
            }
            text
        };

        let rewrite_in_reminders = |text: &str| -> String {
            SYSTEM_REMINDER_REGEX
                .replace_all(text, |caps: &regex::Captures| rewrite(&caps[0]))
                .to_string()
        };

        // 改写 body.system
        match body.get("system").cloned() {
            Some(serde_json::Value::String(sys)) => {
                body.as_object_mut()
                    .unwrap()
                    .insert("system".into(), serde_json::Value::String(rewrite(&sys)));
            }
            Some(serde_json::Value::Array(sys)) => {
                let filtered: Vec<serde_json::Value> = if *billing_mode == BillingMode::Strip {
                    sys.iter()
                        .filter(|item| {
                            if item.get("cache_control").is_some() {
                                return true;
                            }
                            if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                                if BILLING_LINE_REGEX.is_match(text) {
                                    let cleaned =
                                        BILLING_LINE_REGEX.replace_all(text, "").to_string();
                                    if cleaned.trim().is_empty() {
                                        return false;
                                    }
                                }
                            }
                            true
                        })
                        .cloned()
                        .collect()
                } else {
                    sys.clone()
                };

                let rewritten: Vec<serde_json::Value> = filtered
                    .into_iter()
                    .map(|mut item| {
                        if item.get("cache_control").is_some() {
                            return item;
                        }
                        if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                            let new_text = rewrite(text);
                            item.as_object_mut()
                                .unwrap()
                                .insert("text".into(), serde_json::Value::String(new_text));
                        }
                        item
                    })
                    .collect();

                body.as_object_mut()
                    .unwrap()
                    .insert("system".into(), serde_json::Value::Array(rewritten));
            }
            _ => {}
        }

        // 改写消息 — 仅在 <system-reminder> 标签内替换
        if let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) {
            for msg in messages.iter_mut() {
                rewrite_message_content(msg, &rewrite_in_reminders);
            }
        }
    }

    // --- 事件日志批量改写 ---

    fn rewrite_event_batch(&self, body: &mut serde_json::Value, account: &Account) {
        let profile = device_profile(account);

        let events = match body.get_mut("events").and_then(|e| e.as_array_mut()) {
            Some(e) => e,
            None => return,
        };

        let canonical_env = build_canonical_env_map(&profile.env);

        for event in events.iter_mut() {
            let e = match event.as_object_mut() {
                Some(e) => e,
                None => continue,
            };

            rewrite_event_fields(e, &profile, &canonical_env);
            if let Some(event_data) = e.get_mut("event_data").and_then(|v| v.as_object_mut()) {
                rewrite_event_fields(event_data, &profile, &canonical_env);
            }
        }
    }

    // --- GrowthBook remoteEval 改写 (POST /api/eval/{clientKey}) ---

    fn rewrite_growthbook_eval(&self, body: &mut serde_json::Value, account: &Account) {
        let profile = device_profile(account);
        let attrs = match body.get_mut("attributes").and_then(|a| a.as_object_mut()) {
            Some(a) => a,
            None => return,
        };

        apply_growthbook_attributes(attrs, &profile);
    }

    // --- 通用身份改写 ---

    fn rewrite_generic_identity(&self, body: &mut serde_json::Value, account: &Account) {
        if let Some(obj) = body.as_object_mut() {
            let profile = device_profile(account);
            if obj.contains_key("device_id") {
                obj.insert(
                    "device_id".into(),
                    serde_json::Value::String(profile.device_id),
                );
            }
            if obj.contains_key("email") {
                obj.insert("email".into(), serde_json::Value::String(profile.email));
            }
        }
    }
}

// --- 正则表达式 ---

static PLATFORM_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"Platform:\s*\S+").unwrap());
static SHELL_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"Shell:\s*[^\n<]+").unwrap());
static OS_VERSION_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"OS Version:\s*[^\n<]+").unwrap());
static WORKING_DIR_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"((?:Primary )?[Ww]orking directory:\s*)/[^\s<]+").unwrap());
static HOME_PATH_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"/(?:Users|home)/[^/\s]+/").unwrap());
static BILLING_LINE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)^\s*x-anthropic-billing-header:[^\n]*\n?").unwrap());
static BILLING_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"cc_version=[\d.]+\.[a-f0-9]{3};[^;]*;?").unwrap());
/// 仅匹配 cc_version 值部分，用于 Rewrite 模式保留 cc_entrypoint。
static BILLING_VERSION_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"cc_version=[\d.]+\.[a-f0-9]{3}").unwrap());
static CCH_VALUE_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"cch=[a-f0-9]{5}").unwrap());
static GIT_USER_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"Git user:\s*[^\n]+").unwrap());
static SYSTEM_REMINDER_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?s)<system-reminder>(.*?)</system-reminder>").unwrap());
static CLAUDE_MD_CONTENTS_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"Contents of /[^\n]*?CLAUDE\.md").unwrap());
static SKILLS_LIST_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?s)^(.+?\n\n)(- .+?)(\n</system-reminder>\s*)$").unwrap());
static DEFERRED_TOOLS_LIST_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?s)^(<system-reminder>\nThe following deferred tools are now available[^\n]*\n)(.+?)(\n</system-reminder>\s*)$",
    )
    .unwrap()
});

// --- CCH Attestation (xxhash64) ---

const CCH_ATTESTATION_SEED_LEGACY: u64 = 0x6E52736AC806831E;
const CCH_ATTESTATION_SEED_2156: u64 = 0x4D659218E32A3268;
const CCH_PLACEHOLDER: &[u8] = b"cch=00000";

/// 对序列化后的 body 字节计算 cch attestation 并原地替换占位符。
/// 算法：xxhash64(body_with_placeholder, seed) 取低 20 bits → 5 位十六进制。
fn compute_cch_attestation(mut body: Vec<u8>, version: &str) -> Vec<u8> {
    if let Some(pos) = body
        .windows(CCH_PLACEHOLDER.len())
        .position(|w| w == CCH_PLACEHOLDER)
    {
        let hash_input = cch_attestation_input(&body, version);
        let hash = xxhash_rust::xxh64::xxh64(&hash_input, cch_attestation_seed(version));
        let cch = format!("{:05x}", hash & 0xFFFFF);
        // "cch=" 占 4 字节，后续 5 字节是 "00000"。
        body[pos + 4..pos + 9].copy_from_slice(cch.as_bytes());
    }
    body
}

fn refresh_cch_attestation(mut body: Vec<u8>, version: &str) -> Vec<u8> {
    if let Some(pos) = find_cch_value(&body) {
        body[pos + 4..pos + 9].copy_from_slice(b"00000");
        compute_cch_attestation(body, version)
    } else {
        body
    }
}

fn cch_attestation_input(body: &[u8], version: &str) -> Vec<u8> {
    if !matches!(normalize_version(version), "2.1.172" | "2.1.173") {
        return body.to_vec();
    }

    let mut normalized = replace_top_level_string_value(body, "model", b"\"\"");
    normalized = remove_top_level_field(&normalized, "max_tokens");
    remove_top_level_field(&normalized, "fallbacks")
}

fn find_cch_value(body: &[u8]) -> Option<usize> {
    body.windows(CCH_PLACEHOLDER.len()).position(|window| {
        window.starts_with(b"cch=") && window[4..].iter().all(|b| b.is_ascii_hexdigit())
    })
}

// --- CCH fingerprint (SHA256) ---

const CCH_SALT: &str = "59cf53e54c78";
const CCH_POSITIONS: [usize; 3] = [4, 7, 20];

fn compute_cc_version_suffix(first_user_message_text: &str, version: &str) -> String {
    let code_units: Vec<u16> = first_user_message_text.encode_utf16().collect();
    let mut picked = String::new();
    for &pos in &CCH_POSITIONS {
        if let Some(code_unit) = code_units.get(pos) {
            if let Some(ch) = char::from_u32(*code_unit as u32) {
                picked.push(ch);
            } else {
                // JS 字符串索引可能取到 emoji 的单个 surrogate；Node 作为 UTF-8 输入时会按替换字符处理。
                picked.push('\u{FFFD}');
            }
        } else {
            picked.push('0');
        }
    }
    let input = format!("{}{}{}", CCH_SALT, picked, version);
    let hash = Sha256::digest(input.as_bytes());
    format!("{:x}", hash)[..3].to_string()
}

fn random_cc_version_suffix(bytes: [u8; 2]) -> String {
    format!("{:04x}", u16::from_be_bytes(bytes))[..3].to_string()
}

/// 返回指定 Claude Code 版本使用的 CCH attestation seed。
fn cch_attestation_seed(version: &str) -> u64 {
    match normalize_version(version) {
        "2.1.156" | "2.1.169" | "2.1.172" | "2.1.173" => CCH_ATTESTATION_SEED_2156,
        _ => CCH_ATTESTATION_SEED_LEGACY,
    }
}

fn replace_top_level_string_value(body: &[u8], field: &str, replacement: &[u8]) -> Vec<u8> {
    if let Some(range) = find_top_level_field_value_range(body, field) {
        let mut out = Vec::with_capacity(body.len() - range.len() + replacement.len());
        out.extend_from_slice(&body[..range.start]);
        out.extend_from_slice(replacement);
        out.extend_from_slice(&body[range.end..]);
        out
    } else {
        body.to_vec()
    }
}

fn remove_top_level_field(body: &[u8], field: &str) -> Vec<u8> {
    if let Some(range) = find_top_level_field_range(body, field) {
        let mut out = Vec::with_capacity(body.len() - range.len());
        out.extend_from_slice(&body[..range.start]);
        out.extend_from_slice(&body[range.end..]);
        out
    } else {
        body.to_vec()
    }
}

fn find_top_level_field_range(body: &[u8], field: &str) -> Option<std::ops::Range<usize>> {
    let found = find_top_level_field_ranges(body, field)?;
    let mut start = found.key_start;
    let mut end = found.value_end;
    let next = skip_json_ws(body, end);
    if next < body.len() && body[next] == b',' {
        end = next + 1;
    } else {
        let mut prev = start;
        while prev > 0 && is_json_ws(body[prev - 1]) {
            prev -= 1;
        }
        if prev > 0 && body[prev - 1] == b',' {
            start = prev - 1;
        }
    }
    Some(start..end)
}

fn find_top_level_field_value_range(body: &[u8], field: &str) -> Option<std::ops::Range<usize>> {
    let found = find_top_level_field_ranges(body, field)?;
    Some(found.value_start..found.value_end)
}

struct TopLevelFieldRanges {
    key_start: usize,
    value_start: usize,
    value_end: usize,
}

fn find_top_level_field_ranges(body: &[u8], field: &str) -> Option<TopLevelFieldRanges> {
    let mut idx = skip_json_ws(body, 0);
    if body.get(idx) != Some(&b'{') {
        return None;
    }
    idx += 1;

    loop {
        idx = skip_json_ws(body, idx);
        match body.get(idx) {
            Some(b'}') | None => return None,
            Some(b',') => {
                idx += 1;
                continue;
            }
            Some(b'"') => {}
            _ => return None,
        }

        let key_start = idx;
        let key_end = scan_json_string_end(body, key_start)?;
        let key = parse_json_string_bytes(&body[key_start..key_end])?;
        idx = skip_json_ws(body, key_end);
        if body.get(idx) != Some(&b':') {
            return None;
        }
        idx = skip_json_ws(body, idx + 1);
        let value_start = idx;
        let value_end = scan_json_value_end(body, value_start)?;
        if key == field {
            return Some(TopLevelFieldRanges {
                key_start,
                value_start,
                value_end,
            });
        }
        idx = skip_json_ws(body, value_end);
        if body.get(idx) == Some(&b',') {
            idx += 1;
        }
    }
}

fn scan_json_string_end(body: &[u8], start: usize) -> Option<usize> {
    if body.get(start) != Some(&b'"') {
        return None;
    }
    let mut idx = start + 1;
    let mut escaped = false;
    while idx < body.len() {
        let byte = body[idx];
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Some(idx + 1);
        }
        idx += 1;
    }
    None
}

fn scan_json_value_end(body: &[u8], start: usize) -> Option<usize> {
    let mut idx = start;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    while idx < body.len() {
        let byte = body[idx];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            idx += 1;
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                if depth == 0 {
                    return Some(idx);
                }
                depth -= 1;
            }
            b',' if depth == 0 => return Some(idx),
            _ => {}
        }
        idx += 1;
    }
    Some(idx)
}

fn parse_json_string_bytes(bytes: &[u8]) -> Option<String> {
    serde_json::from_slice(bytes).ok()
}

fn skip_json_ws(body: &[u8], mut idx: usize) -> usize {
    while idx < body.len() && is_json_ws(body[idx]) {
        idx += 1;
    }
    idx
}

fn is_json_ws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\n' | b'\r' | b'\t')
}

/// 从 messages 数组中提取首条用户消息文本。
fn extract_first_user_message(body: &serde_json::Value) -> String {
    let messages = match body.get("messages").and_then(|m| m.as_array()) {
        Some(m) => m,
        None => return String::new(),
    };
    for msg in messages {
        let m = match msg.as_object() {
            Some(m) => m,
            None => continue,
        };
        if m.get("role").and_then(|r| r.as_str()) != Some("user") {
            continue;
        }
        match m.get("content") {
            Some(serde_json::Value::String(c)) => return c.clone(),
            Some(serde_json::Value::Array(arr)) => {
                // Claude Code 会把环境上下文作为首个 text block，cc_version 后缀实际取用户 prompt block。
                if let Some(text) = arr
                    .iter()
                    .rev()
                    .find_map(|item| item.get("text").and_then(|t| t.as_str()))
                {
                    return text.to_string();
                }
            }
            _ => {}
        }
    }
    String::new()
}

/// 为 API 模式生成跨轮稳定的 session_id。
fn stable_api_session_id(account: &Account, body: &serde_json::Value) -> String {
    let seed = serde_json::json!({
        "account_id": account.id,
        "device_id": account.device_id,
        "first_user_text": extract_first_user_message(body),
    });
    stable_uuid_from_seed(&seed.to_string())
}

/// 从稳定种子生成 RFC 4122 v4 形态 UUID。
fn stable_uuid_from_seed(seed: &str) -> String {
    let hash = Sha256::digest(seed.as_bytes());
    let mut b = [0u8; 16];
    b.copy_from_slice(&hash[..16]);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}

/// 提取 API 原始 system 文本。
fn extract_api_original_system_text(body: &serde_json::Value) -> Option<String> {
    match body.get("system") {
        Some(serde_json::Value::String(text)) => {
            let text = text.trim();
            (!text.is_empty() && !text.starts_with(CLAUDE_CODE_SYSTEM_PROMPT))
                .then(|| text.to_string())
        }
        Some(serde_json::Value::Array(items)) => {
            let parts = items
                .iter()
                .filter_map(|item| item.get("text").and_then(|text| text.as_str()))
                .map(str::trim)
                .filter(|text| !text.is_empty() && !text.starts_with(CLAUDE_CODE_SYSTEM_PROMPT))
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join("\n\n"))
        }
        _ => None,
    }
}

/// 将 API 原始 system 指令迁移到 messages 前缀，保留用户原有语义。
fn inject_original_system_as_messages(
    body: &mut serde_json::Value,
    original_system_text: Option<String>,
) {
    let Some(original_system_text) = original_system_text else {
        return;
    };
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let mut messages = obj
        .remove("messages")
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    let mut prefixed = vec![
        serde_json::json!({
            "role": "user",
            "content": [{
                "type": "text",
                "text": format!("[System Instructions]\n{original_system_text}")
            }]
        }),
        serde_json::json!({
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "Understood. I will follow these instructions."
            }]
        }),
    ];
    prefixed.append(&mut messages);
    obj.insert("messages".into(), serde_json::Value::Array(prefixed));
}

fn rewrite_message_content<F>(msg: &mut serde_json::Value, rewrite_fn: &F)
where
    F: Fn(&str) -> String,
{
    match msg.get("content").cloned() {
        Some(serde_json::Value::String(s)) => {
            msg.as_object_mut()
                .unwrap()
                .insert("content".into(), serde_json::Value::String(rewrite_fn(&s)));
        }
        Some(serde_json::Value::Array(arr)) => {
            let rewritten: Vec<serde_json::Value> = arr
                .into_iter()
                .map(|mut item| {
                    if item.get("cache_control").is_some() {
                        return item;
                    }
                    if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                        let new_text = rewrite_fn(text);
                        item.as_object_mut()
                            .unwrap()
                            .insert("text".into(), serde_json::Value::String(new_text));
                    }
                    item
                })
                .collect();
            msg.as_object_mut()
                .unwrap()
                .insert("content".into(), serde_json::Value::Array(rewritten));
        }
        _ => {}
    }
}

fn build_canonical_env_map(env: &CanonicalEnvData) -> serde_json::Value {
    crate::model::identity::build_full_env_json(env)
}

/// 改写 event_logging 事件字段，兼容旧顶层结构和 2.1.156 的 event_data 结构。
fn rewrite_event_fields(
    map: &mut serde_json::Map<String, serde_json::Value>,
    profile: &DeviceProfile,
    canonical_env: &serde_json::Value,
) {
    if map.contains_key("device_id") {
        map.insert(
            "device_id".into(),
            serde_json::Value::String(profile.device_id.clone()),
        );
    }
    if map.contains_key("email") {
        map.insert(
            "email".into(),
            serde_json::Value::String(profile.email.clone()),
        );
    }

    map.remove("baseUrl");
    map.remove("base_url");
    map.remove("gateway");

    if map.contains_key("account_uuid") {
        map.insert(
            "account_uuid".into(),
            serde_json::Value::String(profile.account_uuid.clone()),
        );
    }
    if map.contains_key("organization_uuid") {
        if let Some(ref org) = profile.organization_uuid {
            map.insert(
                "organization_uuid".into(),
                serde_json::Value::String(org.clone()),
            );
        } else {
            map.remove("organization_uuid");
        }
    }

    if map.contains_key("env") {
        map.insert("env".into(), canonical_env.clone());
    }

    if let Some(p) = map.remove("process") {
        map.insert("process".into(), rewrite_process(&p, profile));
    }

    if let Some(am) = map.get("additional_metadata").and_then(|v| v.as_str()) {
        let rewritten = rewrite_additional_metadata(am);
        map.insert(
            "additional_metadata".into(),
            serde_json::Value::String(rewritten),
        );
    }

    if let Some(ua_str) = map.get("user_attributes").and_then(|v| v.as_str()) {
        let rewritten = rewrite_user_attributes_json_with_profile(ua_str, profile);
        map.insert(
            "user_attributes".into(),
            serde_json::Value::String(rewritten),
        );
    }
}

// --- 进程指纹改写 ---

fn rewrite_process(original: &serde_json::Value, profile: &DeviceProfile) -> serde_json::Value {
    let engine = base64::engine::general_purpose::STANDARD;
    match original {
        serde_json::Value::String(s) => {
            let decoded = match engine.decode(s) {
                Ok(d) => d,
                Err(_) => return original.clone(),
            };
            let mut obj: serde_json::Value = match serde_json::from_slice(&decoded) {
                Ok(v) => v,
                Err(_) => return original.clone(),
            };
            rewrite_process_fields(&mut obj, profile);
            let out = serde_json::to_vec(&obj).unwrap_or_default();
            serde_json::Value::String(engine.encode(&out))
        }
        serde_json::Value::Object(_) => {
            let mut obj = original.clone();
            rewrite_process_fields(&mut obj, profile);
            obj
        }
        _ => original.clone(),
    }
}

fn rewrite_process_fields(obj: &mut serde_json::Value, profile: &DeviceProfile) {
    if let Some(map) = obj.as_object_mut() {
        let uptime = map.get("uptime").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let snapshot = process_snapshot(&profile.process, &profile.device_id, uptime);
        let snapshot_json = process_snapshot_json(&snapshot);
        if let Some(snapshot_map) = snapshot_json.as_object() {
            for (key, value) in snapshot_map {
                map.insert(key.clone(), value.clone());
            }
        }
    }
}

// --- Base64 additional_metadata 改写 ---

fn rewrite_additional_metadata(encoded: &str) -> String {
    let engine = base64::engine::general_purpose::STANDARD;
    let decoded = match engine.decode(encoded) {
        Ok(d) => d,
        Err(_) => return encoded.to_string(),
    };
    let mut obj: serde_json::Value = match serde_json::from_slice(&decoded) {
        Ok(v) => v,
        Err(_) => return encoded.to_string(),
    };
    if let Some(map) = obj.as_object_mut() {
        map.remove("baseUrl");
        map.remove("base_url");
        map.remove("gateway");
    }
    let out = serde_json::to_vec(&obj).unwrap_or_default();
    engine.encode(&out)
}

/// 改写 GrowthBook 实验事件中 user_attributes JSON 字符串内的身份字段。
fn rewrite_user_attributes_json(json_str: &str, account: &Account) -> String {
    let profile = device_profile(account);
    rewrite_user_attributes_json_with_profile(json_str, &profile)
}

/// 改写 GrowthBook 实验事件中 user_attributes JSON 字符串内的身份字段。
fn rewrite_user_attributes_json_with_profile(json_str: &str, profile: &DeviceProfile) -> String {
    let mut obj: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return json_str.to_string(),
    };
    if let Some(map) = obj.as_object_mut() {
        apply_growthbook_attributes(map, profile);
    }
    serde_json::to_string(&obj).unwrap_or_else(|_| json_str.to_string())
}

/// 对 GrowthBook attributes 补齐 Claude Code 2.1.156 抓包中的关键身份字段。
fn apply_growthbook_attributes(
    map: &mut serde_json::Map<String, serde_json::Value>,
    profile: &DeviceProfile,
) {
    map.insert(
        "id".into(),
        serde_json::Value::String(profile.device_id.clone()),
    );
    map.insert(
        "deviceID".into(),
        serde_json::Value::String(profile.device_id.clone()),
    );
    map.insert(
        "email".into(),
        serde_json::Value::String(profile.email.clone()),
    );
    map.insert(
        "accountUUID".into(),
        serde_json::Value::String(profile.account_uuid.clone()),
    );
    if let Some(ref org) = profile.organization_uuid {
        map.insert(
            "organizationUUID".into(),
            serde_json::Value::String(org.clone()),
        );
    } else {
        map.remove("organizationUUID");
    }
    if let Some(ref sub) = profile.subscription_type {
        map.insert(
            "subscriptionType".into(),
            serde_json::Value::String(sub.clone()),
        );
    }
    map.insert(
        "userType".into(),
        serde_json::Value::String("external".into()),
    );
    map.insert(
        "rateLimitTier".into(),
        serde_json::Value::String(rate_limit_tier(&profile.subscription_type)),
    );
    map.insert("entrypoint".into(), serde_json::Value::String("cli".into()));
    map.remove("apiBaseUrlHost");

    if !profile.env.platform.is_empty() {
        map.insert(
            "platform".into(),
            serde_json::Value::String(profile.env.platform.clone()),
        );
    }
    if !profile.env.version.is_empty() {
        map.insert(
            "appVersion".into(),
            serde_json::Value::String(profile.env.version.clone()),
        );
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

/// 移除 system 和消息内容块中的 cache_control。
fn strip_cache_control(body: &mut serde_json::Value) {
    if let Some(sys) = body.get_mut("system").and_then(|s| s.as_array_mut()) {
        for item in sys.iter_mut() {
            if let Some(block) = item.as_object_mut() {
                block.remove("cache_control");
            }
        }
    }
    if let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) {
        for msg in messages.iter_mut() {
            if let Some(content) = msg.get_mut("content").and_then(|c| c.as_array_mut()) {
                for item in content.iter_mut() {
                    if let Some(block) = item.as_object_mut() {
                        block.remove("cache_control");
                    }
                }
            }
        }
    }
}

/// Anthropic 单个请求允许的 cache_control 断点数量上限。
const MAX_CACHE_BREAKPOINTS: usize = 4;
/// rolling 断点按 19 个真实 message content block 回退,贴近 Anthropic 20-block lookback。
const CACHE_LOOKBACK_STRIDE: usize = 19;
/// stateful 模式最多保留的会话数量,避免单进程长期运行时内存无界增长。
const STATEFUL_CACHE_MAX_SESSIONS: usize = 512;
/// 当前请求相比主线超过 3 倍时,视为可能的 Claude Code 内部暴涨请求。
const STATEFUL_SPIKE_RATIO: usize = 3;
/// 当前请求相比主线多出 128 个 block 以上时,才结合倍数判断为暴涨。
const STATEFUL_SPIKE_ABSOLUTE_DELTA: usize = 128;
/// 并发 sibling 与主线相差超过该值且无 anchor 命中时,不允许覆盖主线状态。
const STATEFUL_PARALLEL_DELTA: usize = CACHE_LOOKBACK_STRIDE * 2;
/// 主线达到一个 lookback 窗口后才认为成熟,避免 1-block 启动请求锁死会话。
const STATEFUL_BOOTSTRAP_BLOCKS: usize = CACHE_LOOKBACK_STRIDE;
/// 读缓存没有增长但写缓存暴涨时,拒绝把本轮断点推进成新的 stateful 主线。
const STATEFUL_USAGE_CREATION_REJECT_THRESHOLD: u64 = 32 * 1024;

impl Rewriter {
    /// 按配置改写 Claude Code messages 缓存断点。
    fn rewrite_message_cache_control(
        &self,
        body: &mut serde_json::Value,
        account: &Account,
        client_type: ClientType,
        mode: MessageCacheControlRewrite,
    ) -> Option<StatefulCacheCompletion> {
        if client_type == ClientType::API {
            match mode {
                MessageCacheControlRewrite::Off => {}
                MessageCacheControlRewrite::Auto
                | MessageCacheControlRewrite::Rolling
                | MessageCacheControlRewrite::Stateful
                | MessageCacheControlRewrite::Sub2api => {
                    strip_message_cache_control(body);
                    add_sub2api_message_cache_control(body, mode, client_type);
                }
            }
            return None;
        }

        match mode {
            MessageCacheControlRewrite::Off => {}
            MessageCacheControlRewrite::Auto => {
                stabilize_claude_code_cache_prefix(body);
                strip_cache_control_for_message_rewrite(body, client_type);
                add_auto_message_cache_control(body, client_type);
            }
            MessageCacheControlRewrite::Rolling => {
                stabilize_claude_code_cache_prefix(body);
                strip_cache_control_for_message_rewrite(body, client_type);
                add_rolling_message_cache_control(body, client_type);
            }
            MessageCacheControlRewrite::Stateful => {
                stabilize_claude_code_cache_prefix(body);
                strip_cache_control_for_message_rewrite(body, client_type);
                return self.add_stateful_message_cache_control(body, account);
            }
            MessageCacheControlRewrite::Sub2api => {
                strip_message_cache_control(body);
                add_sub2api_message_cache_control(body, mode, client_type);
            }
        }
        None
    }

    /// 为同一 Claude Code 会话放置防污染缓存断点。
    ///
    /// 这里记录的是上一轮实际发送给上游的 message block 指纹。正常线性请求会优先
    /// 复用这些断点,再补 bridge/tail；暴涨或并发异常请求只做临时选点,不覆盖主线。
    fn add_stateful_message_cache_control(
        &self,
        body: &mut serde_json::Value,
        account: &Account,
    ) -> Option<StatefulCacheCompletion> {
        let session_key = stateful_session_key(account, body);
        let profile = compute_stateful_request_profile(body);
        let snapshot = session_key.as_ref().and_then(|key| {
            self.stateful_cache
                .lock()
                .ok()
                .and_then(|mut cache| cache.get(key))
        });
        let outcome = select_stateful_message_cache_control_positions(
            body,
            profile.clone(),
            session_key.is_some(),
            snapshot.as_ref(),
        );

        let selected_labels = outcome
            .selected
            .iter()
            .map(|(message_idx, block_idx)| {
                message_cache_control_position_log_label(body, *message_idx, *block_idx)
            })
            .collect::<Vec<_>>();

        for (message_idx, block_idx) in &outcome.selected {
            add_message_cache_control_at_block(body, *message_idx, *block_idx);
        }

        let selected_diagnostics =
            stateful_selected_cache_diagnostics(body, &outcome, snapshot.as_ref());
        let prefix_diagnostics = cache_prefix_diagnostics(body);
        info!(
            target: "cc2api::cache",
            mode = "stateful",
            session = session_key.as_deref().unwrap_or("missing"),
            request_class = outcome.request_class.as_str(),
            available_slots = outcome.available_slots,
            block_count = outcome.message_content_blocks,
            normal_block_count = outcome.normal_block_count.unwrap_or(0),
            reused_count = outcome.reused_count,
            selected_count = outcome.selected.len(),
            promotion = outcome.promotion.as_str(),
            selected = ?selected_labels,
            selected_diag = ?selected_diagnostics,
            prefix_diag = ?prefix_diagnostics,
            skip_reason = outcome.skip_reason.as_deref().unwrap_or(""),
            "stateful message cache_control 选点完成"
        );

        let Some(key) = session_key else {
            return None;
        };
        if !outcome.should_promote {
            return None;
        }
        let anchors = outcome
            .selected
            .iter()
            .filter_map(|position| durable_stateful_anchor_record_at(body, *position))
            .collect::<Vec<_>>();
        if anchors.is_empty() {
            return None;
        }
        Some(StatefulCacheCompletion {
            key,
            profile,
            anchors,
            snapshot_generation: outcome.snapshot_generation,
            snapshot_cache_read_tokens: snapshot
                .and_then(|state| state.confirmed_cache_read_tokens),
            has_reused_anchor: outcome.reused_count > 0,
        })
    }
}

/// 移除 auto / rolling 模式要重新接管的所有缓存断点。
///
/// 这两个模式的目标是把 4 个 breakpoint 预算尽量用于 message history。top-level
/// automatic caching 与 system/tools 显式断点都会占用 slot,会让并行 tool 的尾部
/// 历史只剩 1-2 个断点,无法覆盖真实 20-block lookback。
fn strip_rewritten_cache_control(body: &mut serde_json::Value) {
    if let Some(obj) = body.as_object_mut() {
        obj.remove("cache_control");
    }
    if let Some(sys) = body.get_mut("system").and_then(|s| s.as_array_mut()) {
        for item in sys.iter_mut() {
            if let Some(block) = item.as_object_mut() {
                block.remove("cache_control");
            }
        }
    }
    if let Some(tools) = body.get_mut("tools").and_then(|t| t.as_array_mut()) {
        for tool in tools.iter_mut() {
            if let Some(block) = tool.as_object_mut() {
                block.remove("cache_control");
            }
        }
    }
    strip_message_cache_control(body);
}

/// 按客户端类型清理 message cache 改写要接管的断点。
fn strip_cache_control_for_message_rewrite(body: &mut serde_json::Value, client_type: ClientType) {
    match client_type {
        ClientType::ClaudeCode => strip_rewritten_cache_control(body),
        ClientType::API => strip_message_cache_control(body),
    }
}

/// 移除 messages 内容块中的 cache_control,不影响 system/tools 断点。
fn strip_message_cache_control(body: &mut serde_json::Value) {
    let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) else {
        return;
    };
    for msg in messages.iter_mut() {
        let Some(content) = msg.get_mut("content").and_then(|c| c.as_array_mut()) else {
            continue;
        };
        for item in content.iter_mut() {
            if let Some(block) = item.as_object_mut() {
                block.remove("cache_control");
            }
        }
    }
}

/// 稳定化 Claude Code prompt cache 的 tools/system 前缀顺序。
///
/// Anthropic cache prefix 顺序是 tools -> system -> messages；如果并行 tool 让
/// tools 或自动注入列表的顺序抖动，message 断点再正确也无法命中旧前缀。
fn stabilize_claude_code_cache_prefix(body: &mut serde_json::Value) {
    sort_tools_by_name(body);
    sort_auto_injected_text_lists(body);
    sort_parallel_tool_blocks(body);
}

/// 将 `tools[]` 按工具名排序,保持每个工具对象内容不变。
fn sort_tools_by_name(body: &mut serde_json::Value) {
    let Some(tools) = body.get_mut("tools").and_then(|tools| tools.as_array_mut()) else {
        return;
    };
    tools.sort_by(|a, b| {
        let a_name = a.get("name").and_then(|name| name.as_str()).unwrap_or("");
        let b_name = b.get("name").and_then(|name| name.as_str()).unwrap_or("");
        a_name.cmp(b_name)
    });
}

/// 排序 system 和首个 user message 中 Claude Code 自动注入的列表文本。
fn sort_auto_injected_text_lists(body: &mut serde_json::Value) {
    if let Some(system) = body
        .get_mut("system")
        .and_then(|system| system.as_array_mut())
    {
        for block in system.iter_mut() {
            sort_auto_injected_text_block(block);
        }
    }

    let Some(messages) = body
        .get_mut("messages")
        .and_then(|messages| messages.as_array_mut())
    else {
        return;
    };
    let Some(first) = messages.first_mut() else {
        return;
    };
    if first.get("role").and_then(|role| role.as_str()) != Some("user") {
        return;
    }
    let Some(content) = first
        .get_mut("content")
        .and_then(|content| content.as_array_mut())
    else {
        return;
    };
    for block in content.iter_mut() {
        sort_auto_injected_text_block(block);
    }
}

/// 对单个文本 block 中的 skills / deferred tools 列表做确定性排序。
fn sort_auto_injected_text_block(block: &mut serde_json::Value) {
    let Some(obj) = block.as_object_mut() else {
        return;
    };
    if obj.get("type").and_then(|value| value.as_str()) != Some("text") {
        return;
    }
    let Some(text) = obj.get("text").and_then(|value| value.as_str()) else {
        return;
    };

    let sorted = if is_sortable_skills_text(text) {
        sort_skills_text(text)
    } else if is_sortable_deferred_tools_text(text) {
        sort_deferred_tools_text(text)
    } else {
        None
    };
    if let Some(sorted) = sorted {
        obj.insert("text".into(), serde_json::Value::String(sorted));
    }
}

/// 稳定化同一批并行 tool_use / tool_result 的块顺序。
///
/// Claude Code 并行工具返回顺序可能随实际完成时间抖动。这里只排序连续的同类块,
/// 不跨过 text/thinking 等语义边界,避免改变对话结构。
fn sort_parallel_tool_blocks(body: &mut serde_json::Value) {
    let Some(messages) = body
        .get_mut("messages")
        .and_then(|messages| messages.as_array_mut())
    else {
        return;
    };

    for message in messages.iter_mut() {
        let role = message
            .get("role")
            .and_then(|role| role.as_str())
            .unwrap_or("")
            .to_string();
        let Some(content) = message
            .get_mut("content")
            .and_then(|content| content.as_array_mut())
        else {
            continue;
        };
        match role.as_str() {
            "assistant" => sort_contiguous_blocks_by_key(content, "tool_use", tool_use_sort_key),
            "user" => sort_contiguous_blocks_by_key(content, "tool_result", tool_result_sort_key),
            _ => {}
        }
    }
}

/// 对连续同类 block 片段排序,其他 block 保持原位。
fn sort_contiguous_blocks_by_key<F>(blocks: &mut [serde_json::Value], block_type: &str, key_fn: F)
where
    F: Fn(&serde_json::Value) -> String,
{
    let mut start = 0;
    while start < blocks.len() {
        while start < blocks.len() && block_type_of(&blocks[start]) != Some(block_type) {
            start += 1;
        }
        let mut end = start;
        while end < blocks.len() && block_type_of(&blocks[end]) == Some(block_type) {
            end += 1;
        }
        if end.saturating_sub(start) > 1 {
            blocks[start..end].sort_by_key(|block| key_fn(block));
        }
        start = end;
    }
}

/// 返回 content block 的类型。
fn block_type_of(block: &serde_json::Value) -> Option<&str> {
    block.get("type").and_then(|value| value.as_str())
}

/// 构造 tool_use 稳定排序键。
fn tool_use_sort_key(block: &serde_json::Value) -> String {
    let id = block
        .get("id")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let name = block
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let input = block
        .get("input")
        .map(|value| value.to_string())
        .unwrap_or_default();
    format!("{id}\u{0}{name}\u{0}{input}")
}

/// 构造 tool_result 稳定排序键。
fn tool_result_sort_key(block: &serde_json::Value) -> String {
    block
        .get("tool_use_id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string()
}

/// 判断文本是否是 Claude Code skills 列表。
fn is_sortable_skills_text(text: &str) -> bool {
    text.contains("User-invocable skills")
        || text.starts_with("<system-reminder>The following skills are available")
        || text.starts_with("<system-reminder>\nThe following skills are available")
        || text.contains("<available-skills>")
        || text.contains("<plugin-skills>")
}

/// 判断文本是否是 Claude Code deferred tools 列表。
fn is_sortable_deferred_tools_text(text: &str) -> bool {
    text.contains("deferred tools are now available")
}

/// 按条目排序 skills 列表文本。
fn sort_skills_text(text: &str) -> Option<String> {
    let captures = SKILLS_LIST_REGEX.captures(text)?;
    let header = captures.get(1)?.as_str();
    let entries = captures.get(2)?.as_str();
    let footer = captures.get(3)?.as_str();
    let mut parts = split_skill_entries(entries);
    parts.sort_unstable();
    let sorted_entries = parts.join("\n");
    let sorted = format!("{}{}{}", header, sorted_entries, footer);
    if sorted == text { None } else { Some(sorted) }
}

/// 按 `- ` 条目边界拆分 skills 文本,避免把多行描述拆散后重排。
fn split_skill_entries(entries: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    for line in entries.lines() {
        if line.starts_with("- ") && !current.is_empty() {
            parts.push(current);
            current = String::new();
        } else if !current.is_empty() {
            current.push('\n');
        }
        current.push_str(line);
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// 按条目排序 deferred tools 列表文本。
fn sort_deferred_tools_text(text: &str) -> Option<String> {
    let captures = DEFERRED_TOOLS_LIST_REGEX.captures(text)?;
    let header = captures.get(1)?.as_str();
    let entries = captures.get(2)?.as_str();
    let footer = captures.get(3)?.as_str();
    let mut parts: Vec<&str> = entries
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect();
    parts.sort_unstable();
    let sorted_entries = parts.join("\n");
    let sorted = format!("{}{}{}", header, sorted_entries, footer);
    if sorted == text { None } else { Some(sorted) }
}

/// 为 messages 放置自动缓存修复断点。
fn add_auto_message_cache_control(body: &mut serde_json::Value, client_type: ClientType) {
    let outcome = select_auto_message_cache_control_positions(body);
    let selected_labels = outcome
        .selected
        .iter()
        .map(|(message_idx, block_idx)| {
            message_cache_control_position_log_label(body, *message_idx, *block_idx)
        })
        .collect::<Vec<_>>();
    for (message_idx, block_idx) in &outcome.selected {
        add_message_cache_control_at_block(body, *message_idx, *block_idx);
    }

    let selected_diagnostics = selected_cache_diagnostics(body, &outcome.selected, "auto");
    let prefix_diagnostics = cache_prefix_diagnostics(body);
    let system_breakpoints = count_system_cache_breakpoints(body);
    let tool_breakpoints = count_tool_cache_breakpoints(body);
    info!(
        target: "cc2api::cache",
        mode = "auto",
        client_type = client_type.as_str(),
        available_slots = outcome.available_slots,
        message_content_blocks = outcome.message_content_blocks,
        system_breakpoints,
        tool_breakpoints,
        final_breakpoints = system_breakpoints + tool_breakpoints + outcome.selected.len(),
        selected_count = outcome.selected.len(),
        selected = ?selected_labels,
        selected_diag = ?selected_diagnostics,
        prefix_diag = ?prefix_diagnostics,
        skip_reason = outcome.skip_reason.as_deref().unwrap_or(""),
        "auto message cache_control 选点完成"
    );
}

/// 为 messages 放置更积极的 rolling 缓存断点。
fn add_rolling_message_cache_control(body: &mut serde_json::Value, client_type: ClientType) {
    let outcome = select_rolling_message_cache_control_positions(body);
    let selected_labels = outcome
        .selected
        .iter()
        .map(|(message_idx, block_idx)| {
            message_cache_control_position_log_label(body, *message_idx, *block_idx)
        })
        .collect::<Vec<_>>();
    for (message_idx, block_idx) in &outcome.selected {
        add_message_cache_control_at_block(body, *message_idx, *block_idx);
    }

    let selected_diagnostics = selected_cache_diagnostics(body, &outcome.selected, "rolling");
    let prefix_diagnostics = cache_prefix_diagnostics(body);
    let system_breakpoints = count_system_cache_breakpoints(body);
    let tool_breakpoints = count_tool_cache_breakpoints(body);
    info!(
        target: "cc2api::cache",
        mode = "rolling",
        client_type = client_type.as_str(),
        available_slots = outcome.available_slots,
        message_content_blocks = outcome.message_content_blocks,
        system_breakpoints,
        tool_breakpoints,
        final_breakpoints = system_breakpoints + tool_breakpoints + outcome.selected.len(),
        selected_count = outcome.selected.len(),
        selected = ?selected_labels,
        selected_diag = ?selected_diagnostics,
        prefix_diag = ?prefix_diagnostics,
        skip_reason = outcome.skip_reason.as_deref().unwrap_or(""),
        "rolling message cache_control 选点完成"
    );
}

/// 放置 sub2api 风格的稳定 message 断点。
///
/// API 客户端没有 Claude Code 并行 tool 的固定历史形态；这里不复用 rolling/lookback
/// 算法，避免长历史里断点持续漂移导致上游反复重建 prefix。Claude Code 显式选择
/// `sub2api` 模式时也使用同一算法,便于和 sub2api 行为做线上对照。
fn add_sub2api_message_cache_control(
    body: &mut serde_json::Value,
    configured_mode: MessageCacheControlRewrite,
    client_type: ClientType,
) {
    normalize_sub2api_message_cache_candidate_contents(body);
    let outcome = select_sub2api_message_cache_control_positions(body);
    for (message_idx, block_idx) in &outcome.selected {
        add_message_cache_control_at_block(body, *message_idx, *block_idx);
    }

    let selected_labels = outcome
        .selected
        .iter()
        .map(|(message_idx, block_idx)| {
            message_cache_control_position_log_label(body, *message_idx, *block_idx)
        })
        .collect::<Vec<_>>();
    let selected_diagnostics = selected_cache_diagnostics(body, &outcome.selected, "sub2api");
    let prefix_diagnostics = cache_prefix_diagnostics(body);
    let system_breakpoints = count_system_cache_breakpoints(body);
    let tool_breakpoints = count_tool_cache_breakpoints(body);
    info!(
        target: "cc2api::cache",
        mode = "sub2api",
        configured_mode = configured_mode.as_str(),
        client_type = client_type.as_str(),
        available_slots = outcome.available_slots,
        message_content_blocks = outcome.message_content_blocks,
        system_breakpoints,
        tool_breakpoints,
        final_breakpoints = system_breakpoints + tool_breakpoints + outcome.selected.len(),
        selected_count = outcome.selected.len(),
        selected = ?selected_labels,
        selected_diag = ?selected_diagnostics,
        prefix_diag = ?prefix_diagnostics,
        skip_reason = outcome.skip_reason.as_deref().unwrap_or(""),
        "sub2api message cache_control 选点完成"
    );
}

/// 断点选点结果。
struct CacheBreakpointSelection {
    selected: Vec<(usize, usize)>,
    available_slots: usize,
    message_content_blocks: usize,
    skip_reason: Option<String>,
}

/// stateful 会话缓存快照,用于锁外完成选点。
#[derive(Clone)]
struct StatefulCacheSnapshot {
    normal_profile: StatefulRequestProfile,
    normal_anchors: Vec<AnchorRecord>,
    generation: u64,
    confirmed_cache_read_tokens: Option<u64>,
}

/// 单个会话的正常主线缓存状态。
#[derive(Clone)]
struct SessionCacheState {
    normal_profile: StatefulRequestProfile,
    normal_anchors: Vec<AnchorRecord>,
    generation: u64,
    confirmed_cache_read_tokens: Option<u64>,
}

/// 需要持久到内存中的断点记录。
#[derive(Clone)]
struct AnchorRecord {
    fingerprint: String,
    prefix_hash: String,
    message_idx: usize,
    block_idx: usize,
    block_type: String,
}

/// 当前请求的结构画像。
#[derive(Clone, Debug, PartialEq, Eq)]
struct StatefulRequestProfile {
    block_count: usize,
    message_count: usize,
    tool_result_count: usize,
    assistant_tool_use_count: usize,
    last_user_text_hash: Option<String>,
    tail_role_type: String,
}

/// stateful 请求分类。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatefulRequestClass {
    NormalLinear,
    TransientSpike,
    ParallelSibling,
    Unknown,
}

impl StatefulRequestClass {
    /// 返回日志中使用的稳定分类名。
    fn as_str(self) -> &'static str {
        match self {
            Self::NormalLinear => "normal_linear",
            Self::TransientSpike => "transient_spike",
            Self::ParallelSibling => "parallel_sibling",
            Self::Unknown => "unknown",
        }
    }
}

/// stateful 主线状态更新决策。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatefulPromotion {
    Updated,
    IgnoredSpike,
    IgnoredParallel,
    MissingSession,
    NoReusableAnchor,
    ColdStart,
    BootstrapUpdated,
}

impl StatefulPromotion {
    /// 返回日志中使用的稳定更新原因。
    fn as_str(self) -> &'static str {
        match self {
            Self::Updated => "updated",
            Self::IgnoredSpike => "ignored_spike",
            Self::IgnoredParallel => "ignored_parallel",
            Self::MissingSession => "missing_session",
            Self::NoReusableAnchor => "ignored_no_reuse",
            Self::ColdStart => "cold_start",
            Self::BootstrapUpdated => "bootstrap_updated",
        }
    }
}

/// stateful 断点选点结果。
struct StatefulCacheBreakpointSelection {
    selected: Vec<(usize, usize)>,
    available_slots: usize,
    message_content_blocks: usize,
    normal_block_count: Option<usize>,
    reused_count: usize,
    request_class: StatefulRequestClass,
    promotion: StatefulPromotion,
    should_promote: bool,
    snapshot_generation: Option<u64>,
    skip_reason: Option<String>,
}

/// 单个 stateful 断点的 hash 诊断信息。
#[derive(Debug, PartialEq, Eq)]
struct StatefulSelectedCacheDiagnostic {
    label: String,
    source: &'static str,
    block_hash: String,
    prefix_hash: String,
    wire_prefix_hash: String,
}

/// cache prefix 的分段 hash 诊断信息。
#[derive(Debug, PartialEq, Eq)]
struct CachePrefixDiagnostics {
    root_hash: String,
    system_hash: String,
    tools_hash: String,
    messages_hash: String,
}

/// stateful 模式的内存会话断点缓存。
#[derive(Default)]
struct StatefulCacheStore {
    order: Vec<String>,
    sessions: HashMap<String, SessionCacheState>,
}

impl StatefulCacheStore {
    /// 读取某个 session 的主线快照,并标记为最近使用。
    ///
    /// @param key 会话缓存 key。
    /// @return 命中时返回可锁外使用的主线快照。
    fn get(&mut self, key: &str) -> Option<StatefulCacheSnapshot> {
        let state = self.sessions.get(key)?;
        let snapshot = StatefulCacheSnapshot {
            normal_profile: state.normal_profile.clone(),
            normal_anchors: state.normal_anchors.clone(),
            generation: state.generation,
            confirmed_cache_read_tokens: state.confirmed_cache_read_tokens,
        };
        self.touch(key);
        Some(snapshot)
    }

    /// 在快照未被其它请求推进时更新主线状态。
    ///
    /// @param key 会话缓存 key。
    /// @param profile 当前请求画像。
    /// @param anchors 当前请求实际放置的断点记录。
    /// @param expected_generation 选点时看到的 generation；冷启动为 `None`。
    fn promote_if_current(
        &mut self,
        key: String,
        profile: StatefulRequestProfile,
        anchors: Vec<AnchorRecord>,
        expected_generation: Option<u64>,
        confirmed_cache_read_tokens: Option<u64>,
        has_reused_anchor: bool,
    ) {
        if should_delay_stateful_cold_start_promotion(
            &profile,
            expected_generation,
            has_reused_anchor,
        ) {
            return;
        }
        if let Some(existing) = self.sessions.get(&key) {
            if let Some(expected) = expected_generation {
                if existing.generation != expected {
                    if profile.block_count < existing.normal_profile.block_count {
                        return;
                    }
                    if !anchors_share_fingerprint(&anchors, &existing.normal_anchors) {
                        return;
                    }
                    let class =
                        classify_stateful_request(&profile, Some(&existing.normal_profile), 0);
                    if class != StatefulRequestClass::NormalLinear {
                        return;
                    }
                }
            } else {
                if !has_reused_anchor && profile.block_count < STATEFUL_BOOTSTRAP_BLOCKS {
                    return;
                }
                if profile.block_count < existing.normal_profile.block_count {
                    return;
                }
                if is_stateful_transient_spike(&profile, &existing.normal_profile) {
                    return;
                }
                // expected_generation=None 表示本请求选点时没有看到任何主线快照。若提交时
                // 已经有其它请求先完成并建立主线,当前请求必须证明自己仍在同一条前缀上。
                if !anchors_share_fingerprint(&anchors, &existing.normal_anchors) {
                    return;
                }
            }
        }

        let previous_cache_read_tokens = self
            .sessions
            .get(&key)
            .and_then(|state| state.confirmed_cache_read_tokens);
        let generation = self
            .sessions
            .get(&key)
            .map(|state| state.generation.saturating_add(1))
            .unwrap_or(1);
        self.sessions.insert(
            key.clone(),
            SessionCacheState {
                normal_profile: profile,
                normal_anchors: anchors,
                generation,
                confirmed_cache_read_tokens: confirmed_cache_read_tokens
                    .or(previous_cache_read_tokens),
            },
        );
        self.touch(&key);
        while self.order.len() > STATEFUL_CACHE_MAX_SESSIONS {
            let old = self.order.remove(0);
            self.sessions.remove(&old);
        }
    }

    /// 将 session key 移动到 LRU 队尾。
    fn touch(&mut self, key: &str) {
        self.order.retain(|item| item != key);
        self.order.push(key.to_string());
    }
}

/// 判断冷启动请求是否只应临时打断点,不写入会话主线。
///
/// Claude Code 并行 tool 会在一个新会话开始时发出 1-2 个极小请求；这些请求可以
/// 临时降低单次成本,但没有足够历史证明自己是主线,不能成为后续 stateful anchor。
fn should_delay_stateful_cold_start_promotion(
    profile: &StatefulRequestProfile,
    expected_generation: Option<u64>,
    has_reused_anchor: bool,
) -> bool {
    expected_generation.is_none()
        && !has_reused_anchor
        && profile.block_count < STATEFUL_BOOTSTRAP_BLOCKS
}

/// 判断两个断点集合是否至少共享一个 block 指纹。
fn anchors_share_fingerprint(left: &[AnchorRecord], right: &[AnchorRecord]) -> bool {
    let right_keys = right
        .iter()
        .map(|anchor| (&anchor.fingerprint, &anchor.prefix_hash))
        .collect::<HashSet<_>>();
    left.iter()
        .any(|anchor| right_keys.contains(&(&anchor.fingerprint, &anchor.prefix_hash)))
}

/// 判断本轮上游 usage 是否支持把延迟提交断点确认为新的主线。
fn stateful_completion_usage_is_trustworthy(
    completion: &StatefulCacheCompletion,
    usage: Option<StatefulCacheUsage>,
) -> bool {
    let Some(usage) = usage else {
        return true;
    };
    let Some(snapshot_read_tokens) = completion.snapshot_cache_read_tokens else {
        return true;
    };
    !(usage.cache_read_input_tokens <= snapshot_read_tokens
        && usage.cache_creation_input_tokens > STATEFUL_USAGE_CREATION_REJECT_THRESHOLD)
}

/// 计算自动缓存修复断点位置。
fn select_auto_message_cache_control_positions(
    body: &serde_json::Value,
) -> CacheBreakpointSelection {
    select_message_cache_control_positions(body, CacheBreakpointMode::Auto)
}

/// 计算 stateful 可持久化的稳定文本断点位置。
fn select_stateful_durable_message_cache_control_positions(
    body: &serde_json::Value,
) -> CacheBreakpointSelection {
    select_message_cache_control_positions(body, CacheBreakpointMode::StatefulDurable)
}

/// 计算更积极的 rolling 断点位置。
fn select_rolling_message_cache_control_positions(
    body: &serde_json::Value,
) -> CacheBreakpointSelection {
    select_message_cache_control_positions(body, CacheBreakpointMode::Rolling)
}

/// 计算 sub2api 风格的 message 缓存断点。
///
/// 对齐 sub2api 的非 Claude Code 客户端策略：最后一条 message 一个断点；
/// messages 数量达到 4 时，再给倒数第二个 user message 一个断点。
fn select_sub2api_message_cache_control_positions(
    body: &serde_json::Value,
) -> CacheBreakpointSelection {
    let non_message_breakpoints = count_non_message_cache_breakpoints(body);
    let available = MAX_CACHE_BREAKPOINTS.saturating_sub(non_message_breakpoints);
    let Some(messages) = body
        .get("messages")
        .and_then(|messages| messages.as_array())
    else {
        return CacheBreakpointSelection {
            selected: Vec::new(),
            available_slots: available,
            message_content_blocks: 0,
            skip_reason: Some("no_messages".into()),
        };
    };
    let message_content_blocks = message_block_positions(body)
        .map(|blocks| blocks.len())
        .unwrap_or(0);

    if available == 0 {
        return CacheBreakpointSelection {
            selected: Vec::new(),
            available_slots: available,
            message_content_blocks,
            skip_reason: Some("no_available_slots".into()),
        };
    }
    if messages.is_empty() {
        return CacheBreakpointSelection {
            selected: Vec::new(),
            available_slots: available,
            message_content_blocks,
            skip_reason: Some("no_messages".into()),
        };
    }

    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    if let Some(position) =
        find_last_cacheable_block_in_message(body, messages.len() - 1, CacheBreakpointMode::Auto)
    {
        push_unique_position(&mut selected, &mut seen, position, available);
    }
    if messages.len() >= 4 {
        let mut user_count = 0;
        for message_idx in (0..messages.len()).rev() {
            if messages[message_idx]
                .get("role")
                .and_then(|role| role.as_str())
                != Some("user")
            {
                continue;
            }
            user_count += 1;
            if user_count != 2 {
                continue;
            }
            if let Some(position) =
                find_last_cacheable_block_in_message(body, message_idx, CacheBreakpointMode::Auto)
            {
                push_unique_position(&mut selected, &mut seen, position, available);
            }
            break;
        }
    }
    selected.sort_unstable();

    let skip_reason = if selected.is_empty() {
        Some("no_cacheable_blocks".into())
    } else {
        None
    };
    CacheBreakpointSelection {
        selected,
        available_slots: available,
        message_content_blocks,
        skip_reason,
    }
}

/// sub2api 风格断点策略兼容 string content。
///
/// sub2api 会把被选中 message 的字符串 content 升级为 text block 数组后再打断点；
/// 这里提前只规范化两个候选 message，避免改变其它历史消息结构。
fn normalize_sub2api_message_cache_candidate_contents(body: &mut serde_json::Value) {
    let Some(messages) = body
        .get("messages")
        .and_then(|messages| messages.as_array())
    else {
        return;
    };
    if messages.is_empty() {
        return;
    }

    let mut indexes = vec![messages.len() - 1];
    if messages.len() >= 4 {
        let mut user_count = 0;
        for message_idx in (0..messages.len()).rev() {
            if messages[message_idx]
                .get("role")
                .and_then(|role| role.as_str())
                != Some("user")
            {
                continue;
            }
            user_count += 1;
            if user_count == 2 {
                indexes.push(message_idx);
                break;
            }
        }
    }

    let Some(messages) = body
        .get_mut("messages")
        .and_then(|messages| messages.as_array_mut())
    else {
        return;
    };
    for message_idx in indexes {
        normalize_message_string_content(messages.get_mut(message_idx));
    }
}

/// 将单条 message 的字符串 content 升级为单个 text block。
fn normalize_message_string_content(message: Option<&mut serde_json::Value>) {
    let Some(message) = message else {
        return;
    };
    let Some(content) = message.get("content").cloned() else {
        return;
    };
    let serde_json::Value::String(text) = content else {
        return;
    };
    if let Some(obj) = message.as_object_mut() {
        obj.insert(
            "content".into(),
            serde_json::json!([{ "type": "text", "text": text }]),
        );
    }
}

/// 计算 stateful 防污染缓存断点位置。
fn select_stateful_message_cache_control_positions(
    body: &serde_json::Value,
    profile: StatefulRequestProfile,
    has_session_key: bool,
    snapshot: Option<&StatefulCacheSnapshot>,
) -> StatefulCacheBreakpointSelection {
    let base = select_auto_message_cache_control_positions(body);
    let durable = select_stateful_durable_message_cache_control_positions(body);
    let normal_block_count = snapshot.map(|state| state.normal_profile.block_count);
    if base.selected.is_empty() {
        return StatefulCacheBreakpointSelection {
            selected: base.selected,
            available_slots: base.available_slots,
            message_content_blocks: base.message_content_blocks,
            normal_block_count,
            reused_count: 0,
            request_class: StatefulRequestClass::Unknown,
            promotion: if has_session_key {
                StatefulPromotion::NoReusableAnchor
            } else {
                StatefulPromotion::MissingSession
            },
            should_promote: false,
            snapshot_generation: snapshot.map(|state| state.generation),
            skip_reason: base.skip_reason,
        };
    }

    let Some(snapshot) = snapshot else {
        let selected = merge_stateful_anchor_and_auto_positions(
            body,
            durable.selected,
            base.selected,
            base.available_slots,
        );
        return StatefulCacheBreakpointSelection {
            selected,
            available_slots: base.available_slots,
            message_content_blocks: base.message_content_blocks,
            normal_block_count,
            reused_count: 0,
            request_class: StatefulRequestClass::Unknown,
            promotion: if has_session_key {
                StatefulPromotion::ColdStart
            } else {
                StatefulPromotion::MissingSession
            },
            should_promote: has_session_key,
            snapshot_generation: None,
            skip_reason: base.skip_reason,
        };
    };

    let anchor_positions = find_stateful_anchor_positions(body, &snapshot.normal_anchors);
    let request_class = classify_stateful_request(
        &profile,
        Some(&snapshot.normal_profile),
        anchor_positions.len(),
    );
    if matches!(request_class, StatefulRequestClass::TransientSpike) {
        return StatefulCacheBreakpointSelection {
            selected: base.selected,
            available_slots: base.available_slots,
            message_content_blocks: base.message_content_blocks,
            normal_block_count,
            reused_count: anchor_positions.len(),
            request_class,
            promotion: StatefulPromotion::IgnoredSpike,
            should_promote: false,
            snapshot_generation: Some(snapshot.generation),
            skip_reason: base.skip_reason,
        };
    }

    if is_stateful_profile_bootstrap(&snapshot.normal_profile) && anchor_positions.is_empty() {
        let selected = merge_stateful_anchor_and_auto_positions(
            body,
            durable.selected,
            base.selected,
            base.available_slots,
        );
        return StatefulCacheBreakpointSelection {
            selected,
            available_slots: base.available_slots,
            message_content_blocks: base.message_content_blocks,
            normal_block_count,
            reused_count: anchor_positions.len(),
            request_class: StatefulRequestClass::NormalLinear,
            promotion: if has_session_key {
                StatefulPromotion::BootstrapUpdated
            } else {
                StatefulPromotion::MissingSession
            },
            should_promote: has_session_key,
            snapshot_generation: Some(snapshot.generation),
            skip_reason: base.skip_reason,
        };
    }

    if matches!(request_class, StatefulRequestClass::ParallelSibling) && anchor_positions.len() < 2
    {
        return StatefulCacheBreakpointSelection {
            selected: base.selected,
            available_slots: base.available_slots,
            message_content_blocks: base.message_content_blocks,
            normal_block_count,
            reused_count: 0,
            request_class,
            promotion: StatefulPromotion::IgnoredParallel,
            should_promote: false,
            snapshot_generation: Some(snapshot.generation),
            skip_reason: base.skip_reason,
        };
    }
    if anchor_positions.is_empty() {
        return StatefulCacheBreakpointSelection {
            selected: base.selected,
            available_slots: base.available_slots,
            message_content_blocks: base.message_content_blocks,
            normal_block_count,
            reused_count: 0,
            request_class,
            promotion: StatefulPromotion::NoReusableAnchor,
            should_promote: false,
            snapshot_generation: Some(snapshot.generation),
            skip_reason: base.skip_reason,
        };
    }

    let selected = merge_stateful_anchor_and_auto_positions(
        body,
        anchor_positions,
        durable.selected.into_iter().chain(base.selected).collect(),
        base.available_slots,
    );
    let reused_count = count_stateful_reused_positions(body, &selected, &snapshot.normal_anchors);
    let should_promote = has_session_key
        && (reused_count > 0
            || matches!(
                request_class,
                StatefulRequestClass::NormalLinear | StatefulRequestClass::Unknown
            ));
    let promotion = if should_promote {
        StatefulPromotion::Updated
    } else if has_session_key {
        StatefulPromotion::NoReusableAnchor
    } else {
        StatefulPromotion::MissingSession
    };

    StatefulCacheBreakpointSelection {
        selected,
        available_slots: base.available_slots,
        message_content_blocks: base.message_content_blocks,
        normal_block_count,
        reused_count,
        request_class,
        promotion,
        should_promote,
        snapshot_generation: Some(snapshot.generation),
        skip_reason: base.skip_reason,
    }
}

/// 计算当前请求的结构画像。
fn compute_stateful_request_profile(body: &serde_json::Value) -> StatefulRequestProfile {
    let messages = body
        .get("messages")
        .and_then(|messages| messages.as_array())
        .cloned()
        .unwrap_or_default();
    let message_blocks = message_block_positions(body).unwrap_or_default();
    let mut tool_result_count = 0;
    let mut assistant_tool_use_count = 0;
    let mut last_user_text_hash = None;
    let mut tail_role_type = String::new();

    for (message_idx, block_idx) in &message_blocks {
        let Some(message) = messages.get(*message_idx) else {
            continue;
        };
        let role = message
            .get("role")
            .and_then(|role| role.as_str())
            .unwrap_or("");
        let Some(block) = message
            .get("content")
            .and_then(|content| content.as_array())
            .and_then(|content| content.get(*block_idx))
        else {
            continue;
        };
        let block_type = block_type_of(block).unwrap_or("unknown");
        if role == "user" && block_type == "tool_result" {
            tool_result_count += 1;
        }
        if role == "assistant" && block_type == "tool_use" {
            assistant_tool_use_count += 1;
        }
        if role == "user" && block_type == "text" {
            if let Some(text) = block.get("text").and_then(|text| text.as_str()) {
                last_user_text_hash = Some(hash_text(text));
            }
        }
        tail_role_type = format!("{role}:{block_type}");
    }

    StatefulRequestProfile {
        block_count: message_blocks.len(),
        message_count: messages.len(),
        tool_result_count,
        assistant_tool_use_count,
        last_user_text_hash,
        tail_role_type,
    }
}

/// 判断当前请求相对主线属于哪类形态。
fn classify_stateful_request(
    profile: &StatefulRequestProfile,
    normal: Option<&StatefulRequestProfile>,
    reused_count: usize,
) -> StatefulRequestClass {
    let Some(normal) = normal else {
        return StatefulRequestClass::Unknown;
    };
    if is_stateful_transient_spike(profile, normal) {
        return StatefulRequestClass::TransientSpike;
    }
    let delta = profile.block_count.abs_diff(normal.block_count);
    if !is_stateful_profile_bootstrap(normal) && delta > STATEFUL_PARALLEL_DELTA && reused_count < 2
    {
        return StatefulRequestClass::ParallelSibling;
    }
    StatefulRequestClass::NormalLinear
}

/// 判断主线是否仍处于冷启动阶段。
fn is_stateful_profile_bootstrap(profile: &StatefulRequestProfile) -> bool {
    profile.block_count < STATEFUL_BOOTSTRAP_BLOCKS
}

/// 判断当前请求是否明显是相对主线的暴涨请求。
fn is_stateful_transient_spike(
    profile: &StatefulRequestProfile,
    normal: &StatefulRequestProfile,
) -> bool {
    profile.block_count > normal.block_count.saturating_mul(STATEFUL_SPIKE_RATIO)
        && profile.block_count.saturating_sub(normal.block_count) > STATEFUL_SPIKE_ABSOLUTE_DELTA
}

/// 查找上一轮断点在当前请求中的位置。
fn find_stateful_anchor_positions(
    body: &serde_json::Value,
    anchors: &[AnchorRecord],
) -> Vec<(usize, usize)> {
    if anchors.is_empty() {
        return Vec::new();
    }
    let message_blocks = message_block_positions(body).unwrap_or_default();
    let position_by_fingerprint = message_cacheable_position_fingerprint_map(
        body,
        &message_blocks,
        CacheBreakpointMode::StatefulDurable,
    );
    let mut positions = anchors
        .iter()
        .filter_map(|anchor| {
            position_by_fingerprint
                .get(&stateful_anchor_match_key(anchor))
                .copied()
        })
        .collect::<Vec<_>>();
    positions.sort_unstable();
    positions.dedup();
    positions
}

/// 合并复用锚点与当前 auto 选点。
fn merge_stateful_anchor_and_auto_positions(
    body: &serde_json::Value,
    anchor_positions: Vec<(usize, usize)>,
    auto_positions: Vec<(usize, usize)>,
    available: usize,
) -> Vec<(usize, usize)> {
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    for position in anchor_positions {
        push_unique_position(&mut selected, &mut seen, position, available);
    }

    // 已经占满 slot 时优先保持旧断点集合。只有尾部离最后一个旧断点足够远,
    // 才替换最后一个旧断点创建新尾部缓存,避免每轮正常增长都重建 cache。
    if selected.len() >= available {
        if let Some(replacement) =
            stateful_tail_anchor_replacement(body, &selected, &auto_positions)
        {
            selected.sort_unstable();
            selected.pop();
            seen = selected.iter().copied().collect();
            push_unique_position(&mut selected, &mut seen, replacement, available);
        }
        selected.sort_unstable();
        return selected;
    }

    for position in auto_positions {
        if !stateful_auto_position_can_follow_reused_anchors(&selected, position) {
            continue;
        }
        push_unique_position(&mut selected, &mut seen, position, available);
    }
    if selected.len() < available {
        if let Some(boundary) =
            claude_code_auto_injected_boundary_position(body, CacheBreakpointMode::Auto)
        {
            if stateful_auto_position_can_follow_reused_anchors(&selected, boundary) {
                push_unique_position(&mut selected, &mut seen, boundary, available);
            }
        }
    }
    selected.sort_unstable();
    selected
}

/// 判断是否应该用当前尾部断点替换上一轮最后一个断点。
fn stateful_tail_anchor_replacement(
    body: &serde_json::Value,
    selected: &[(usize, usize)],
    auto_positions: &[(usize, usize)],
) -> Option<(usize, usize)> {
    let tail_candidate = auto_positions
        .iter()
        .copied()
        .filter(|position| is_stateful_durable_anchor_at(body, *position))
        .max()?;
    if selected.contains(&tail_candidate) {
        return None;
    }
    let latest_anchor = selected.iter().copied().max()?;
    if tail_candidate <= latest_anchor {
        return None;
    }
    let distance = message_block_rank_distance(body, latest_anchor, tail_candidate)?;
    if distance < CACHE_LOOKBACK_STRIDE {
        return None;
    }
    Some(tail_candidate)
}

/// 判断自动补点是否能追加到复用锚点之后。
///
/// Anthropic 的 prompt cache key 包含 cache_control 本身。已有锚点前方新增断点会改变
/// 该锚点的真实 wire prefix,导致看似复用的旧锚点实际重新建缓存。
fn stateful_auto_position_can_follow_reused_anchors(
    selected: &[(usize, usize)],
    position: (usize, usize),
) -> bool {
    selected
        .iter()
        .copied()
        .max()
        .map(|latest_anchor| position > latest_anchor)
        .unwrap_or(true)
}

/// 计算两个 message content block 在请求线性 block 序列中的距离。
fn message_block_rank_distance(
    body: &serde_json::Value,
    from: (usize, usize),
    to: (usize, usize),
) -> Option<usize> {
    let message_blocks = message_block_positions(body)?;
    let ranks = message_blocks
        .iter()
        .enumerate()
        .map(|(rank, position)| (*position, rank))
        .collect::<HashMap<_, _>>();
    let from_rank = ranks.get(&from)?;
    let to_rank = ranks.get(&to)?;
    to_rank.checked_sub(*from_rank)
}

/// 统计本轮实际选中位置中复用了多少历史锚点。
fn count_stateful_reused_positions(
    body: &serde_json::Value,
    selected: &[(usize, usize)],
    anchors: &[AnchorRecord],
) -> usize {
    let anchor_keys = anchors
        .iter()
        .map(stateful_anchor_match_key)
        .collect::<HashSet<_>>();
    selected
        .iter()
        .filter_map(|position| stateful_anchor_match_key_at(body, *position))
        .filter(|key| anchor_keys.contains(key))
        .count()
}

/// 构造 stateful 选点诊断信息,只记录 hash 和结构标签,不记录 prompt 原文。
fn stateful_selected_cache_diagnostics(
    body: &serde_json::Value,
    outcome: &StatefulCacheBreakpointSelection,
    snapshot: Option<&StatefulCacheSnapshot>,
) -> Vec<StatefulSelectedCacheDiagnostic> {
    let anchor_keys = snapshot
        .map(|snapshot| {
            snapshot
                .normal_anchors
                .iter()
                .map(stateful_anchor_match_key)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    outcome
        .selected
        .iter()
        .map(|position| {
            let block_hash =
                message_block_fingerprint_at(body, *position).unwrap_or_else(|| "missing".into());
            let prefix_hash =
                message_prefix_hash_at(body, *position).unwrap_or_else(|| "missing".into());
            let wire_prefix_hash =
                message_wire_prefix_hash_at(body, *position).unwrap_or_else(|| "missing".into());
            let source = if stateful_anchor_match_key_at(body, *position)
                .map(|key| anchor_keys.contains(&key))
                .unwrap_or(false)
            {
                "reused_anchor"
            } else if matches!(outcome.promotion, StatefulPromotion::BootstrapUpdated) {
                "bootstrap"
            } else {
                "auto"
            };
            StatefulSelectedCacheDiagnostic {
                label: message_cache_control_position_log_label(body, position.0, position.1),
                source,
                block_hash: short_hash(&block_hash),
                prefix_hash: short_hash(&prefix_hash),
                wire_prefix_hash: short_hash(&wire_prefix_hash),
            }
        })
        .collect()
}

/// 构造 auto / rolling 选点诊断信息,只记录结构标签与 hash。
fn selected_cache_diagnostics(
    body: &serde_json::Value,
    positions: &[(usize, usize)],
    source: &'static str,
) -> Vec<StatefulSelectedCacheDiagnostic> {
    positions
        .iter()
        .map(|position| StatefulSelectedCacheDiagnostic {
            label: message_cache_control_position_log_label(body, position.0, position.1),
            source,
            block_hash: short_hash(
                &message_block_fingerprint_at(body, *position).unwrap_or_else(|| "missing".into()),
            ),
            prefix_hash: short_hash(
                &message_prefix_hash_at(body, *position).unwrap_or_else(|| "missing".into()),
            ),
            wire_prefix_hash: short_hash(
                &message_wire_prefix_hash_at(body, *position).unwrap_or_else(|| "missing".into()),
            ),
        })
        .collect()
}

/// 构造请求主要前缀段的 hash 诊断信息。
fn cache_prefix_diagnostics(body: &serde_json::Value) -> CachePrefixDiagnostics {
    CachePrefixDiagnostics {
        root_hash: short_hash(&body_root_without_messages_hash(body)),
        system_hash: short_hash(&stable_json_hash(
            body.get("system").unwrap_or(&serde_json::Value::Null),
        )),
        tools_hash: short_hash(&stable_json_hash(
            body.get("tools").unwrap_or(&serde_json::Value::Null),
        )),
        messages_hash: short_hash(&stable_json_hash(
            body.get("messages").unwrap_or(&serde_json::Value::Null),
        )),
    }
}

/// 计算指定 message block 之前完整请求前缀的 hash。
fn message_prefix_hash_at(body: &serde_json::Value, position: (usize, usize)) -> Option<String> {
    let mut prefix = body.clone();
    let messages = prefix
        .get_mut("messages")
        .and_then(|value| value.as_array_mut())?;
    if position.0 >= messages.len() {
        return None;
    }
    messages.truncate(position.0 + 1);
    let message = messages.get_mut(position.0)?;
    let content = message
        .get_mut("content")
        .and_then(|value| value.as_array_mut())?;
    if position.1 >= content.len() {
        return None;
    }
    content.truncate(position.1 + 1);
    Some(stable_json_hash(&prefix))
}

/// 计算指定 message block 之前包含 cache_control 的真实上游前缀 hash。
fn message_wire_prefix_hash_at(
    body: &serde_json::Value,
    position: (usize, usize),
) -> Option<String> {
    let mut prefix = body.clone();
    let messages = prefix
        .get_mut("messages")
        .and_then(|value| value.as_array_mut())?;
    if position.0 >= messages.len() {
        return None;
    }
    messages.truncate(position.0 + 1);
    let message = messages.get_mut(position.0)?;
    let content = message
        .get_mut("content")
        .and_then(|value| value.as_array_mut())?;
    if position.1 >= content.len() {
        return None;
    }
    content.truncate(position.1 + 1);
    Some(wire_json_hash(&prefix))
}

/// 计算不含 messages 的请求根级 hash。
fn body_root_without_messages_hash(body: &serde_json::Value) -> String {
    let mut root = body.clone();
    if let Some(map) = root.as_object_mut() {
        map.remove("messages");
    }
    stable_json_hash(&root)
}

/// 缩短 hash 便于日志横向比较。
fn short_hash(hash: &str) -> String {
    hash.chars().take(12).collect()
}

/// 构建可缓存 message block 指纹到当前位置的映射。
fn message_cacheable_position_fingerprint_map(
    body: &serde_json::Value,
    message_blocks: &[(usize, usize)],
    mode: CacheBreakpointMode,
) -> HashMap<(String, String), (usize, usize)> {
    let mut map = HashMap::new();
    for position in message_blocks {
        if !is_cacheable_message_block_at(body, *position, mode) {
            continue;
        }
        if let Some(key) = stateful_anchor_match_key_at(body, *position) {
            map.insert(key, *position);
        }
    }
    map
}

/// 返回指定 message block 的稳定指纹。
fn message_block_fingerprint_at(
    body: &serde_json::Value,
    position: (usize, usize),
) -> Option<String> {
    let messages = body
        .get("messages")
        .and_then(|messages| messages.as_array())?;
    let message = messages.get(position.0)?;
    let role = message
        .get("role")
        .and_then(|role| role.as_str())
        .unwrap_or("");
    let content = message
        .get("content")
        .and_then(|content| content.as_array())?;
    let block = content.get(position.1)?;
    let block_type = block_type_of(block).unwrap_or("unknown");
    let prev_hash = neighbor_block_hash(content, position.1.checked_sub(1));
    let payload = serde_json::json!({
        "role": role,
        "block_type": block_type,
        "message_idx": position.0,
        "block_idx": position.1,
        "block_hash": stable_json_hash(block),
        "prev_hash": prev_hash,
    });
    Some(stable_json_hash(&payload))
}

/// 构建当前断点的内存锚点记录。
fn anchor_record_at(body: &serde_json::Value, position: (usize, usize)) -> Option<AnchorRecord> {
    let fingerprint = message_block_fingerprint_at(body, position)?;
    let prefix_hash = message_prefix_hash_at(body, position)?;
    let block = body
        .get("messages")
        .and_then(|messages| messages.as_array())
        .and_then(|messages| messages.get(position.0))
        .and_then(|message| message.get("content"))
        .and_then(|content| content.as_array())
        .and_then(|content| content.get(position.1))?;
    Some(AnchorRecord {
        fingerprint,
        prefix_hash,
        message_idx: position.0,
        block_idx: position.1,
        block_type: block_type_of(block).unwrap_or("unknown").to_string(),
    })
}

/// 构造 stateful 锚点匹配键。
fn stateful_anchor_match_key(anchor: &AnchorRecord) -> (String, String) {
    (anchor.fingerprint.clone(), anchor.prefix_hash.clone())
}

/// 构造当前位置的 stateful 锚点匹配键。
fn stateful_anchor_match_key_at(
    body: &serde_json::Value,
    position: (usize, usize),
) -> Option<(String, String)> {
    Some((
        message_block_fingerprint_at(body, position)?,
        message_prefix_hash_at(body, position)?,
    ))
}

/// 构建可持久化的 stateful 锚点记录。
fn durable_stateful_anchor_record_at(
    body: &serde_json::Value,
    position: (usize, usize),
) -> Option<AnchorRecord> {
    if !is_stateful_durable_anchor_at(body, position) {
        return None;
    }
    anchor_record_at(body, position)
}

/// 判断指定 block 是否适合作为跨请求复用的会话锚点。
fn is_stateful_durable_anchor_at(body: &serde_json::Value, position: (usize, usize)) -> bool {
    is_cacheable_message_block_at(body, position, CacheBreakpointMode::StatefulDurable)
}

/// 计算相邻块 hash,用于避免重复 block 被误匹配。
fn neighbor_block_hash(content: &[serde_json::Value], index: Option<usize>) -> Option<String> {
    let index = index?;
    content.get(index).map(stable_json_hash)
}

/// 计算剥离 cache_control 后的 JSON hash。
fn stable_json_hash(value: &serde_json::Value) -> String {
    let mut stable = value.clone();
    strip_cache_control_recursive(&mut stable);
    let bytes = serde_json::to_vec(&stable).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

/// 计算保留 cache_control 的 JSON hash,用于比对真实上游缓存前缀。
fn wire_json_hash(value: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

/// 递归剥离 `cache_control`,保证指纹不受本次改写影响。
fn strip_cache_control_recursive(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.remove("cache_control");
            for child in map.values_mut() {
                strip_cache_control_recursive(child);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                strip_cache_control_recursive(child);
            }
        }
        _ => {}
    }
}

/// 计算文本 hash。
fn hash_text(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// 提取 stateful 会话缓存 key。
fn stateful_session_key(account: &Account, body: &serde_json::Value) -> Option<String> {
    let session_id = extract_claude_code_session_id(body)?;
    Some(format!("{}:{}", account.id, session_id))
}

/// 从 Claude Code metadata.user_id 中提取 session_id。
fn extract_claude_code_session_id(body: &serde_json::Value) -> Option<String> {
    let user_id = body
        .get("metadata")
        .and_then(|metadata| metadata.get("user_id"))
        .and_then(|user_id| user_id.as_str())?;

    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(user_id) {
        if let Some(session_id) = parsed.get("session_id").and_then(|value| value.as_str()) {
            if !session_id.is_empty() {
                return Some(session_id.to_string());
            }
        }
    }

    user_id
        .rfind("_session_")
        .map(|idx| user_id[idx + 9..].to_string())
        .filter(|session_id| !session_id.is_empty())
}

/// 缓存断点选点模式。
#[derive(Clone, Copy)]
enum CacheBreakpointMode {
    /// 保守自动修复。
    Auto,
    /// 激进 rolling 对照。
    Rolling,
    /// stateful 持久化锚点,只选择稳定文本块。
    StatefulDurable,
}

/// 计算 message 缓存断点。
fn select_message_cache_control_positions(
    body: &serde_json::Value,
    mode: CacheBreakpointMode,
) -> CacheBreakpointSelection {
    let non_message_breakpoints = count_non_message_cache_breakpoints(body);
    let available = MAX_CACHE_BREAKPOINTS.saturating_sub(non_message_breakpoints);
    let Some(message_blocks) = message_block_positions(body) else {
        return CacheBreakpointSelection {
            selected: Vec::new(),
            available_slots: available,
            message_content_blocks: 0,
            skip_reason: Some("no_messages".into()),
        };
    };

    if available == 0 {
        return CacheBreakpointSelection {
            selected: Vec::new(),
            available_slots: available,
            message_content_blocks: message_blocks.len(),
            skip_reason: Some("no_available_slots".into()),
        };
    }
    if message_blocks.is_empty() {
        return CacheBreakpointSelection {
            selected: Vec::new(),
            available_slots: available,
            message_content_blocks: 0,
            skip_reason: Some("no_content_blocks".into()),
        };
    }

    let Some((tail_idx, tail_position)) =
        find_tail_message_cache_breakpoint(body, &message_blocks, mode)
    else {
        return CacheBreakpointSelection {
            selected: Vec::new(),
            available_slots: available,
            message_content_blocks: message_blocks.len(),
            skip_reason: Some("no_cacheable_blocks".into()),
        };
    };

    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    push_unique_position(&mut selected, &mut seen, tail_position, available);
    let mut cursor = tail_idx;
    while selected.len() < available && cursor > 0 {
        let window_start = cursor.saturating_sub(CACHE_LOOKBACK_STRIDE);
        if let Some((position_idx, position)) =
            find_window_message_cache_breakpoint(body, &message_blocks, window_start, cursor, mode)
        {
            push_unique_position(&mut selected, &mut seen, position, available);
            cursor = position_idx;
            continue;
        }
        if window_start == 0 {
            break;
        }
        cursor = window_start;
    }

    if selected.len() < available {
        if let Some(boundary) = claude_code_auto_injected_boundary_position(body, mode) {
            push_unique_position(&mut selected, &mut seen, boundary, available);
        }
    }

    selected.sort_unstable();

    CacheBreakpointSelection {
        selected,
        available_slots: available,
        message_content_blocks: message_blocks.len(),
        skip_reason: None,
    }
}

/// 追加未选择过的位置,并保持不超过可用断点数量。
fn push_unique_position(
    selected: &mut Vec<(usize, usize)>,
    seen: &mut HashSet<(usize, usize)>,
    position: (usize, usize),
    available: usize,
) {
    if selected.len() >= available {
        return;
    }
    if seen.insert(position) {
        selected.push(position);
    }
}

/// 从尾部找到最新可放置断点的 message block。
fn find_tail_message_cache_breakpoint(
    body: &serde_json::Value,
    message_blocks: &[(usize, usize)],
    mode: CacheBreakpointMode,
) -> Option<(usize, (usize, usize))> {
    (0..message_blocks.len()).rev().find_map(|idx| {
        let position = message_blocks[idx];
        if is_cacheable_message_block_at(body, position, mode) {
            Some((idx, position))
        } else {
            None
        }
    })
}

/// 在 lookback 窗口中找到下一个缓存断点。
fn find_window_message_cache_breakpoint(
    body: &serde_json::Value,
    message_blocks: &[(usize, usize)],
    window_start: usize,
    cursor: usize,
    mode: CacheBreakpointMode,
) -> Option<(usize, (usize, usize))> {
    let preferred = (window_start..cursor).find_map(|idx| {
        let position = message_blocks[idx];
        if is_preferred_auto_message_block_at(body, position) {
            Some((idx, position))
        } else {
            None
        }
    });
    if preferred.is_some()
        || matches!(
            mode,
            CacheBreakpointMode::Auto | CacheBreakpointMode::StatefulDurable
        )
    {
        return preferred;
    }

    (window_start..cursor).find_map(|idx| {
        let position = message_blocks[idx];
        if is_cacheable_message_block_at(body, position, mode) {
            Some((idx, position))
        } else {
            None
        }
    })
}

/// 查找某条 message 内最后一个可放置 cache_control 的 content block。
fn find_last_cacheable_block_in_message(
    body: &serde_json::Value,
    message_idx: usize,
    mode: CacheBreakpointMode,
) -> Option<(usize, usize)> {
    let content_len = body
        .get("messages")
        .and_then(|messages| messages.as_array())
        .and_then(|messages| messages.get(message_idx))
        .and_then(|message| message.get("content"))
        .and_then(|content| content.as_array())
        .map(|content| content.len())?;

    (0..content_len)
        .rev()
        .map(|block_idx| (message_idx, block_idx))
        .find(|position| is_cacheable_message_block_at(body, *position, mode))
}

/// 构造缓存断点诊断标签,只记录结构化位置和 block 类型,避免日志泄漏 prompt 文本。
fn message_cache_control_position_log_label(
    body: &serde_json::Value,
    message_idx: usize,
    block_idx: usize,
) -> String {
    let Some(message) = body
        .get("messages")
        .and_then(|messages| messages.as_array())
        .and_then(|messages| messages.get(message_idx))
    else {
        return format!("msg[{message_idx}].content[{block_idx}] role=? type=?");
    };
    let role = message
        .get("role")
        .and_then(|role| role.as_str())
        .unwrap_or("?");
    let Some(block) = message
        .get("content")
        .and_then(|content| content.as_array())
        .and_then(|content| content.get(block_idx))
    else {
        return format!("msg[{message_idx}].content[{block_idx}] role={role} type=?");
    };
    let block_type = block
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("?");
    let tool_name = if block_type == "tool_use" {
        block.get("name").and_then(|value| value.as_str())
    } else {
        None
    };

    match tool_name {
        Some(name) => {
            format!(
                "msg[{message_idx}].content[{block_idx}] role={role} type={block_type} tool={name}"
            )
        }
        None => format!("msg[{message_idx}].content[{block_idx}] role={role} type={block_type}"),
    }
}

/// 统计 system/tools 上已经占用的 cache_control 断点数量。
///
/// 请求根级 `cache_control` 不是 Anthropic message history 的真实 block 断点,
/// 不参与 message 侧 slot 计算,否则会误跳过尾部历史断点。
fn count_non_message_cache_breakpoints(body: &serde_json::Value) -> usize {
    count_system_cache_breakpoints(body) + count_tool_cache_breakpoints(body)
}

/// 统计 system 上已经占用的 cache_control 断点数量。
fn count_system_cache_breakpoints(body: &serde_json::Value) -> usize {
    body.get("system")
        .and_then(|s| s.as_array())
        .map(|sys| {
            sys.iter()
                .filter(|item| item.get("cache_control").is_some())
                .count()
        })
        .unwrap_or(0)
}

/// 统计 tools 上已经占用的 cache_control 断点数量。
fn count_tool_cache_breakpoints(body: &serde_json::Value) -> usize {
    body.get("tools")
        .and_then(|t| t.as_array())
        .map(|tools| {
            tools
                .iter()
                .filter(|tool| tool.get("cache_control").is_some())
                .count()
        })
        .unwrap_or(0)
}

/// 按请求原始顺序收集 message 顶层 content block 位置。
fn message_block_positions(body: &serde_json::Value) -> Option<Vec<(usize, usize)>> {
    let messages = body.get("messages").and_then(|m| m.as_array())?;

    let mut positions = Vec::new();
    for (message_idx, msg) in messages.iter().enumerate() {
        let Some(content) = msg.get("content").and_then(|c| c.as_array()) else {
            continue;
        };
        for block_idx in 0..content.len() {
            positions.push((message_idx, block_idx));
        }
    }
    Some(positions)
}

/// 判断指定 message content block 是否可以直接放置 cache_control。
fn is_cacheable_message_block_at(
    body: &serde_json::Value,
    position: (usize, usize),
    mode: CacheBreakpointMode,
) -> bool {
    let Some(message) = body
        .get("messages")
        .and_then(|messages| messages.as_array())
        .and_then(|messages| messages.get(position.0))
    else {
        return false;
    };
    let role = message
        .get("role")
        .and_then(|role| role.as_str())
        .unwrap_or("");
    let Some(block) = message
        .get("content")
        .and_then(|content| content.as_array())
        .and_then(|content| content.get(position.1))
    else {
        return false;
    };
    is_cacheable_message_block(role, block, mode)
}

/// 判断指定 message content block 是否是 auto 模式优先选择的稳定边界。
fn is_preferred_auto_message_block_at(body: &serde_json::Value, position: (usize, usize)) -> bool {
    let Some(message) = body
        .get("messages")
        .and_then(|messages| messages.as_array())
        .and_then(|messages| messages.get(position.0))
    else {
        return false;
    };
    let role = message
        .get("role")
        .and_then(|role| role.as_str())
        .unwrap_or("");
    let Some(block) = message
        .get("content")
        .and_then(|content| content.as_array())
        .and_then(|content| content.get(position.1))
    else {
        return false;
    };
    let block_type = block.get("type").and_then(|value| value.as_str());
    matches!(role, "user" | "assistant") && block_type == Some("text")
}

/// 返回 Claude Code 自动注入块末尾的位置。
///
/// Claude Code 会把 hooks、skills、CLAUDE.md、deferred-tools、MCP 资源等稳定前缀
/// 放进首个 user message。尾部滚动断点用不满 slot 时,在该边界补一个断点,
/// 能避免这些稳定前缀被后续真实用户内容一起重写。
fn claude_code_auto_injected_boundary_position(
    body: &serde_json::Value,
    mode: CacheBreakpointMode,
) -> Option<(usize, usize)> {
    let messages = body.get("messages").and_then(|m| m.as_array())?;
    let first = messages.first()?;
    if first.get("role").and_then(|role| role.as_str()) != Some("user") {
        return None;
    }
    let content = first.get("content").and_then(|c| c.as_array())?;

    content
        .iter()
        .enumerate()
        .filter(|(_, block)| {
            is_cacheable_message_block("user", block, mode)
                && is_claude_code_auto_injected_block(block)
        })
        .map(|(block_idx, _)| (0, block_idx))
        .last()
}

/// 判断 content block 是否属于 Claude Code 自动注入的稳定前缀。
fn is_claude_code_auto_injected_block(block: &serde_json::Value) -> bool {
    let Some(text) = block.as_object().and_then(|obj| {
        if obj.get("type").and_then(|t| t.as_str()) != Some("text") {
            return None;
        }
        obj.get("text").and_then(|text| text.as_str())
    }) else {
        return false;
    };

    if text.starts_with("<system-reminder>") && text.contains("hook success") {
        return true;
    }
    if text.starts_with("<system-reminder>")
        && (text.contains("<available-skills>") || text.contains("<plugin-skills>"))
    {
        return true;
    }
    if text.contains("<system-reminder>") && CLAUDE_MD_CONTENTS_REGEX.is_match(text) {
        return true;
    }
    if text.contains("<deferred-tools>") || is_sortable_deferred_tools_text(text) {
        return true;
    }
    text.contains("<mcp-resources>") || text.contains("Available MCP servers:")
}

/// 在指定 message content block 上设置 cache_control。
fn add_message_cache_control_at_block(
    body: &mut serde_json::Value,
    message_idx: usize,
    block_idx: usize,
) {
    let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) else {
        return;
    };
    let Some(msg) = messages.get_mut(message_idx) else {
        return;
    };
    let Some(content) = msg.get_mut("content").and_then(|c| c.as_array_mut()) else {
        return;
    };
    let Some(block) = content
        .get_mut(block_idx)
        .and_then(|item| item.as_object_mut())
    else {
        return;
    };
    block.insert("cache_control".into(), default_message_cache_control());
}

/// 判断 message 内容块是否可以放置 cache_control。
fn is_cacheable_message_block(
    role: &str,
    block: &serde_json::Value,
    mode: CacheBreakpointMode,
) -> bool {
    let Some(block) = block.as_object() else {
        return false;
    };
    let block_type = block.get("type").and_then(|t| t.as_str());
    if matches!(block_type, Some("thinking" | "redacted_thinking")) {
        return false;
    }

    match mode {
        CacheBreakpointMode::Auto => match role {
            // assistant tool_use 是并行工具调度结构,自动修复不把它作为稳定缓存边界。
            "assistant" => block_type == Some("text"),
            "user" => matches!(block_type, Some("text" | "tool_result")),
            _ => false,
        },
        CacheBreakpointMode::Rolling => match role {
            // assistant tool_use 是并行调度结构,rolling 也不把它作为缓存边界。
            "assistant" => block_type == Some("text"),
            "user" => matches!(block_type, Some("text" | "tool_result")),
            _ => false,
        },
        CacheBreakpointMode::StatefulDurable => {
            matches!(role, "user" | "assistant") && block_type == Some("text")
        }
    }
}

/// message 断点稳定化新建 cache_control 的默认值。
fn default_message_cache_control() -> serde_json::Value {
    serde_json::json!({ "type": "ephemeral", "ttl": "1h" })
}

/// 将已有 ephemeral cache_control 的 ttl 改写为目标值。
///
/// 这里刻意只处理 sub2api TTL 改写同款范围,不递归 tool_result.content,
/// 也不新增任何 cache_control,避免改变缓存断点数量。
fn rewrite_existing_ephemeral_cache_control_ttl(
    body: &mut serde_json::Value,
    mode: CacheControlTtlRewrite,
) {
    let ttl = match mode.target_ttl() {
        Some(ttl) => ttl,
        None => return,
    };

    if let Some(obj) = body.as_object_mut() {
        rewrite_block_cache_control_ttl(obj, ttl);
    }

    if let Some(sys) = body.get_mut("system").and_then(|s| s.as_array_mut()) {
        for item in sys.iter_mut() {
            if let Some(block) = item.as_object_mut() {
                rewrite_block_cache_control_ttl(block, ttl);
            }
        }
    }

    if let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) {
        for msg in messages.iter_mut() {
            if let Some(content) = msg.get_mut("content").and_then(|c| c.as_array_mut()) {
                for item in content.iter_mut() {
                    if let Some(block) = item.as_object_mut() {
                        rewrite_block_cache_control_ttl(block, ttl);
                    }
                }
            }
        }
    }

    if let Some(tools) = body.get_mut("tools").and_then(|t| t.as_array_mut()) {
        for tool in tools.iter_mut() {
            if let Some(block) = tool.as_object_mut() {
                rewrite_block_cache_control_ttl(block, ttl);
            }
        }
    }
}

/// 覆盖单个已有 ephemeral cache_control 的 ttl。
fn rewrite_block_cache_control_ttl(
    block: &mut serde_json::Map<String, serde_json::Value>,
    ttl: &str,
) {
    let Some(cache_control) = block
        .get_mut("cache_control")
        .and_then(|value| value.as_object_mut())
    else {
        return;
    };
    if cache_control.get("type").and_then(|value| value.as_str()) != Some("ephemeral") {
        return;
    }
    cache_control.insert("ttl".into(), serde_json::Value::String(ttl.to_string()));
}

/// 将匹配模型上的顶层 `thinking.type=disabled` 改写为 `adaptive`。
///
/// 这个改写只碰顶层请求参数,用于兼容上游已不接受 disabled thinking 的模型。
/// 历史消息里的 signed thinking block 不在这里处理,避免破坏 Anthropic 签名校验。
fn rewrite_disabled_thinking_to_adaptive(
    body: &mut serde_json::Value,
    config: &DisabledThinkingRewrite,
) {
    if !config.enabled {
        return;
    }
    let model = body
        .get("model")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if !disabled_thinking_model_matches(model, &config.models) {
        return;
    }
    let Some(thinking) = body
        .get_mut("thinking")
        .and_then(|value| value.as_object_mut())
    else {
        return;
    };
    if thinking.get("type").and_then(|value| value.as_str()) != Some("disabled") {
        return;
    }
    thinking.insert("type".into(), serde_json::Value::String("adaptive".into()));
}

/// 判断模型是否命中 `thinking.type=disabled` 兼容改写列表。
fn disabled_thinking_model_matches(model: &str, configured_models: &[String]) -> bool {
    configured_models
        .iter()
        .any(|configured| configured.as_str() == model)
}

/// 按 Claude Code 2.1.156 抓包约束 API 模式的 `max_tokens`。
fn normalize_api_max_tokens(body: &mut serde_json::Value) {
    match body.get("max_tokens").and_then(|v| v.as_f64()) {
        Some(max_tokens) if max_tokens > API_MAX_TOKENS_LIMIT as f64 => {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("max_tokens".into(), serde_json::json!(API_MAX_TOKENS_LIMIT));
            }
        }
        Some(_) => {}
        None if body.get("max_tokens").is_none() => {
            let max_tokens = api_default_max_tokens(body);
            if let Some(obj) = body.as_object_mut() {
                obj.insert("max_tokens".into(), serde_json::json!(max_tokens));
            }
        }
        None => {}
    }
}

/// 返回 API 模式缺省 `max_tokens`。
fn api_default_max_tokens(body: &serde_json::Value) -> u64 {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_lowercase();

    if model == "claude-opus-4-8" || is_fable_model(&model) {
        API_MAX_TOKENS_LIMIT
    } else {
        API_DEFAULT_MAX_TOKENS
    }
}

/// 为 Fable 主请求补齐当前 Claude Code 抓包中的服务端 fallback 列表。
fn ensure_fable_fallbacks(body: &mut serde_json::Value) {
    let model = body
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or_default();
    if !is_fable_model(model) {
        return;
    }
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    obj.entry("fallbacks")
        .or_insert_with(|| serde_json::json!([{ "model": FABLE_FALLBACK_MODEL_ID }]));
}

/// 移除消息和 system 中的空文本内容块。
pub(crate) fn strip_empty_text_blocks(body: &mut serde_json::Value) {
    fn filter_blocks(blocks: &mut Vec<serde_json::Value>) {
        blocks.retain(|item| {
            if let Some(block) = item.as_object() {
                if block.get("type").and_then(|t| t.as_str()) == Some("text") {
                    let text = block.get("text").and_then(|t| t.as_str()).unwrap_or("");
                    if text.is_empty() {
                        return false;
                    }
                }
            }
            true
        });
        // 递归清理 tool_result 里的嵌套空文本,避免上游拒绝空 block。
        for item in blocks.iter_mut() {
            if let Some(block) = item.as_object_mut() {
                if block.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                    if let Some(content) = block.get_mut("content").and_then(|c| c.as_array_mut()) {
                        filter_blocks(content);
                    }
                }
            }
        }
    }

    if let Some(sys) = body.get_mut("system").and_then(|s| s.as_array_mut()) {
        filter_blocks(sys);
    }
    if let Some(messages) = body.get_mut("messages").and_then(|m| m.as_array_mut()) {
        for msg in messages.iter_mut() {
            if let Some(content) = msg.get_mut("content").and_then(|c| c.as_array_mut()) {
                filter_blocks(content);
            }
        }
    }
}

/// 从 body 中提取 Claude Code session_id。
///
/// @param body 已解析的请求体。
/// @return 能从 `metadata.user_id` 解析到 session 时返回 session_id。
pub fn extract_session_id_from_body(body: &serde_json::Value) -> Option<String> {
    extract_claude_code_session_id(body)
}

/// 清理 body 中的内部 _session_id 标记。
///
/// @param body 需要清理的请求体。
pub fn clean_session_id_from_body(body: &mut serde_json::Value) {
    if let Some(metadata) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        metadata.remove("_session_id");
    }
}

/// 判断请求来自 Claude Code 还是纯 API。
///
/// @param user_agent 下游请求的 User-Agent。
/// @param path 当前转发的 upstream path。
/// @param body 已解析的请求体。
/// @return 返回识别出的客户端类型。
pub fn detect_client_type(user_agent: &str, path: &str, body: &serde_json::Value) -> ClientType {
    let ua_lower = user_agent.to_lowercase();
    let is_claude_code_ua =
        ua_lower.starts_with("claude-code/") || ua_lower.starts_with("claude-cli/");
    let metadata_diag = claude_code_metadata_user_id_diagnostic(body);
    let client_type = if is_claude_code_ua {
        ClientType::ClaudeCode
    } else {
        ClientType::API
    };
    if path.starts_with("/v1/messages") {
        info!(
            target: "cc2api::cache",
            "[缓存诊断] 客户端识别: type={} ua={} metadata={}",
            client_type.as_str(),
            if is_claude_code_ua { "claude_code" } else { "api" },
            metadata_diag.human_summary()
        );
    }
    client_type
}

/// `metadata.user_id` 的脱敏诊断信息。
struct MetadataUserIdDiagnostic {
    strict_valid: bool,
    session_usable: bool,
    reason: &'static str,
    metadata_type: &'static str,
    metadata_keys: Vec<String>,
    user_id_type: &'static str,
    user_id_len: usize,
    user_id_hash: String,
    user_id_json_keys: Vec<String>,
    has_device_id: bool,
    has_account_uuid: bool,
    has_session_id: bool,
    account_uuid_like: bool,
    session_uuid_like: bool,
    legacy_account_marker: bool,
    legacy_session_marker: bool,
}

impl MetadataUserIdDiagnostic {
    /// 生成人类可读的脱敏摘要。
    fn human_summary(&self) -> String {
        let keys = if self.metadata_keys.is_empty() {
            "-".to_string()
        } else {
            self.metadata_keys.join(",")
        };
        let json_keys = if self.user_id_json_keys.is_empty() {
            "-".to_string()
        } else {
            self.user_id_json_keys.join(",")
        };
        let status = if self.strict_valid {
            "ok"
        } else if self.session_usable {
            "present"
        } else if matches!(self.reason, "missing_metadata" | "missing_user_id") {
            "missing"
        } else {
            "invalid"
        };
        if self.strict_valid {
            return format!(
                "{}(reason={},strict=Y,uid={} len={} hash={} keys={} json_keys={})",
                status,
                self.reason,
                self.user_id_type,
                self.user_id_len,
                empty_dash(&self.user_id_hash),
                keys,
                json_keys
            );
        }
        format!(
            "{}(reason={},strict={},meta={} keys={},uid={} len={} hash={},json_keys={},json_fields=device:{}/account:{}/session:{},uuid=account:{}/session:{},legacy=account:{}/session:{})",
            status,
            self.reason,
            yes_no(self.strict_valid),
            self.metadata_type,
            keys,
            self.user_id_type,
            self.user_id_len,
            empty_dash(&self.user_id_hash),
            json_keys,
            yes_no(self.has_device_id),
            yes_no(self.has_account_uuid),
            yes_no(self.has_session_id),
            yes_no(self.account_uuid_like),
            yes_no(self.session_uuid_like),
            yes_no(self.legacy_account_marker),
            yes_no(self.legacy_session_marker)
        )
    }
}

/// 生成 `metadata.user_id` 诊断,只记录结构、长度与 hash,不记录原值。
fn claude_code_metadata_user_id_diagnostic(body: &serde_json::Value) -> MetadataUserIdDiagnostic {
    let Some(metadata) = body.get("metadata") else {
        return metadata_user_id_diag("missing_metadata");
    };
    let Some(metadata_obj) = metadata.as_object() else {
        let mut diag = metadata_user_id_diag("metadata_not_object");
        diag.metadata_type = json_type_name(metadata);
        return diag;
    };

    let mut metadata_keys = metadata_obj.keys().cloned().collect::<Vec<_>>();
    metadata_keys.sort();
    let Some(user_id_value) = metadata_obj.get("user_id") else {
        let mut diag = metadata_user_id_diag("missing_user_id");
        diag.metadata_type = "object";
        diag.metadata_keys = metadata_keys;
        return diag;
    };
    let Some(user_id) = user_id_value.as_str().map(str::trim) else {
        let mut diag = metadata_user_id_diag("user_id_not_string");
        diag.metadata_type = "object";
        diag.metadata_keys = metadata_keys;
        diag.user_id_type = json_type_name(user_id_value);
        return diag;
    };
    if user_id.is_empty() {
        let mut diag = metadata_user_id_diag("user_id_empty");
        diag.metadata_type = "object";
        diag.metadata_keys = metadata_keys;
        diag.user_id_type = "string";
        return diag;
    }

    let mut diag = metadata_user_id_diag("invalid_user_id");
    diag.metadata_type = "object";
    diag.metadata_keys = metadata_keys;
    diag.user_id_type = "string";
    diag.user_id_len = user_id.len();
    diag.user_id_hash = short_sha256(user_id.as_bytes());

    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(user_id) {
        let Some(parsed_obj) = parsed.as_object() else {
            diag.reason = "json_user_id_not_object";
            return diag;
        };
        diag.user_id_json_keys = parsed_obj.keys().cloned().collect();
        diag.user_id_json_keys.sort();
        diag.has_device_id = parsed_obj
            .get("device_id")
            .and_then(|value| value.as_str())
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        diag.has_account_uuid = parsed_obj
            .get("account_uuid")
            .and_then(|value| value.as_str())
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        diag.has_session_id = parsed_obj
            .get("session_id")
            .and_then(|value| value.as_str())
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);
        diag.account_uuid_like = parsed_obj
            .get("account_uuid")
            .and_then(|value| value.as_str())
            .map(is_uuid_like)
            .unwrap_or(false);
        diag.session_uuid_like = parsed_obj
            .get("session_id")
            .and_then(|value| value.as_str())
            .map(is_uuid_like)
            .unwrap_or(false);
        diag.session_usable = diag.has_session_id;
        diag.strict_valid = diag.has_device_id && diag.account_uuid_like && diag.session_uuid_like;
        diag.reason = if diag.strict_valid {
            "valid_json"
        } else if diag.session_usable {
            "json_session_present"
        } else {
            "invalid_json_shape"
        };
        return diag;
    }

    diag.legacy_account_marker = user_id.contains("_account_");
    diag.legacy_session_marker = user_id.contains("_session_");
    let Some(account_idx) = user_id.find("_account_") else {
        diag.reason = "missing_legacy_account_marker";
        return diag;
    };
    let Some(session_idx) = user_id[account_idx + 9..].find("_session_") else {
        diag.reason = "missing_legacy_session_marker";
        return diag;
    };
    let session_idx = account_idx + 9 + session_idx;
    let account_uuid = &user_id[account_idx + 9..session_idx];
    let session_uuid = &user_id[session_idx + 9..];
    diag.account_uuid_like = is_uuid_like(account_uuid);
    diag.session_uuid_like = is_uuid_like(session_uuid);
    diag.session_usable = !session_uuid.trim().is_empty();
    diag.strict_valid =
        user_id.starts_with("user_") && diag.account_uuid_like && diag.session_uuid_like;
    diag.reason = if diag.strict_valid {
        "valid_legacy"
    } else if diag.session_usable {
        "legacy_session_present"
    } else {
        "invalid_legacy_shape"
    };
    diag
}

/// 构造空白 `metadata.user_id` 诊断。
fn metadata_user_id_diag(reason: &'static str) -> MetadataUserIdDiagnostic {
    MetadataUserIdDiagnostic {
        strict_valid: false,
        session_usable: false,
        reason,
        metadata_type: "missing",
        metadata_keys: Vec::new(),
        user_id_type: "missing",
        user_id_len: 0,
        user_id_hash: String::new(),
        user_id_json_keys: Vec::new(),
        has_device_id: false,
        has_account_uuid: false,
        has_session_id: false,
        account_uuid_like: false,
        session_uuid_like: false,
        legacy_account_marker: false,
        legacy_session_marker: false,
    }
}

/// 返回 JSON 值的简短类型名。
fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// 计算日志用 12 位 SHA-256 摘要。
fn short_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(&digest[..6])
}

/// bool 的中文短值,让日志更容易肉眼扫读。
fn yes_no(value: bool) -> &'static str {
    if value { "Y" } else { "N" }
}

/// 空字符串在日志里显示为 `-`。
fn empty_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

/// 校验 UUID 字符串格式,避免普通 API metadata 误触 ClaudeCode 分支。
fn is_uuid_like(value: &str) -> bool {
    let value = value.trim();
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

const CLAUDE_CODE_SYSTEM_PROMPT: &str = "You are Claude Code, Anthropic's official CLI for Claude.";
const CLAUDE_CODE_SYSTEM_PROMPT_EXPANSION: &str = r#"You are an interactive agent that helps users with software engineering tasks. Use the instructions below and the tools available to you to assist the user.

IMPORTANT: Assist with authorized security testing, defensive security, CTF challenges, and educational contexts. Refuse requests for destructive techniques, DoS attacks, mass targeting, supply chain compromise, or detection evasion for malicious purposes. Dual-use security tools (C2 frameworks, credential testing, exploit development) require clear authorization context: pentesting engagements, CTF competitions, security research, or defensive use cases.
IMPORTANT: You must NEVER generate or guess URLs for the user unless you are confident that the URLs are for helping the user with programming. You may use URLs provided by the user in their messages or local files.

# Tone and style
 - Only use emojis if the user explicitly requests it. Avoid using emojis in all communication unless asked.
 - Your responses should be short and concise.
 - When referencing specific functions or pieces of code include the pattern file_path:line_number to allow the user to easily navigate to the source code location.
 - When referencing GitHub issues or pull requests, use the owner/repo#123 format (e.g. anthropics/claude-code#100) so they render as clickable links.
 - Do not use a colon before tool calls. Your tool calls may not be shown directly in the output, so text like "Let me read the file:" followed by a read tool call should just be "Let me read the file." with a period."#;

/// 仅在 `<system-reminder>` 标签内替换 `Git user:` 行。
/// 不影响 messages、tools 和 `<system-reminder>` 外部的文本，避免破坏 git 操作。
fn scrub_git_user_in_reminders(body: &mut serde_json::Value, replacement_name: &str) {
    let replacement = format!("Git user: {}", replacement_name);
    let scrub = |text: &str| -> String {
        SYSTEM_REMINDER_REGEX
            .replace_all(text, |caps: &regex::Captures| {
                GIT_USER_REGEX
                    .replace_all(&caps[0], replacement.as_str())
                    .to_string()
            })
            .to_string()
    };

    if let Some(system) = body.get_mut("system") {
        match system {
            serde_json::Value::String(s) => {
                *s = scrub(s);
            }
            serde_json::Value::Array(arr) => {
                for item in arr.iter_mut() {
                    if item.get("cache_control").is_some() {
                        continue;
                    }
                    if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                        let new_text = scrub(text);
                        item.as_object_mut()
                            .unwrap()
                            .insert("text".into(), serde_json::Value::String(new_text));
                    }
                }
            }
            _ => {}
        }
    }
}

/// 将 canonical env 的 platform 映射为 X-Stainless-OS 值。
fn stainless_os_from_platform(platform: &str) -> &str {
    match platform {
        "darwin" => "Mac OS X",
        "win32" => "Windows",
        _ => "Linux",
    }
}

fn nth_index(s: &str, c: char, n: usize) -> Option<usize> {
    let mut count = 0;
    for (i, ch) in s.chars().enumerate() {
        if ch == c {
            count += 1;
            if count == n {
                return Some(i);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        CacheControlTtlRewrite, ClientType, DisabledThinkingRewrite, EnvPassthrough,
        MessageCacheControlRewrite, Rewriter, cch_attestation_input, cch_attestation_seed,
        compute_cc_version_suffix, compute_cch_attestation, extract_first_user_message,
        matches_1m_whitelist, ordered_anthropic_headers, strip_beta_token,
    };
    use crate::model::account::{
        Account, AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData,
        CanonicalProcessData, CanonicalPromptEnvData,
    };
    use crate::service::version_profile::{
        DEFAULT_CLAUDE_CODE_BUILD_TIME, DEFAULT_CLAUDE_CODE_VERSION,
        DEFAULT_CLAUDE_CODE_VERSION_BASE, FABLE_FALLBACK_BETA_TOKENS, FABLE_MESSAGE_BETA_TOKENS,
        MCP_CLIENT_CAPABILITIES, MCP_PROTOCOL_VERSION, MESSAGE_BETA_TOKENS,
        STAINLESS_PACKAGE_VERSION, STAINLESS_RUNTIME_VERSION, claude_cli_user_agent,
        claude_code_user_agent,
    };
    use base64::Engine;
    use chrono::Utc;
    use serde_json::json;

    const CTX_1M: &str = "context-1m-2025-08-07";

    fn test_account() -> Account {
        let env = CanonicalEnvData {
            platform: "linux".into(),
            platform_raw: "linux".into(),
            arch: "x64".into(),
            node_version: "v22.15.0".into(),
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
        let prompt = CanonicalPromptEnvData {
            platform: "linux".into(),
            shell: "bash".into(),
            os_version: "Linux 6.5.0-generic".into(),
            working_dir: "/home/user/project".into(),
        };
        let process = CanonicalProcessData {
            constrained_memory: 0,
            rss_range: [1, 2],
            heap_total_range: [1, 2],
            heap_used_range: [1, 2],
            external_range: [1, 2],
            array_buffers_range: [1, 2],
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
            canonical_process: serde_json::to_value(process).unwrap(),
            billing_mode: BillingMode::Rewrite,
            account_uuid: Some("account-uuid".into()),
            organization_uuid: Some("org-uuid".into()),
            subscription_type: Some("max".into()),
            concurrency: 3,
            priority: 50,
            rpm_limit: 0,
            rate_limited_at: None,
            rate_limit_reset_at: None,
            disable_reason: String::new(),
            auto_telemetry: false,
            auto_poll_usage: false,
            allow_1m_models: "opus".into(),
            telemetry_count: 0,
            usage_data: json!({}),
            usage_fetched_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn rewrite_messages_body(
        body: serde_json::Value,
        client_type: ClientType,
    ) -> serde_json::Value {
        rewrite_messages_body_with_modes(
            body,
            client_type,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Off,
        )
    }

    fn rewrite_messages_body_with_ttl(
        body: serde_json::Value,
        client_type: ClientType,
        cache_ttl_rewrite: CacheControlTtlRewrite,
    ) -> serde_json::Value {
        rewrite_messages_body_with_modes(
            body,
            client_type,
            cache_ttl_rewrite,
            MessageCacheControlRewrite::Off,
        )
    }

    fn rewrite_messages_body_with_modes(
        body: serde_json::Value,
        client_type: ClientType,
        cache_ttl_rewrite: CacheControlTtlRewrite,
        message_cache_rewrite: MessageCacheControlRewrite,
    ) -> serde_json::Value {
        let account = test_account();
        let rewriter = Rewriter::new();
        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            client_type,
            EnvPassthrough::default(),
            cache_ttl_rewrite,
            message_cache_rewrite,
        );
        serde_json::from_slice(&out).unwrap()
    }

    fn rewrite_messages_body_with_disabled_thinking(
        body: serde_json::Value,
        disabled_thinking_rewrite: DisabledThinkingRewrite,
    ) -> serde_json::Value {
        let account = test_account();
        let rewriter = Rewriter::new();
        let (out, _completion) = rewriter.rewrite_body_with_stateful_completion(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Off,
            &disabled_thinking_rewrite,
        );
        serde_json::from_slice(&out).unwrap()
    }

    fn rewrite_messages_body_with_rewriter(
        rewriter: &Rewriter,
        body: serde_json::Value,
        client_type: ClientType,
        cache_ttl_rewrite: CacheControlTtlRewrite,
        message_cache_rewrite: MessageCacheControlRewrite,
    ) -> serde_json::Value {
        let account = test_account();
        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            client_type,
            EnvPassthrough::default(),
            cache_ttl_rewrite,
            message_cache_rewrite,
        );
        serde_json::from_slice(&out).unwrap()
    }

    fn rewrite_messages_body_with_stateful_completion(
        rewriter: &Rewriter,
        body: serde_json::Value,
    ) -> (serde_json::Value, Option<super::StatefulCacheCompletion>) {
        let account = test_account();
        let (out, completion) = rewriter.rewrite_body_with_stateful_completion(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
            &DisabledThinkingRewrite::off(),
        );
        (serde_json::from_slice(&out).unwrap(), completion)
    }

    fn message_cache_control_positions(body: &serde_json::Value) -> Vec<(usize, usize)> {
        let Some(messages) = body.get("messages").and_then(|m| m.as_array()) else {
            return Vec::new();
        };

        let mut positions = Vec::new();
        for (message_idx, msg) in messages.iter().enumerate() {
            let Some(content) = msg.get("content").and_then(|c| c.as_array()) else {
                continue;
            };
            for (block_idx, block) in content.iter().enumerate() {
                if block.get("cache_control").is_some() {
                    positions.push((message_idx, block_idx));
                }
            }
        }
        positions
    }

    fn message_cache_fingerprints(body: &serde_json::Value) -> Vec<String> {
        message_cache_control_positions(body)
            .into_iter()
            .filter_map(|position| super::message_block_fingerprint_at(body, position))
            .collect()
    }

    fn message_cache_positions_and_fingerprints(
        body: &serde_json::Value,
    ) -> Vec<((usize, usize), String)> {
        message_cache_control_positions(body)
            .into_iter()
            .filter_map(|position| {
                super::message_block_fingerprint_at(body, position)
                    .map(|fingerprint| (position, fingerprint))
            })
            .collect()
    }

    fn stateful_session_body(session_id: &str, block_count: usize) -> serde_json::Value {
        let messages: Vec<serde_json::Value> = (0..block_count)
            .map(|idx| {
                json!({
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                    "content": [{
                        "type": "text",
                        "text": format!("main-block-{}", idx)
                    }]
                })
            })
            .collect();
        json!({
            "metadata": {
                "user_id": json!({
                    "device_id": "device-1",
                    "account_uuid": "account-uuid",
                    "session_id": session_id
                }).to_string()
            },
            "messages": messages
        })
    }

    fn stateful_session_body_with_prefix(
        session_id: &str,
        block_count: usize,
        prefix: &str,
    ) -> serde_json::Value {
        let messages: Vec<serde_json::Value> = (0..block_count)
            .map(|idx| {
                json!({
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                    "content": [{
                        "type": "text",
                        "text": format!("{}-block-{}", prefix, idx)
                    }]
                })
            })
            .collect();
        json!({
            "metadata": {
                "user_id": json!({
                    "device_id": "device-1",
                    "account_uuid": "account-uuid",
                    "session_id": session_id
                }).to_string()
            },
            "messages": messages
        })
    }

    fn stateful_tool_result_tail_body(
        session_id: &str,
        tool_result_count: usize,
    ) -> serde_json::Value {
        let mut messages = vec![json!({
            "role": "user",
            "content": [{
                "type": "text",
                "text": "stable text anchor"
            }]
        })];
        for idx in 0..tool_result_count {
            messages.push(json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": format!("toolu_{}", idx),
                    "content": format!("tool result {}", idx)
                }]
            }));
        }
        json!({
            "metadata": {
                "user_id": json!({
                    "device_id": "device-1",
                    "account_uuid": "account-uuid",
                    "session_id": session_id
                }).to_string()
            },
            "messages": messages
        })
    }

    #[test]
    fn cache_control_ttl_rewrite_parse_accepts_known_values() {
        assert_eq!(
            CacheControlTtlRewrite::parse("off").unwrap(),
            CacheControlTtlRewrite::Off
        );
        assert_eq!(
            CacheControlTtlRewrite::parse("5m").unwrap(),
            CacheControlTtlRewrite::FiveMinutes
        );
        assert_eq!(
            CacheControlTtlRewrite::parse("1h").unwrap(),
            CacheControlTtlRewrite::OneHour
        );
    }

    #[test]
    fn cache_control_ttl_rewrite_parse_rejects_unknown_value() {
        let err = CacheControlTtlRewrite::parse("30m").unwrap_err();

        assert!(err.to_string().contains("cache_control_ttl_rewrite"));
    }

    #[test]
    fn disabled_thinking_rewrite_updates_matched_model_before_output_body() {
        let out = rewrite_messages_body_with_disabled_thinking(
            json!({
                "model": "claude-fable-5",
                "thinking": {"type": "disabled"},
                "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
            }),
            DisabledThinkingRewrite {
                enabled: true,
                models: vec!["claude-fable-5".into()],
            },
        );

        assert_eq!(out["thinking"]["type"], "adaptive");
    }

    #[test]
    fn disabled_thinking_rewrite_keeps_unmatched_model() {
        let out = rewrite_messages_body_with_disabled_thinking(
            json!({
                "model": "claude-opus-4-8",
                "thinking": {"type": "disabled"},
                "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]
            }),
            DisabledThinkingRewrite {
                enabled: true,
                models: vec!["claude-fable-5".into()],
            },
        );

        assert_eq!(out["thinking"]["type"], "disabled");
    }

    #[test]
    fn disabled_thinking_rewrite_runs_before_cch_and_keeps_cache_breakpoints() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let messages: Vec<serde_json::Value> = (0..60)
            .map(|idx| {
                json!({
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx)
                    }]
                })
            })
            .collect();
        let body = json!({
            "model": "claude-fable-5",
            "thinking": {"type": "disabled"},
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.156.abc; cc_entrypoint=cli; cch=12345;"
            }],
            "messages": messages
        });

        let (out, _completion) = rewriter.rewrite_body_with_stateful_completion(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::FiveMinutes,
            MessageCacheControlRewrite::Auto,
            &DisabledThinkingRewrite {
                enabled: true,
                models: vec!["claude-fable-5".into()],
            },
        );
        let text = String::from_utf8(out.clone()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();

        assert_eq!(parsed["thinking"]["type"], "adaptive");
        assert_eq!(message_cache_control_positions(&parsed).len(), 4);
        assert!(!text.contains("cch=12345"));
        assert!(!text.contains("cch=00000"));

        let actual = super::CCH_VALUE_REGEX
            .find(&text)
            .map(|m| m.as_str().to_string())
            .expect("cch value exists");
        let mut placeholder_body = out;
        let cch_pos = placeholder_body
            .windows(super::CCH_PLACEHOLDER.len())
            .position(|window| window.starts_with(b"cch="))
            .expect("cch value position");
        placeholder_body[cch_pos + 4..cch_pos + 9].copy_from_slice(b"00000");
        let expected = String::from_utf8(compute_cch_attestation(
            placeholder_body,
            DEFAULT_CLAUDE_CODE_VERSION,
        ))
        .unwrap();

        assert!(expected.contains(&actual));
    }

    #[test]
    fn message_cache_control_rewrite_parse_accepts_known_values() {
        assert_eq!(
            MessageCacheControlRewrite::parse("off").unwrap(),
            MessageCacheControlRewrite::Off
        );
        assert_eq!(
            MessageCacheControlRewrite::parse("auto").unwrap(),
            MessageCacheControlRewrite::Auto
        );
        assert_eq!(
            MessageCacheControlRewrite::parse("stable").unwrap(),
            MessageCacheControlRewrite::Auto
        );
        assert_eq!(
            MessageCacheControlRewrite::parse("rolling").unwrap(),
            MessageCacheControlRewrite::Rolling
        );
        assert_eq!(
            MessageCacheControlRewrite::parse("stateful").unwrap(),
            MessageCacheControlRewrite::Stateful
        );
        assert_eq!(
            MessageCacheControlRewrite::parse("sub2api").unwrap(),
            MessageCacheControlRewrite::Sub2api
        );
        assert_eq!(
            MessageCacheControlRewrite::parse("anchored").unwrap(),
            MessageCacheControlRewrite::Auto
        );
    }

    #[test]
    fn message_cache_control_rewrite_parse_rejects_unknown_value() {
        let err = MessageCacheControlRewrite::parse("last").unwrap_err();

        assert!(err.to_string().contains("message_cache_control_rewrite"));
    }

    #[test]
    fn cache_control_ttl_rewrite_off_keeps_existing_ttl_values() {
        let parsed = rewrite_messages_body_with_ttl(
            json!({
                "cache_control": { "type": "ephemeral" },
                "system": [{
                    "type": "text",
                    "text": "sys",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hi",
                        "cache_control": { "type": "ephemeral", "ttl": "5m" }
                    }]
                }],
                "tools": [{
                    "name": "a",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
        );

        assert!(parsed["cache_control"].get("ttl").is_none());
        assert_eq!(parsed["system"][0]["cache_control"]["ttl"], json!("1h"));
        assert_eq!(
            parsed["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("5m")
        );
        assert_eq!(parsed["tools"][0]["cache_control"]["ttl"], json!("1h"));
    }

    #[test]
    fn cache_control_ttl_rewrite_updates_only_existing_ephemeral_breakpoints() {
        let parsed = rewrite_messages_body_with_ttl(
            json!({
                "cache_control": { "type": "ephemeral" },
                "system": [
                    {
                        "type": "text",
                        "text": "cached sys",
                        "cache_control": { "type": "ephemeral", "ttl": "5m" }
                    },
                    {
                        "type": "text",
                        "text": "plain sys"
                    }
                ],
                "messages": [{
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "hi",
                            "cache_control": { "type": "ephemeral" }
                        },
                        {
                            "type": "text",
                            "text": "persistent",
                            "cache_control": { "type": "persistent", "ttl": "5m" }
                        },
                        {
                            "type": "tool_result",
                            "content": [{
                                "type": "text",
                                "text": "nested",
                                "cache_control": { "type": "ephemeral", "ttl": "5m" }
                            }]
                        }
                    ]
                }],
                "tools": [
                    {
                        "name": "a",
                        "input_schema": {},
                        "cache_control": { "type": "ephemeral" }
                    },
                    {
                        "name": "b",
                        "input_schema": {}
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::OneHour,
        );
        let rendered = serde_json::to_string(&parsed).unwrap();

        assert_eq!(rendered.matches("\"cache_control\"").count(), 6);
        assert_eq!(parsed["cache_control"]["ttl"], json!("1h"));
        assert_eq!(parsed["system"][0]["cache_control"]["ttl"], json!("1h"));
        assert!(parsed["system"][1].get("cache_control").is_none());
        assert_eq!(
            parsed["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("1h")
        );
        assert_eq!(
            parsed["messages"][0]["content"][1]["cache_control"]["ttl"],
            json!("5m")
        );
        assert_eq!(
            parsed["messages"][0]["content"][2]["content"][0]["cache_control"]["ttl"],
            json!("5m")
        );
        assert_eq!(parsed["tools"][0]["cache_control"]["ttl"], json!("1h"));
        assert!(parsed["tools"][1].get("cache_control").is_none());
    }

    #[test]
    fn cache_control_ttl_rewrite_can_force_five_minutes() {
        let parsed = rewrite_messages_body_with_ttl(
            json!({
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hi",
                        "cache_control": { "type": "ephemeral", "ttl": "1h" }
                    }]
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::FiveMinutes,
        );

        assert_eq!(
            parsed["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("5m")
        );
    }

    #[test]
    fn message_cache_control_auto_rewrites_cache_breakpoints() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "system": [{
                    "type": "text",
                    "text": "sys",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": [
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "u1",
                            "cache_control": { "type": "ephemeral", "ttl": "1h" }
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "text",
                            "text": "a1",
                            "cache_control": { "type": "ephemeral", "ttl": "1h" }
                        }]
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "u2-old",
                                "cache_control": { "type": "ephemeral", "ttl": "1h" }
                            },
                            {
                                "type": "text",
                                "text": "u2"
                            }
                        ]
                    },
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "thinking",
                                "thinking": "skip me"
                            },
                            {
                                "type": "text",
                                "text": "a2"
                            },
                            "tail-marker"
                        ]
                    }
                ],
                "tools": [{
                    "name": "a",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert!(parsed["system"][0].get("cache_control").is_none());
        assert!(parsed["tools"][0].get("cache_control").is_none());
        assert_eq!(
            parsed["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("1h")
        );
        assert!(
            parsed["messages"][1]["content"][0]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][2]["content"][0]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][2]["content"][1]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][3]["content"][0]
                .get("cache_control")
                .is_none()
        );
        assert_eq!(
            parsed["messages"][3]["content"][1]["cache_control"]["ttl"],
            json!("1h")
        );
        assert_eq!(parsed["messages"][3]["content"][2], json!("tail-marker"));
    }

    #[test]
    fn message_cache_control_auto_keeps_string_content_unchanged() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [{
                    "role": "user",
                    "content": "hello"
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::FiveMinutes,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(parsed["messages"][0]["content"], json!("hello"));
    }

    #[test]
    fn message_cache_control_auto_places_tail_breakpoints_within_lookback() {
        let messages: Vec<serde_json::Value> = (0..45)
            .map(|idx| {
                json!({
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx),
                        "cache_control": { "type": "ephemeral", "ttl": "5m" }
                    }]
                })
            })
            .collect();
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        let positions = message_cache_control_positions(&parsed);

        assert_eq!(positions, vec![(0, 0), (6, 0), (25, 0), (44, 0)]);
        for pair in positions.windows(2) {
            assert!(pair[1].0 - pair[0].0 <= 19);
        }
    }

    #[test]
    fn message_cache_control_auto_takes_over_non_message_breakpoint_slots() {
        let messages: Vec<serde_json::Value> = (0..60)
            .map(|idx| {
                json!({
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx)
                    }]
                })
            })
            .collect();
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "cache_control": { "type": "ephemeral", "ttl": "1h" },
                "system": [{
                    "type": "text",
                    "text": "sys",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "tools": [{
                    "name": "a",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(2, 0), (21, 0), (40, 0), (59, 0)]
        );
        assert!(parsed.get("cache_control").is_none());
        assert!(parsed["system"][0].get("cache_control").is_none());
        assert!(parsed["tools"][0].get("cache_control").is_none());
    }

    #[test]
    fn message_cache_control_auto_clears_full_non_message_breakpoint_slots() {
        let messages: Vec<serde_json::Value> = (0..60)
            .map(|idx| {
                json!({
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx),
                        "cache_control": { "type": "ephemeral", "ttl": "5m" }
                    }]
                })
            })
            .collect();
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "cache_control": { "type": "ephemeral", "ttl": "1h" },
                "system": [
                    {
                        "type": "text",
                        "text": "sys-1",
                        "cache_control": { "type": "ephemeral", "ttl": "1h" }
                    },
                    {
                        "type": "text",
                        "text": "sys-2",
                        "cache_control": { "type": "ephemeral", "ttl": "1h" }
                    },
                    {
                        "type": "text",
                        "text": "sys-3",
                        "cache_control": { "type": "ephemeral", "ttl": "1h" }
                    }
                ],
                "tools": [{
                    "name": "a",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(2, 0), (21, 0), (40, 0), (59, 0)]
        );
        assert!(parsed.get("cache_control").is_none());
        assert!(parsed["system"][0].get("cache_control").is_none());
        assert!(parsed["system"][1].get("cache_control").is_none());
        assert!(parsed["system"][2].get("cache_control").is_none());
        assert!(parsed["tools"][0].get("cache_control").is_none());
    }

    #[test]
    fn message_cache_control_auto_root_cache_control_does_not_skip_short_tail() {
        let messages: Vec<serde_json::Value> = (0..6)
            .map(|idx| {
                json!({
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx)
                    }]
                })
            })
            .collect();
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "cache_control": { "type": "ephemeral", "ttl": "1h" },
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (5, 0)]
        );
        assert!(parsed.get("cache_control").is_none());
    }

    #[test]
    fn message_cache_control_auto_marks_claude_code_auto_injected_boundary_when_slot_remains() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [{
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "<system-reminder>The following skills are available:\n<available-skills>\n- test-skill\n</available-skills></system-reminder>"
                        },
                        {
                            "type": "text",
                            "text": "<system-reminder>Contents of /repo/CLAUDE.md (project instructions):\n# Project</system-reminder>"
                        },
                        {
                            "type": "text",
                            "text": "real user prompt"
                        }
                    ]
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (0, 1), (0, 2)]
        );
    }

    #[test]
    fn message_cache_control_auto_stabilizes_tools_order_by_name() {
        let body = |tools: serde_json::Value| {
            json!({
                "tools": tools,
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hello"
                    }]
                }]
            })
        };

        let first = rewrite_messages_body_with_modes(
            body(json!([
                { "name": "Write", "description": "w", "input_schema": { "type": "object" } },
                { "name": "Bash", "description": "b", "input_schema": { "type": "object" } },
                { "name": "Read", "description": "r", "input_schema": { "type": "object" } }
            ])),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );
        let second = rewrite_messages_body_with_modes(
            body(json!([
                { "name": "Read", "description": "r", "input_schema": { "type": "object" } },
                { "name": "Write", "description": "w", "input_schema": { "type": "object" } },
                { "name": "Bash", "description": "b", "input_schema": { "type": "object" } }
            ])),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(first["tools"], second["tools"]);
        assert_eq!(first["tools"][0]["name"], json!("Bash"));
        assert_eq!(first["tools"][1]["name"], json!("Read"));
        assert_eq!(first["tools"][2]["name"], json!("Write"));
    }

    #[test]
    fn message_cache_control_auto_stabilizes_system_skills_and_deferred_tools_lists() {
        let body = |skills: &str, deferred: &str| {
            json!({
                "system": [
                    {
                        "type": "text",
                        "text": skills
                    },
                    {
                        "type": "text",
                        "text": deferred
                    }
                ],
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hello"
                    }]
                }]
            })
        };
        let skills_unsorted =
            "User-invocable skills:\n\n- zeta: z\n- alpha: a\n- middle: m\n</system-reminder>";
        let skills_sorted =
            "User-invocable skills:\n\n- alpha: a\n- middle: m\n- zeta: z\n</system-reminder>";
        let deferred_unsorted = "<system-reminder>\nThe following deferred tools are now available:\nWrite\nBash\nRead\n</system-reminder>";
        let deferred_sorted = "<system-reminder>\nThe following deferred tools are now available:\nBash\nRead\nWrite\n</system-reminder>";

        let first = rewrite_messages_body_with_modes(
            body(skills_unsorted, deferred_unsorted),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );
        let second = rewrite_messages_body_with_modes(
            body(skills_sorted, deferred_sorted),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(first["system"], second["system"]);
        assert_eq!(first["system"][0]["text"], json!(skills_sorted));
        assert_eq!(first["system"][1]["text"], json!(deferred_sorted));
    }

    #[test]
    fn message_cache_control_auto_keeps_multiline_skill_entries_intact() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "system": [{
                    "type": "text",
                    "text": "User-invocable skills:\n\n- zeta: z\n  second line\n- alpha: a\n  more detail\n</system-reminder>"
                }],
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hello"
                    }]
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            parsed["system"][0]["text"],
            json!(
                "User-invocable skills:\n\n- alpha: a\n  more detail\n- zeta: z\n  second line\n</system-reminder>"
            )
        );
    }

    #[test]
    fn message_cache_control_auto_stabilizes_first_user_auto_injected_lists() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [{
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "<system-reminder>The following skills are available:\n\n- zeta: z\n- alpha: a\n</system-reminder>"
                        },
                        {
                            "type": "text",
                            "text": "<system-reminder>\nThe following deferred tools are now available:\nWrite\nBash\n</system-reminder>"
                        },
                        {
                            "type": "text",
                            "text": "real user prompt"
                        }
                    ]
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            parsed["messages"][0]["content"][0]["text"],
            json!(
                "<system-reminder>The following skills are available:\n\n- alpha: a\n- zeta: z\n</system-reminder>"
            )
        );
        assert_eq!(
            parsed["messages"][0]["content"][1]["text"],
            json!(
                "<system-reminder>\nThe following deferred tools are now available:\nBash\nWrite\n</system-reminder>"
            )
        );
        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (0, 1), (0, 2)]
        );
    }

    #[test]
    fn message_cache_control_stable_alias_uses_auto_sorting() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "tools": [
                    { "name": "Write", "input_schema": {} },
                    { "name": "Read", "input_schema": {} }
                ],
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hello"
                    }]
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::parse("stable").unwrap(),
        );

        assert_eq!(parsed["tools"][0]["name"], json!("Read"));
        assert_eq!(parsed["tools"][1]["name"], json!("Write"));
    }

    #[test]
    fn message_cache_control_auto_does_not_steal_tail_slots_for_auto_boundary() {
        let mut messages: Vec<serde_json::Value> = (0..60)
            .map(|idx| {
                json!({
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx)
                    }]
                })
            })
            .collect();
        messages[0] = json!({
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "<deferred-tools>\n<tool>Read</tool>\n</deferred-tools>"
                },
                {
                    "type": "text",
                    "text": "block-0"
                }
            ]
        });

        let parsed = rewrite_messages_body_with_modes(
            json!({
                "system": [{
                    "type": "text",
                    "text": "sys",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "tools": [{
                    "name": "a",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(2, 0), (21, 0), (40, 0), (59, 0)]
        );
        assert!(
            parsed["messages"][0]["content"][0]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_auto_skips_uncacheable_tail_blocks() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "text",
                            "text": "cacheable"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "thinking",
                            "thinking": "skip"
                        }]
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(message_cache_control_positions(&parsed), vec![(0, 0)]);
        assert!(
            parsed["messages"][1]["content"][0]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_auto_avoids_assistant_tool_use_breakpoints() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "system": [{
                    "type": "text",
                    "text": "sys",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "tools": [{
                    "name": "Read",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": [
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "u0"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "text",
                                "text": "a1"
                            },
                            {
                                "type": "tool_use",
                                "id": "toolu_1",
                                "name": "Read",
                                "input": { "file_path": "/tmp/a" }
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": "toolu_1",
                            "content": "result"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "thinking",
                                "thinking": "skip"
                            },
                            {
                                "type": "tool_use",
                                "id": "toolu_2",
                                "name": "Bash",
                                "input": { "command": "pwd" }
                            }
                        ]
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (2, 0)]
        );
        assert!(
            parsed["messages"][1]["content"][1]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][2]["content"][0]
                .get("cache_control")
                .is_some()
        );
        assert!(
            parsed["messages"][3]["content"][1]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_auto_marks_latest_tool_result_tail() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "initial prompt"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "text",
                            "text": "older assistant boundary"
                        }]
                    },
                    {
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": "toolu_big",
                            "content": "large result that must become the newest stable tail"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "tool_use",
                            "id": "toolu_tail",
                            "name": "Read",
                            "input": { "file_path": "/tmp/tail" }
                        }]
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (2, 0)]
        );
        assert!(
            parsed["messages"][1]["content"][0]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][2]["content"][0]
                .get("cache_control")
                .is_some()
        );
        assert!(
            parsed["messages"][3]["content"][0]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_rolling_avoids_tool_use_but_allows_tool_result_breakpoints() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "system": [{
                    "type": "text",
                    "text": "sys",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "tools": [{
                    "name": "Read",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": [
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "u0"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "text",
                                "text": "a1"
                            },
                            {
                                "type": "tool_use",
                                "id": "toolu_1",
                                "name": "Read",
                                "input": { "file_path": "/tmp/a" }
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": "toolu_1",
                            "content": "result"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "thinking",
                                "thinking": "skip"
                            },
                            {
                                "type": "tool_use",
                                "id": "toolu_2",
                                "name": "Bash",
                                "input": { "command": "pwd" }
                            }
                        ]
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Rolling,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (2, 0)]
        );
        assert!(
            parsed["messages"][1]["content"][1]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][2]["content"][0]
                .get("cache_control")
                .is_some()
        );
        assert!(
            parsed["messages"][3]["content"][1]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_auto_counts_blocks_without_using_tool_use_breakpoints() {
        let mut blocks = Vec::new();
        blocks.push(json!({
            "type": "text",
            "text": "anchor"
        }));
        for idx in 0..18 {
            blocks.push(json!({
                "type": "thinking",
                "thinking": format!("thinking-{}", idx)
            }));
        }
        blocks.push(json!({
            "type": "tool_use",
            "id": "toolu_tail",
            "name": "Read",
            "input": { "file_path": "/tmp/tail" }
        }));
        blocks.push(json!({
            "type": "text",
            "text": "tail"
        }));

        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [{
                    "role": "assistant",
                    "content": blocks
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (0, 20)]
        );
        assert!(
            parsed["messages"][0]["content"][18]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][0]["content"][19]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_rolling_counts_blocks_without_using_tool_use_breakpoints() {
        let mut blocks = Vec::new();
        blocks.push(json!({
            "type": "text",
            "text": "anchor"
        }));
        for idx in 0..18 {
            blocks.push(json!({
                "type": "thinking",
                "thinking": format!("thinking-{}", idx)
            }));
        }
        blocks.push(json!({
            "type": "tool_use",
            "id": "toolu_tail",
            "name": "Read",
            "input": { "file_path": "/tmp/tail" }
        }));
        blocks.push(json!({
            "type": "text",
            "text": "tail"
        }));

        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [{
                    "role": "assistant",
                    "content": blocks
                }]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Rolling,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (0, 20)]
        );
        assert!(
            parsed["messages"][0]["content"][18]
                .get("cache_control")
                .is_none()
        );
        assert!(
            parsed["messages"][0]["content"][19]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_rolling_prefers_text_before_tool_result_in_window() {
        let mut messages = Vec::new();
        messages.push(json!({
            "role": "user",
            "content": [{
                "type": "text",
                "text": "anchor"
            }]
        }));
        for idx in 1..6 {
            messages.push(json!({
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": format!("toolu_before_{}", idx),
                    "name": "Read",
                    "input": { "file_path": format!("/tmp/before-{}", idx) }
                }]
            }));
        }
        messages.push(json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "toolu_window",
                "content": "window result"
            }]
        }));
        for idx in 7..12 {
            messages.push(json!({
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": format!("toolu_gap_{}", idx),
                    "name": "Read",
                    "input": { "file_path": format!("/tmp/gap-{}", idx) }
                }]
            }));
        }
        messages.push(json!({
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "stable window text"
            }]
        }));
        for idx in 13..25 {
            messages.push(json!({
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": format!("toolu_after_{}", idx),
                    "name": "Read",
                    "input": { "file_path": format!("/tmp/after-{}", idx) }
                }]
            }));
        }
        messages.push(json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "toolu_tail",
                "content": "tail result"
            }]
        }));

        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Rolling,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (12, 0), (25, 0)]
        );
        assert!(
            parsed["messages"][6]["content"][0]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_rolling_falls_back_to_tool_result_when_window_has_no_text() {
        let mut messages = Vec::new();
        messages.push(json!({
            "role": "user",
            "content": [{
                "type": "text",
                "text": "anchor"
            }]
        }));
        for idx in 1..6 {
            messages.push(json!({
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": format!("toolu_before_{}", idx),
                    "name": "Read",
                    "input": { "file_path": format!("/tmp/before-{}", idx) }
                }]
            }));
        }
        messages.push(json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "toolu_window",
                "content": "window fallback"
            }]
        }));
        for idx in 7..25 {
            messages.push(json!({
                "role": "assistant",
                "content": [{
                    "type": "tool_use",
                    "id": format!("toolu_gap_{}", idx),
                    "name": "Read",
                    "input": { "file_path": format!("/tmp/gap-{}", idx) }
                }]
            }));
        }
        messages.push(json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "toolu_tail",
                "content": "tail result"
            }]
        }));

        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Rolling,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (6, 0), (25, 0)]
        );
        for idx in 7..25 {
            assert!(
                parsed["messages"][idx]["content"][0]
                    .get("cache_control")
                    .is_none()
            );
        }
    }

    #[test]
    fn message_cache_control_auto_stabilizes_adjacent_tool_use_and_results() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "tool_use",
                                "id": "toolu_b",
                                "name": "Bash",
                                "input": { "command": "pwd" }
                            },
                            {
                                "type": "tool_use",
                                "id": "toolu_a",
                                "name": "Read",
                                "input": { "file_path": "/tmp/a" }
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": "toolu_b",
                                "content": "b"
                            },
                            {
                                "type": "tool_result",
                                "tool_use_id": "toolu_a",
                                "content": "a"
                            }
                        ]
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(parsed["messages"][0]["content"][0]["id"], json!("toolu_a"));
        assert_eq!(parsed["messages"][0]["content"][1]["id"], json!("toolu_b"));
        assert_eq!(
            parsed["messages"][1]["content"][0]["tool_use_id"],
            json!("toolu_a")
        );
        assert_eq!(
            parsed["messages"][1]["content"][1]["tool_use_id"],
            json!("toolu_b")
        );
        assert_eq!(message_cache_control_positions(&parsed), vec![(1, 1)]);
        assert!(
            parsed["messages"][0]["content"][0]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_rolling_stabilizes_adjacent_tool_use_and_results() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [
                    {
                        "role": "assistant",
                        "content": [
                            {
                                "type": "tool_use",
                                "id": "toolu_b",
                                "name": "Bash",
                                "input": { "command": "pwd" }
                            },
                            {
                                "type": "tool_use",
                                "id": "toolu_a",
                                "name": "Read",
                                "input": { "file_path": "/tmp/a" }
                            }
                        ]
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": "toolu_b",
                                "content": "b"
                            },
                            {
                                "type": "tool_result",
                                "tool_use_id": "toolu_a",
                                "content": "a"
                            }
                        ]
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Rolling,
        );

        assert_eq!(parsed["messages"][0]["content"][0]["id"], json!("toolu_a"));
        assert_eq!(parsed["messages"][0]["content"][1]["id"], json!("toolu_b"));
        assert_eq!(
            parsed["messages"][1]["content"][0]["tool_use_id"],
            json!("toolu_a")
        );
        assert_eq!(
            parsed["messages"][1]["content"][1]["tool_use_id"],
            json!("toolu_b")
        );
        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(1, 0), (1, 1)]
        );
        assert!(
            parsed["messages"][0]["content"][0]
                .get("cache_control")
                .is_none()
        );
    }

    #[test]
    fn message_cache_control_auto_ttl_rewrite_updates_created_breakpoints() {
        let messages: Vec<serde_json::Value> = (0..22)
            .map(|idx| {
                json!({
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx)
                    }]
                })
            })
            .collect();
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": messages
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::FiveMinutes,
            MessageCacheControlRewrite::Auto,
        );

        for (message_idx, block_idx) in message_cache_control_positions(&parsed) {
            assert_eq!(
                parsed["messages"][message_idx]["content"][block_idx]["cache_control"]["ttl"],
                json!("5m")
            );
        }
    }

    #[test]
    fn message_cache_control_stateful_reuses_previous_normal_anchors() {
        let rewriter = Rewriter::new();
        let first = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-reuse", 60),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let first_fingerprints = message_cache_fingerprints(&first);

        let second = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-reuse", 63),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let second_fingerprints = message_cache_fingerprints(&second);
        let reused = second_fingerprints
            .iter()
            .filter(|fingerprint| first_fingerprints.contains(fingerprint))
            .count();

        assert!(reused >= 2);
        assert_eq!(message_cache_control_positions(&second).len(), 4);
    }

    #[test]
    fn message_cache_control_stateful_completion_is_delayed_until_upstream_finishes() {
        let key = "1:session-delayed".to_string();
        let rewriter = Rewriter::new();
        let (first, first_completion) = rewrite_messages_body_with_stateful_completion(
            &rewriter,
            stateful_session_body("session-delayed", 60),
        );
        let first_completion = first_completion.expect("first request should create completion");
        let first_fingerprints = message_cache_fingerprints(&first);

        assert!(rewriter.stateful_cache.lock().unwrap().get(&key).is_none());

        let (_second_before_completion, _pending_completion) =
            rewrite_messages_body_with_stateful_completion(
                &rewriter,
                stateful_session_body("session-delayed", 63),
            );
        assert!(rewriter.stateful_cache.lock().unwrap().get(&key).is_none());

        rewriter.complete_stateful_cache(Some(first_completion));
        let snapshot = rewriter
            .stateful_cache
            .lock()
            .unwrap()
            .get(&key)
            .expect("snapshot after completion");
        assert_eq!(snapshot.normal_profile.block_count, 60);

        let (third, third_completion) = rewrite_messages_body_with_stateful_completion(
            &rewriter,
            stateful_session_body("session-delayed", 63),
        );
        let third_fingerprints = message_cache_fingerprints(&third);
        let reused = third_fingerprints
            .iter()
            .filter(|fingerprint| first_fingerprints.contains(fingerprint))
            .count();

        assert!(reused >= 2);
        rewriter.complete_stateful_cache(third_completion);
    }

    #[test]
    fn message_cache_control_stateful_does_not_persist_tool_result_anchors() {
        let key = "1:session-tool-result-durable".to_string();
        let rewriter = Rewriter::new();
        let (parsed, completion) = rewrite_messages_body_with_stateful_completion(
            &rewriter,
            stateful_tool_result_tail_body("session-tool-result-durable", 25),
        );

        assert!(
            message_cache_control_positions(&parsed)
                .iter()
                .any(|(message_idx, block_idx)| {
                    parsed["messages"][*message_idx]["content"][*block_idx]["type"]
                        == json!("tool_result")
                })
        );

        rewriter.complete_stateful_cache(completion);
        let snapshot = rewriter
            .stateful_cache
            .lock()
            .unwrap()
            .get(&key)
            .expect("snapshot");

        assert!(!snapshot.normal_anchors.is_empty());
        assert!(
            snapshot
                .normal_anchors
                .iter()
                .all(|anchor| anchor.block_type == "text")
        );
    }

    #[test]
    fn message_cache_control_stateful_rejects_completion_when_usage_shows_rebuild() {
        let key = "1:session-usage-feedback".to_string();
        let rewriter = Rewriter::new();
        let (_first, first_completion) = rewrite_messages_body_with_stateful_completion(
            &rewriter,
            stateful_session_body("session-usage-feedback", 60),
        );
        rewriter.complete_stateful_cache_with_usage(
            first_completion,
            Some(super::StatefulCacheUsage {
                cache_read_input_tokens: 70_000,
                cache_creation_input_tokens: 6_000,
                ..Default::default()
            }),
        );
        let first_snapshot = rewriter
            .stateful_cache
            .lock()
            .unwrap()
            .get(&key)
            .expect("first snapshot");
        assert_eq!(first_snapshot.confirmed_cache_read_tokens, Some(70_000));

        let (_second, second_completion) = rewrite_messages_body_with_stateful_completion(
            &rewriter,
            stateful_session_body("session-usage-feedback", 79),
        );
        rewriter.complete_stateful_cache_with_usage(
            second_completion,
            Some(super::StatefulCacheUsage {
                cache_read_input_tokens: 70_000,
                cache_creation_input_tokens: 120_000,
                ..Default::default()
            }),
        );
        let rejected_snapshot = rewriter
            .stateful_cache
            .lock()
            .unwrap()
            .get(&key)
            .expect("rejected snapshot");
        assert_eq!(rejected_snapshot.normal_profile.block_count, 60);
        assert_eq!(rejected_snapshot.confirmed_cache_read_tokens, Some(70_000));

        let (_third, third_completion) = rewrite_messages_body_with_stateful_completion(
            &rewriter,
            stateful_session_body("session-usage-feedback", 79),
        );
        rewriter.complete_stateful_cache_with_usage(
            third_completion,
            Some(super::StatefulCacheUsage {
                cache_read_input_tokens: 90_000,
                cache_creation_input_tokens: 120_000,
                ..Default::default()
            }),
        );
        let accepted_snapshot = rewriter
            .stateful_cache
            .lock()
            .unwrap()
            .get(&key)
            .expect("accepted snapshot");

        assert_eq!(accepted_snapshot.normal_profile.block_count, 79);
        assert_eq!(accepted_snapshot.confirmed_cache_read_tokens, Some(90_000));

        let (_fourth, fourth_completion) = rewrite_messages_body_with_stateful_completion(
            &rewriter,
            stateful_session_body("session-usage-feedback", 98),
        );
        rewriter.complete_stateful_cache(fourth_completion);
        let no_usage_snapshot = rewriter
            .stateful_cache
            .lock()
            .unwrap()
            .get(&key)
            .expect("no usage snapshot");

        assert_eq!(no_usage_snapshot.normal_profile.block_count, 98);
        assert_eq!(no_usage_snapshot.confirmed_cache_read_tokens, Some(90_000));
    }

    #[test]
    fn message_cache_control_stateful_keeps_full_anchor_set_before_tail_rollover() {
        let rewriter = Rewriter::new();
        let first = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-sticky", 48),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let first_positions = message_cache_positions_and_fingerprints(&first);

        let second = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-sticky", 48),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let second_positions = message_cache_positions_and_fingerprints(&second);

        let third = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-sticky", 66),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let third_positions = message_cache_positions_and_fingerprints(&third);

        assert_eq!(first_positions.len(), 4);
        assert_eq!(second_positions, first_positions);
        assert_eq!(third_positions, first_positions);
    }

    #[test]
    fn message_cache_control_stateful_rolls_one_tail_anchor_after_lookback() {
        let rewriter = Rewriter::new();
        let first = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-rollover", 48),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let first_positions = message_cache_positions_and_fingerprints(&first);

        let second = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-rollover", 67),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let second_positions = message_cache_positions_and_fingerprints(&second);

        let first_prefix = first_positions.iter().take(3).cloned().collect::<Vec<_>>();
        let second_prefix = second_positions.iter().take(3).cloned().collect::<Vec<_>>();

        assert_eq!(first_positions.len(), 4);
        assert_eq!(second_positions.len(), 4);
        assert_eq!(second_prefix, first_prefix);
        assert_ne!(second_positions[3], first_positions[3]);
    }

    #[test]
    fn message_cache_control_stateful_does_not_insert_auto_before_reused_anchor() {
        let rewriter = Rewriter::new();
        let first = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-wire-prefix", 22),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let first_positions = message_cache_control_positions(&first);
        let reused_anchor = *first_positions.iter().max().expect("latest anchor");
        let reused_fingerprint =
            super::message_block_fingerprint_at(&first, reused_anchor).expect("fingerprint");
        let first_wire_prefix =
            super::message_wire_prefix_hash_at(&first, reused_anchor).expect("wire prefix");

        let second = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-wire-prefix", 36),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let second_positions = message_cache_control_positions(&second);
        let second_reused_anchor = second_positions
            .iter()
            .copied()
            .find(|position| {
                super::message_block_fingerprint_at(&second, *position).as_deref()
                    == Some(reused_fingerprint.as_str())
            })
            .expect("reused anchor");
        let second_wire_prefix =
            super::message_wire_prefix_hash_at(&second, second_reused_anchor).expect("wire prefix");
        let new_breakpoints_before_reused_anchor = second_positions
            .iter()
            .filter(|position| {
                **position < second_reused_anchor && !first_positions.contains(position)
            })
            .collect::<Vec<_>>();

        assert_eq!(first_positions, vec![(0, 0), (2, 0), (21, 0)]);
        assert_eq!(second_reused_anchor, reused_anchor);
        assert_eq!(first_wire_prefix, second_wire_prefix);
        assert!(new_breakpoints_before_reused_anchor.is_empty());
        assert_eq!(second_positions, vec![(0, 0), (2, 0), (21, 0), (35, 0)]);
    }

    #[test]
    fn message_cache_control_stateful_bootstrap_promotes_past_tiny_first_request() {
        let rewriter = Rewriter::new();
        let first = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-bootstrap", 1),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        assert_eq!(message_cache_control_positions(&first), vec![(0, 0)]);
        assert!(
            rewriter
                .stateful_cache
                .lock()
                .unwrap()
                .get("1:session-bootstrap")
                .is_none()
        );

        for count in [2, 12] {
            let parsed = rewrite_messages_body_with_rewriter(
                &rewriter,
                stateful_session_body("session-bootstrap", count),
                ClientType::ClaudeCode,
                CacheControlTtlRewrite::Off,
                MessageCacheControlRewrite::Stateful,
            );
            assert!(!message_cache_control_positions(&parsed).is_empty());
            assert!(
                rewriter
                    .stateful_cache
                    .lock()
                    .unwrap()
                    .get("1:session-bootstrap")
                    .is_none()
            );
        }

        let bootstrap = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-bootstrap", 22),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        assert!(!message_cache_control_positions(&bootstrap).is_empty());

        let snapshot = rewriter
            .stateful_cache
            .lock()
            .unwrap()
            .get("1:session-bootstrap")
            .expect("snapshot");
        assert_eq!(snapshot.normal_profile.block_count, 22);

        let mature = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-bootstrap", 36),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let mature_fingerprints = message_cache_fingerprints(&mature);
        let reused = mature_fingerprints
            .iter()
            .filter(|fingerprint| {
                snapshot
                    .normal_anchors
                    .iter()
                    .any(|anchor| &anchor.fingerprint == *fingerprint)
            })
            .count();

        assert!(reused >= 1);
    }

    #[test]
    fn message_cache_control_stateful_spike_does_not_pollute_normal_anchors() {
        let rewriter = Rewriter::new();
        let first = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-spike", 76),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let first_fingerprints = message_cache_fingerprints(&first);

        let spike = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-spike", 567),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        assert_eq!(message_cache_control_positions(&spike).len(), 4);

        let recovered = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-spike", 78),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let recovered_fingerprints = message_cache_fingerprints(&recovered);
        let reused = recovered_fingerprints
            .iter()
            .filter(|fingerprint| first_fingerprints.contains(fingerprint))
            .count();

        assert!(reused >= 2);
        assert_eq!(message_cache_control_positions(&recovered).len(), 4);
    }

    #[test]
    fn message_cache_control_stateful_parallel_sibling_without_anchor_does_not_overwrite() {
        let rewriter = Rewriter::new();
        let normal = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-parallel", 80),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let normal_fingerprints = message_cache_fingerprints(&normal);

        let divergent = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-parallel", 30),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        assert!(!message_cache_control_positions(&divergent).is_empty());

        let resumed = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body("session-parallel", 82),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let resumed_fingerprints = message_cache_fingerprints(&resumed);
        let reused = resumed_fingerprints
            .iter()
            .filter(|fingerprint| normal_fingerprints.contains(fingerprint))
            .count();

        assert!(reused >= 2);
    }

    #[test]
    fn message_cache_control_stateful_miss_does_not_replace_existing_normal_anchors() {
        let rewriter = Rewriter::new();
        let normal = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body_with_prefix("session-miss", 80, "normal"),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let normal_fingerprints = message_cache_fingerprints(&normal);

        let miss = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body_with_prefix("session-miss", 80, "different"),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        assert!(!message_cache_control_positions(&miss).is_empty());

        let resumed = rewrite_messages_body_with_rewriter(
            &rewriter,
            stateful_session_body_with_prefix("session-miss", 82, "normal"),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );
        let resumed_fingerprints = message_cache_fingerprints(&resumed);
        let reused = resumed_fingerprints
            .iter()
            .filter(|fingerprint| normal_fingerprints.contains(fingerprint))
            .count();

        assert!(reused >= 2);
    }

    #[test]
    fn message_cache_control_stateful_stale_generation_without_shared_anchor_is_rejected() {
        let key = "1:session-stale".to_string();
        let first_body = stateful_session_body_with_prefix("session-stale", 80, "first");
        let second_body = stateful_session_body_with_prefix("session-stale", 82, "second");
        let stale_body = stateful_session_body_with_prefix("session-stale", 81, "stale");
        let first_profile = super::compute_stateful_request_profile(&first_body);
        let second_profile = super::compute_stateful_request_profile(&second_body);
        let stale_profile = super::compute_stateful_request_profile(&stale_body);
        let first_anchors = super::select_auto_message_cache_control_positions(&first_body)
            .selected
            .iter()
            .filter_map(|position| super::durable_stateful_anchor_record_at(&first_body, *position))
            .collect::<Vec<_>>();
        let second_anchors = super::select_auto_message_cache_control_positions(&second_body)
            .selected
            .iter()
            .filter_map(|position| {
                super::durable_stateful_anchor_record_at(&second_body, *position)
            })
            .collect::<Vec<_>>();
        let stale_anchors = super::select_auto_message_cache_control_positions(&stale_body)
            .selected
            .iter()
            .filter_map(|position| super::durable_stateful_anchor_record_at(&stale_body, *position))
            .collect::<Vec<_>>();
        let mut store = super::StatefulCacheStore::default();

        store.promote_if_current(
            key.clone(),
            first_profile,
            first_anchors.clone(),
            None,
            None,
            false,
        );
        store.promote_if_current(
            key.clone(),
            second_profile,
            second_anchors,
            Some(1),
            None,
            true,
        );
        store.promote_if_current(
            key.clone(),
            stale_profile,
            stale_anchors,
            Some(1),
            None,
            false,
        );

        let snapshot = store.get(&key).expect("snapshot");
        assert_eq!(snapshot.normal_profile.block_count, 82);
        assert!(
            super::anchors_share_fingerprint(&snapshot.normal_anchors, &first_anchors) == false
        );
    }

    #[test]
    fn message_cache_control_stateful_cold_parallel_completion_without_shared_anchor_is_rejected() {
        let key = "1:session-cold-parallel".to_string();
        let first_body = stateful_session_body_with_prefix("session-cold-parallel", 80, "first");
        let parallel_body =
            stateful_session_body_with_prefix("session-cold-parallel", 82, "parallel");
        let first_profile = super::compute_stateful_request_profile(&first_body);
        let parallel_profile = super::compute_stateful_request_profile(&parallel_body);
        let first_anchors = super::select_auto_message_cache_control_positions(&first_body)
            .selected
            .iter()
            .filter_map(|position| super::durable_stateful_anchor_record_at(&first_body, *position))
            .collect::<Vec<_>>();
        let parallel_anchors = super::select_auto_message_cache_control_positions(&parallel_body)
            .selected
            .iter()
            .filter_map(|position| {
                super::durable_stateful_anchor_record_at(&parallel_body, *position)
            })
            .collect::<Vec<_>>();
        let mut store = super::StatefulCacheStore::default();

        store.promote_if_current(
            key.clone(),
            first_profile,
            first_anchors.clone(),
            None,
            None,
            false,
        );
        store.promote_if_current(
            key.clone(),
            parallel_profile,
            parallel_anchors,
            None,
            None,
            false,
        );

        let snapshot = store.get(&key).expect("snapshot");
        assert_eq!(snapshot.normal_profile.block_count, 80);
        assert!(super::anchors_share_fingerprint(
            &snapshot.normal_anchors,
            &first_anchors
        ));
    }

    #[test]
    fn message_cache_control_stateful_appended_neighbor_keeps_anchor_match() {
        let first = json!({
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "same" }
                ]
            }]
        });
        let second = json!({
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "same" },
                    { "type": "text", "text": "new-neighbor" }
                ]
            }]
        });
        let anchor = super::anchor_record_at(&first, (0, 0)).expect("anchor");
        let positions = super::find_stateful_anchor_positions(&second, &[anchor]);

        assert_eq!(positions, vec![(0, 0)]);
    }

    #[test]
    fn message_cache_control_stateful_prefix_mismatch_rejects_anchor_match() {
        let first = json!({
            "system": [{
                "type": "text",
                "text": "old prefix"
            }],
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "same block" }
                ]
            }]
        });
        let second = json!({
            "system": [{
                "type": "text",
                "text": "new prefix"
            }],
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "same block" }
                ]
            }]
        });
        let anchor = super::anchor_record_at(&first, (0, 0)).expect("anchor");
        let positions = super::find_stateful_anchor_positions(&second, &[anchor]);

        assert!(positions.is_empty());
    }

    #[test]
    fn message_cache_control_stateful_diagnostics_use_hashes_without_prompt_text() {
        let body = json!({
            "system": [{
                "type": "text",
                "text": "secret-system-text"
            }],
            "tools": [{
                "name": "Read",
                "description": "secret-tool-text"
            }],
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "secret-user-text" },
                    { "type": "text", "text": "secret-next-text" }
                ]
            }]
        });
        let outcome = super::StatefulCacheBreakpointSelection {
            selected: vec![(0, 0)],
            available_slots: 4,
            message_content_blocks: 2,
            normal_block_count: None,
            reused_count: 0,
            request_class: super::StatefulRequestClass::Unknown,
            promotion: super::StatefulPromotion::ColdStart,
            should_promote: true,
            snapshot_generation: None,
            skip_reason: None,
        };

        let selected = super::stateful_selected_cache_diagnostics(&body, &outcome, None);
        let prefix = super::cache_prefix_diagnostics(&body);
        let rendered = format!("{selected:?} {prefix:?}");

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].source, "auto");
        assert!(!rendered.contains("secret-system-text"));
        assert!(!rendered.contains("secret-tool-text"));
        assert!(!rendered.contains("secret-user-text"));
        assert!(!rendered.contains("secret-next-text"));
        assert_eq!(selected[0].block_hash.len(), 12);
        assert_eq!(selected[0].prefix_hash.len(), 12);
        assert_eq!(selected[0].wire_prefix_hash.len(), 12);
        assert_eq!(prefix.root_hash.len(), 12);
        assert_eq!(prefix.system_hash.len(), 12);
        assert_eq!(prefix.tools_hash.len(), 12);
        assert_eq!(prefix.messages_hash.len(), 12);
    }

    #[test]
    fn message_cache_control_stateful_prefix_hash_changes_when_prefix_changes() {
        let first = json!({
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "same" }
                ]
            }]
        });
        let second = json!({
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "same" },
                    { "type": "text", "text": "next" }
                ]
            }]
        });

        assert_eq!(
            super::message_block_fingerprint_at(&first, (0, 0)),
            super::message_block_fingerprint_at(&second, (0, 0))
        );
        assert_eq!(
            super::message_prefix_hash_at(&first, (0, 0)),
            super::message_prefix_hash_at(&second, (0, 0))
        );
        assert_ne!(
            super::message_prefix_hash_at(&first, (0, 0)),
            super::message_prefix_hash_at(&second, (0, 1))
        );
    }

    #[test]
    fn message_cache_control_stateful_duplicate_blocks_require_context_match() {
        let first = json!({
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "before-a" },
                    { "type": "text", "text": "same" },
                    { "type": "text", "text": "after-a" },
                    { "type": "text", "text": "same" },
                    { "type": "text", "text": "after-b" }
                ]
            }]
        });
        let second = json!({
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "before-a" },
                    { "type": "text", "text": "same" },
                    { "type": "text", "text": "after-a" },
                    { "type": "text", "text": "inserted" },
                    { "type": "text", "text": "same" },
                    { "type": "text", "text": "after-b" }
                ]
            }]
        });
        let anchor = super::anchor_record_at(&first, (0, 3)).expect("anchor");
        let positions = super::find_stateful_anchor_positions(&second, &[anchor]);

        assert!(positions.is_empty());
    }

    #[test]
    fn message_cache_control_stateful_ttl_rewrite_updates_created_breakpoints() {
        let parsed = rewrite_messages_body_with_modes(
            stateful_session_body("session-ttl", 22),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::FiveMinutes,
            MessageCacheControlRewrite::Stateful,
        );

        for (message_idx, block_idx) in message_cache_control_positions(&parsed) {
            assert_eq!(
                parsed["messages"][message_idx]["content"][block_idx]["cache_control"]["ttl"],
                json!("5m")
            );
        }
    }

    #[test]
    fn message_cache_control_stateful_applies_to_api_mode() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "u1",
                        "cache_control": { "type": "ephemeral", "ttl": "1h" }
                    }]
                }]
            }),
            ClientType::API,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Stateful,
        );

        assert_eq!(
            parsed["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("1h")
        );
        assert_eq!(parsed["system"][2]["cache_control"]["ttl"], json!("1h"));
    }

    #[test]
    fn message_cache_control_auto_alias_applies_to_api_mode() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "u1",
                            "cache_control": { "type": "ephemeral", "ttl": "1h" }
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "text",
                            "text": "a1"
                        }]
                    }
                ]
            }),
            ClientType::API,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(message_cache_control_positions(&parsed), vec![(1, 0)]);
        assert_eq!(parsed["system"][2]["cache_control"]["ttl"], json!("1h"));
    }

    #[test]
    fn message_cache_control_auto_applies_to_api_mode() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "messages": [
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "u1",
                            "cache_control": { "type": "ephemeral", "ttl": "1h" }
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "text",
                            "text": "a1"
                        }]
                    }
                ]
            }),
            ClientType::API,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );

        assert_eq!(message_cache_control_positions(&parsed), vec![(1, 0)]);
        assert_eq!(parsed["system"][2]["cache_control"]["ttl"], json!("1h"));
    }

    #[test]
    fn message_cache_control_api_modes_use_sub2api_style_breakpoints() {
        for mode in [
            MessageCacheControlRewrite::Auto,
            MessageCacheControlRewrite::Rolling,
            MessageCacheControlRewrite::Stateful,
            MessageCacheControlRewrite::Sub2api,
        ] {
            let parsed = rewrite_messages_body_with_modes(
                json!({
                    "messages": [
                        {
                            "role": "user",
                            "content": "u1"
                        },
                        {
                            "role": "assistant",
                            "content": [{
                                "type": "text",
                                "text": "a1"
                            }]
                        },
                        {
                            "role": "user",
                            "content": [{
                                "type": "text",
                                "text": "u2",
                                "cache_control": { "type": "ephemeral", "ttl": "5m" }
                            }]
                        },
                        {
                            "role": "assistant",
                            "content": [{
                                "type": "tool_use",
                                "id": "toolu_1",
                                "name": "Read",
                                "input": {}
                            }]
                        },
                        {
                            "role": "user",
                            "content": [{
                                "type": "tool_result",
                                "tool_use_id": "toolu_1",
                                "content": "result"
                            }]
                        }
                    ]
                }),
                ClientType::API,
                CacheControlTtlRewrite::Off,
                mode,
            );

            assert_eq!(
                message_cache_control_positions(&parsed),
                vec![(2, 0), (4, 0)]
            );
            assert_eq!(parsed["messages"][0]["content"], json!("u1"));
            assert_eq!(parsed["system"][2]["cache_control"]["ttl"], json!("1h"));
        }
    }

    #[test]
    fn message_cache_control_sub2api_applies_to_claude_code_mode() {
        let parsed = rewrite_messages_body_with_modes(
            json!({
                "system": [{
                    "type": "text",
                    "text": "system",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "tools": [{
                    "name": "Read",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                }],
                "messages": [
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "u1",
                            "cache_control": { "type": "ephemeral", "ttl": "5m" }
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "text",
                            "text": "a1"
                        }]
                    },
                    {
                        "role": "user",
                        "content": [{
                            "type": "text",
                            "text": "u2"
                        }]
                    },
                    {
                        "role": "assistant",
                        "content": [{
                            "type": "text",
                            "text": "a2"
                        }]
                    }
                ]
            }),
            ClientType::ClaudeCode,
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Sub2api,
        );

        assert_eq!(
            message_cache_control_positions(&parsed),
            vec![(0, 0), (3, 0)]
        );
        assert!(parsed["system"][0].get("cache_control").is_some());
        assert!(parsed["tools"][0].get("cache_control").is_some());
    }

    #[test]
    fn api_mode_keeps_ttl_rewrite_without_creating_extra_breakpoints() {
        let parsed = rewrite_messages_body_with_ttl(
            json!({
                "cache_control": { "type": "ephemeral" },
                "system": [{
                    "type": "text",
                    "text": "sys",
                    "cache_control": { "type": "ephemeral" }
                }],
                "messages": [{
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hi",
                        "cache_control": { "type": "ephemeral" }
                    }]
                }],
                "tools": [{
                    "name": "a",
                    "input_schema": {},
                    "cache_control": { "type": "ephemeral" }
                }]
            }),
            ClientType::API,
            CacheControlTtlRewrite::OneHour,
        );

        assert_eq!(parsed["cache_control"]["ttl"], json!("1h"));
        assert!(parsed["system"][0].get("cache_control").is_none());
        assert!(parsed["system"][1].get("cache_control").is_none());
        assert_eq!(parsed["system"][2]["cache_control"]["ttl"], json!("1h"));
        assert!(
            parsed["messages"][0]["content"][0]
                .get("cache_control")
                .is_none()
        );
        assert_eq!(parsed["tools"][0]["cache_control"]["ttl"], json!("1h"));
    }

    #[test]
    fn api_mode_rewrites_system_to_billing_banner_and_expansion() {
        let parsed = rewrite_messages_body(
            json!({
                "model": "claude-sonnet-4-6",
                "system": "Be precise.",
                "messages": [{
                    "role": "user",
                    "content": [{ "type": "text", "text": "hello from user" }]
                }]
            }),
            ClientType::API,
        );

        let system = parsed["system"].as_array().expect("system array");
        assert_eq!(system.len(), 3);
        assert!(
            system[0]["text"].as_str().unwrap().starts_with(
                format!("x-anthropic-billing-header: cc_version={DEFAULT_CLAUDE_CODE_VERSION}.")
                    .as_str()
            )
        );
        assert!(system[0]["text"].as_str().unwrap().contains("cch="));
        assert_eq!(system[1]["text"], json!(super::CLAUDE_CODE_SYSTEM_PROMPT));
        assert_eq!(
            system[2]["text"],
            json!(super::CLAUDE_CODE_SYSTEM_PROMPT_EXPANSION)
        );
        assert_eq!(system[2]["cache_control"]["ttl"], json!("1h"));
        assert_eq!(
            parsed["messages"][0]["content"][0]["text"],
            json!("[System Instructions]\nBe precise.")
        );
        assert_eq!(
            parsed["messages"][1]["content"][0]["text"],
            json!("Understood. I will follow these instructions.")
        );
        assert_eq!(
            parsed["messages"][2]["content"][0]["text"],
            json!("hello from user")
        );
    }

    #[test]
    fn api_mode_preserves_existing_tools_and_tool_choice() {
        let parsed = rewrite_messages_body(
            json!({
                "tool_choice": { "type": "tool", "name": "Read" },
                "tools": [{
                    "name": "Read",
                    "description": "read file",
                    "input_schema": { "type": "object" }
                }],
                "messages": []
            }),
            ClientType::API,
        );

        assert_eq!(parsed["tools"][0]["name"], json!("Read"));
        assert_eq!(parsed["tool_choice"]["name"], json!("Read"));
    }

    #[test]
    fn api_messages_defaults_opus48_max_tokens_to_capture_value() {
        let parsed = rewrite_messages_body(
            json!({
                "model": "claude-opus-4-8",
                "messages": []
            }),
            ClientType::API,
        );

        assert_eq!(parsed["max_tokens"], json!(64000));
    }

    #[test]
    fn api_messages_defaults_fable_to_capture_max_tokens_and_fallbacks() {
        let parsed = rewrite_messages_body(
            json!({
                "model": "claude-fable-5",
                "messages": []
            }),
            ClientType::API,
        );

        assert_eq!(parsed["max_tokens"], json!(64000));
        assert_eq!(parsed["fallbacks"], json!([{ "model": "claude-opus-4-8" }]));
    }

    #[test]
    fn api_messages_keeps_existing_fable_fallbacks() {
        let parsed = rewrite_messages_body(
            json!({
                "model": "claude-fable-5",
                "messages": [],
                "fallbacks": [{"model": "custom-fallback"}]
            }),
            ClientType::API,
        );

        assert_eq!(parsed["fallbacks"], json!([{ "model": "custom-fallback" }]));
    }

    #[test]
    fn api_messages_defaults_haiku_max_tokens_to_title_generation_value() {
        let parsed = rewrite_messages_body(
            json!({
                "model": "claude-haiku-4-5-20251001",
                "messages": []
            }),
            ClientType::API,
        );

        assert_eq!(parsed["max_tokens"], json!(32000));
    }

    #[test]
    fn api_messages_defaults_other_claude_models_to_standard_max_tokens() {
        let parsed = rewrite_messages_body(
            json!({
                "model": "claude-sonnet-4-6",
                "messages": []
            }),
            ClientType::API,
        );

        assert_eq!(parsed["max_tokens"], json!(32000));
    }

    #[test]
    fn api_messages_preserves_probe_and_capture_max_tokens() {
        let haiku_probe = rewrite_messages_body(
            json!({
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1,
                "messages": []
            }),
            ClientType::API,
        );
        let opus_capture = rewrite_messages_body(
            json!({
                "model": "claude-opus-4-8",
                "max_tokens": 64000,
                "messages": []
            }),
            ClientType::API,
        );

        assert_eq!(haiku_probe["max_tokens"], json!(1));
        assert_eq!(opus_capture["max_tokens"], json!(64000));
    }

    #[test]
    fn api_messages_caps_oversized_max_tokens_to_capture_limit() {
        let parsed = rewrite_messages_body(
            json!({
                "model": "claude-opus-4-8",
                "max_tokens": 128000,
                "messages": []
            }),
            ClientType::API,
        );

        assert_eq!(parsed["max_tokens"], json!(64000));
    }

    #[test]
    fn claude_code_messages_do_not_normalize_max_tokens() {
        let existing = rewrite_messages_body(
            json!({
                "model": "claude-opus-4-8",
                "max_tokens": 128000,
                "messages": []
            }),
            ClientType::ClaudeCode,
        );
        let missing = rewrite_messages_body(
            json!({
                "model": "claude-opus-4-8",
                "messages": []
            }),
            ClientType::ClaudeCode,
        );

        assert_eq!(existing["max_tokens"], json!(128000));
        assert!(missing.get("max_tokens").is_none());
    }

    #[test]
    fn api_messages_headers_use_current_profile() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let body = json!({
            "metadata": {
                "user_id": json!({
                    "device_id": "device-1",
                    "account_uuid": "account-uuid",
                    "session_id": "session-1"
                }).to_string()
            }
        });
        let headers = rewriter.rewrite_headers(
            &std::collections::HashMap::new(),
            "/v1/messages",
            &account,
            ClientType::API,
            "claude-opus-4-8",
            &body,
        );
        assert_eq!(
            headers.get("User-Agent").unwrap(),
            claude_cli_user_agent(DEFAULT_CLAUDE_CODE_VERSION).as_str()
        );
        assert_eq!(headers.get("anthropic-beta").unwrap(), MESSAGE_BETA_TOKENS);
        assert_eq!(
            headers.get("X-Stainless-Package-Version").unwrap(),
            STAINLESS_PACKAGE_VERSION
        );
        assert_eq!(
            headers.get("X-Stainless-Runtime-Version").unwrap(),
            STAINLESS_RUNTIME_VERSION
        );
        assert_eq!(
            headers.get("X-Claude-Code-Session-Id").unwrap(),
            "session-1"
        );
    }

    #[test]
    fn fable_messages_headers_use_fallback_beta_without_context_1m() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let headers = rewriter.rewrite_headers(
            &std::collections::HashMap::new(),
            "/v1/messages",
            &account,
            ClientType::API,
            "claude-fable-5",
            &json!({}),
        );
        let beta = headers.get("anthropic-beta").unwrap();

        assert_eq!(beta, FABLE_MESSAGE_BETA_TOKENS);
        assert!(beta.contains(FABLE_FALLBACK_BETA_TOKENS));
        assert!(!beta.contains(CTX_1M));
        assert!(beta.contains("effort-2025-11-24,server-side-fallback-2026-06-01"));
        assert!(beta.contains("fallback-credit-2026-06-01,extended-cache-ttl-2025-04-11"));
    }

    #[test]
    fn fable_context_1m_beta_keeps_claude_code_order_when_allowed() {
        let mut account = test_account();
        account.allow_1m_models = "fable".into();
        let rewriter = Rewriter::new();
        let mut incoming = std::collections::HashMap::new();
        incoming.insert("anthropic-beta".to_string(), CTX_1M.to_string());

        let headers = rewriter.rewrite_headers(
            &incoming,
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            "claude-fable-5",
            &json!({}),
        );
        let beta = headers.get("anthropic-beta").unwrap();

        assert_eq!(
            beta,
            "claude-code-20250219,oauth-2025-04-20,context-1m-2025-08-07,interleaved-thinking-2025-05-14,redact-thinking-2026-02-12,thinking-token-count-2026-05-13,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,effort-2025-11-24,server-side-fallback-2026-06-01,fallback-credit-2026-06-01,extended-cache-ttl-2025-04-11,cache-diagnosis-2026-04-07"
        );
    }

    #[test]
    fn fable_1m_model_without_incoming_context_1m_does_not_inject_beta() {
        let mut account = test_account();
        account.allow_1m_models = "fable".into();
        let rewriter = Rewriter::new();

        let headers = rewriter.rewrite_headers(
            &std::collections::HashMap::new(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            "claude-fable-5",
            &json!({}),
        );
        let beta = headers.get("anthropic-beta").unwrap();

        assert_eq!(beta, FABLE_MESSAGE_BETA_TOKENS);
        assert!(!beta.contains(CTX_1M));
    }

    #[test]
    fn opus_claude_code_headers_preserve_context_1m_when_allowed() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let mut incoming = std::collections::HashMap::new();
        incoming.insert("anthropic-beta".to_string(), CTX_1M.to_string());

        let headers = rewriter.rewrite_headers(
            &incoming,
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            "claude-opus-4-8[1m]",
            &json!({}),
        );
        let beta = headers.get("anthropic-beta").unwrap();

        assert!(beta.contains(CTX_1M));
        assert!(beta.contains("claude-code-20250219"));
        assert!(!beta.contains("server-side-fallback-2026-06-01"));
    }

    #[test]
    fn api_mode_overwrites_client_metadata_user_id_and_aligns_session_header() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let body = json!({
            "model": "claude-sonnet-4-6",
            "metadata": {
                "user_id": "client-supplied-user",
                "trace_id": "keep-me"
            },
            "messages": [{
                "role": "user",
                "content": [{ "type": "text", "text": "stable api session" }]
            }]
        });

        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::API,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Off,
        );
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let user_id = parsed["metadata"]["user_id"].as_str().unwrap();
        let user_id_json: serde_json::Value = serde_json::from_str(user_id).unwrap();

        assert_ne!(user_id, "client-supplied-user");
        assert_eq!(parsed["metadata"]["trace_id"], json!("keep-me"));
        assert_eq!(user_id_json["device_id"], json!(account.device_id));
        assert_eq!(
            user_id_json["account_uuid"],
            json!(account.account_uuid.clone().unwrap())
        );

        let headers = rewriter.rewrite_headers(
            &std::collections::HashMap::new(),
            "/v1/messages",
            &account,
            ClientType::API,
            "claude-sonnet-4-6",
            &parsed,
        );
        assert_eq!(
            headers.get("X-Claude-Code-Session-Id").unwrap(),
            user_id_json["session_id"].as_str().unwrap()
        );
    }

    #[test]
    fn client_type_uses_claude_code_ua_as_authoritative_signal() {
        let valid_json_user_id = json!({
            "device_id": "device-1",
            "account_uuid": "550e8400-e29b-41d4-a716-446655440000",
            "session_id": "123e4567-e89b-12d3-a456-426614174000"
        })
        .to_string();
        let valid_legacy_user_id = "user_a1b2c3d4_account_550e8400-e29b-41d4-a716-446655440000_session_123e4567-e89b-12d3-a456-426614174000";

        assert_eq!(
            super::detect_client_type(
                "claude-cli/2.1.156 (external, cli)",
                "/v1/messages",
                &json!({"metadata": {"user_id": valid_json_user_id}})
            ),
            ClientType::ClaudeCode
        );
        assert_eq!(
            super::detect_client_type(
                "Claude-Code/2.1.156",
                "/v1/messages",
                &json!({"metadata": {"user_id": valid_legacy_user_id}})
            ),
            ClientType::ClaudeCode
        );
        assert_eq!(
            super::detect_client_type(
                "curl/8.0",
                "/v1/messages",
                &json!({"metadata": {"user_id": valid_legacy_user_id}})
            ),
            ClientType::API
        );
        assert_eq!(
            super::detect_client_type(
                "claude-cli/2.1.156",
                "/v1/messages",
                &json!({"metadata": {"user_id": "session_abc"}})
            ),
            ClientType::ClaudeCode
        );
        assert_eq!(
            super::detect_client_type(
                "claude-cli/2.1.156",
                "/v1/messages",
                &json!({"messages": []})
            ),
            ClientType::ClaudeCode
        );
        assert_eq!(
            super::detect_client_type(
                "claude-cli/2.1.156",
                "/api/event_logging/2024-06-01/events",
                &json!({"events": []})
            ),
            ClientType::ClaudeCode
        );
    }

    #[test]
    fn metadata_user_id_diagnostic_treats_json_session_as_present() {
        let user_id = json!({
            "device_id": "device-1",
            "account_uuid": "not-a-uuid",
            "session_id": "123e4567-e89b-12d3-a456-426614174000"
        })
        .to_string();

        let diag = super::claude_code_metadata_user_id_diagnostic(
            &json!({"metadata": {"user_id": user_id}}),
        );
        let summary = diag.human_summary();

        assert!(summary.starts_with("present("), "{summary}");
        assert!(summary.contains("reason=json_session_present"), "{summary}");
        assert!(
            summary.contains("json_fields=device:Y/account:Y/session:Y"),
            "{summary}"
        );
        assert!(summary.contains("uuid=account:N/session:Y"), "{summary}");
    }

    #[test]
    fn api_mode_cch_is_computed_from_final_body_without_internal_session_marker() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let body = json!({
            "model": "claude-sonnet-4-6",
            "system": "Act as tester.",
            "messages": [{
                "role": "user",
                "content": [{ "type": "text", "text": "hello cch" }]
            }]
        });

        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::API,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Auto,
        );
        let text = String::from_utf8(out.clone()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();

        assert!(!text.contains("_session_id"));
        assert!(!text.contains("cch=00000"));
        assert!(
            parsed["metadata"]["user_id"]
                .as_str()
                .unwrap()
                .contains("session_id")
        );
        assert!(!message_cache_control_positions(&parsed).is_empty());

        let actual = super::CCH_VALUE_REGEX
            .find(&text)
            .map(|m| m.as_str().to_string())
            .expect("cch value exists");
        let mut placeholder_body = out;
        let cch_pos = placeholder_body
            .windows(super::CCH_PLACEHOLDER.len())
            .position(|window| window.starts_with(b"cch="))
            .expect("cch value position");
        placeholder_body[cch_pos + 4..cch_pos + 9].copy_from_slice(b"00000");
        let expected = String::from_utf8(compute_cch_attestation(
            placeholder_body,
            DEFAULT_CLAUDE_CODE_VERSION,
        ))
        .unwrap();

        assert!(expected.contains(&actual));
    }

    #[test]
    fn message_beta_tokens_include_cache_features_without_forcing_1m() {
        let account = Account {
            allow_1m_models: String::new(),
            ..test_account()
        };
        let rewriter = Rewriter::new();
        let mut incoming = std::collections::HashMap::new();
        incoming.insert(
            "anthropic-beta".to_string(),
            "context-1m-2025-08-07,extended-cache-ttl-2025-04-11".to_string(),
        );

        let headers = rewriter.rewrite_headers(
            &incoming,
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            "claude-sonnet-4-6",
            &json!({}),
        );
        let beta = headers.get("anthropic-beta").unwrap();

        assert!(beta.contains("extended-cache-ttl-2025-04-11"));
        assert!(beta.contains("cache-diagnosis-2026-04-07"));
        assert!(!beta.contains("context-1m-2025-08-07"));
    }

    #[test]
    fn claude_code_rewrite_skips_cached_system_blocks() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let body = json!({
            "system": [
                {
                    "type": "text",
                    "text": "x-anthropic-billing-header: cc_version=2.1.156.abc; cc_entrypoint=cli; cch=12345;\nPlatform: darwin\nWorking directory: /Users/real/project\n<system-reminder>Git user: real</system-reminder>",
                    "cache_control": { "type": "ephemeral", "ttl": "1h" }
                },
                {
                    "type": "text",
                    "text": "Platform: darwin\nWorking directory: /Users/real/project"
                }
            ],
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "<system-reminder>Git user: real\nWorking directory: /Users/real/project</system-reminder>",
                        "cache_control": { "type": "ephemeral", "ttl": "1h" }
                    },
                    {
                        "type": "text",
                        "text": "<system-reminder>Git user: real\nWorking directory: /Users/real/project</system-reminder>"
                    }
                ]
            }]
        });

        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Off,
        );
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();

        assert_eq!(
            parsed["system"][0]["text"],
            "x-anthropic-billing-header: cc_version=2.1.156.abc; cc_entrypoint=cli; cch=12345;\nPlatform: darwin\nWorking directory: /Users/real/project\n<system-reminder>Git user: real</system-reminder>"
        );
        assert_eq!(
            parsed["system"][1]["text"],
            "Platform: linux\nWorking directory: /home/user/project"
        );
        assert_eq!(
            parsed["messages"][0]["content"][0]["text"],
            "<system-reminder>Git user: real\nWorking directory: /Users/real/project</system-reminder>"
        );
        assert_eq!(
            parsed["messages"][0]["content"][1]["text"],
            "<system-reminder>Git user: real\nWorking directory: /home/user/project</system-reminder>"
        );
    }

    #[test]
    fn endpoint_headers_use_distinct_profiles() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let empty = std::collections::HashMap::new();
        let body = json!({});

        let event_headers = rewriter.rewrite_headers(
            &empty,
            "/api/event_logging/v2/batch",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert_eq!(
            event_headers.get("User-Agent").unwrap(),
            claude_code_user_agent(DEFAULT_CLAUDE_CODE_VERSION).as_str()
        );
        assert_eq!(event_headers.get("x-service-name").unwrap(), "claude-code");
        assert_eq!(
            event_headers.get("anthropic-beta").unwrap(),
            "oauth-2025-04-20"
        );

        let legacy_event_headers = rewriter.rewrite_headers(
            &empty,
            "/api/event_logging/batch",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert_eq!(
            legacy_event_headers.get("anthropic-beta").unwrap(),
            "oauth-2025-04-20"
        );

        let eval_headers = rewriter.rewrite_headers(
            &empty,
            "/api/eval/sdk-zAZezfDKGoZuXXKe",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert_eq!(eval_headers.get("User-Agent").unwrap(), "Bun/1.3.14");

        let trigger_headers = rewriter.rewrite_headers(
            &empty,
            "/v1/code/triggers",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert_eq!(
            trigger_headers.get("anthropic-beta").unwrap(),
            "ccr-triggers-2026-01-30"
        );
        assert_eq!(
            trigger_headers.get("anthropic-client-platform").unwrap(),
            "claude_code_cli"
        );
        assert_eq!(
            trigger_headers.get("x-organization-uuid").unwrap(),
            "org-uuid"
        );

        let mcp_headers = rewriter.rewrite_headers(
            &empty,
            "/v1/mcp_servers",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert_eq!(
            mcp_headers.get("anthropic-beta").unwrap(),
            "mcp-servers-2025-12-04"
        );
        assert_eq!(mcp_headers.get("User-Agent").unwrap(), "axios/1.15.2");
        assert_eq!(
            mcp_headers
                .get("anthropic-mcp-client-capabilities")
                .unwrap(),
            MCP_CLIENT_CAPABILITIES
        );
        assert_eq!(
            mcp_headers.get("mcp-protocol-version").unwrap(),
            MCP_PROTOCOL_VERSION
        );

        let mut incoming_mcp_headers = std::collections::HashMap::new();
        incoming_mcp_headers.insert("User-Agent".to_string(), "claude-cli/2.1.172".to_string());
        incoming_mcp_headers.insert("anthropic-beta".to_string(), "client-token".to_string());
        let cc_mcp_headers = rewriter.rewrite_headers(
            &incoming_mcp_headers,
            "/v1/mcp_servers",
            &account,
            ClientType::ClaudeCode,
            "",
            &body,
        );
        assert_eq!(
            cc_mcp_headers
                .get("anthropic-mcp-client-capabilities")
                .unwrap(),
            MCP_CLIENT_CAPABILITIES
        );
        assert_eq!(
            cc_mcp_headers.get("mcp-protocol-version").unwrap(),
            MCP_PROTOCOL_VERSION
        );

        let registry_headers = rewriter.rewrite_headers(
            &empty,
            "/mcp-registry/v0/servers",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert!(registry_headers.get("anthropic-beta").is_none());

        let oauth_headers = rewriter.rewrite_headers(
            &empty,
            "/api/oauth/account/settings",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert_eq!(
            oauth_headers.get("anthropic-beta").unwrap(),
            "oauth-2025-04-20"
        );
        assert_eq!(
            oauth_headers.get("User-Agent").unwrap(),
            claude_cli_user_agent(DEFAULT_CLAUDE_CODE_VERSION).as_str()
        );

        let penguin_headers = rewriter.rewrite_headers(
            &empty,
            "/api/claude_code_penguin_mode",
            &account,
            ClientType::API,
            "",
            &body,
        );
        assert_eq!(
            penguin_headers.get("anthropic-beta").unwrap(),
            "oauth-2025-04-20"
        );
        assert_eq!(penguin_headers.get("User-Agent").unwrap(), "axios/1.15.2");
    }

    #[test]
    fn endpoint_wire_order_matches_current_capture() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let empty = std::collections::HashMap::new();
        let body = json!({
            "metadata": {
                "user_id": json!({
                    "device_id": "device-1",
                    "account_uuid": "account-uuid",
                    "session_id": "session-1"
                }).to_string()
            }
        });

        let mut message_headers = rewriter.rewrite_headers(
            &empty,
            "/v1/messages",
            &account,
            ClientType::API,
            "claude-opus-4-8",
            &body,
        );
        message_headers.insert("Authorization".into(), "Bearer redacted".into());
        assert_eq!(
            ordered_header_names("/v1/messages", &message_headers),
            vec![
                "Accept",
                "Authorization",
                "Content-Type",
                "User-Agent",
                "X-Claude-Code-Session-Id",
                "X-Stainless-Arch",
                "X-Stainless-Lang",
                "X-Stainless-OS",
                "X-Stainless-Package-Version",
                "X-Stainless-Retry-Count",
                "X-Stainless-Runtime",
                "X-Stainless-Runtime-Version",
                "X-Stainless-Timeout",
                "anthropic-beta",
                "anthropic-dangerous-direct-browser-access",
                "anthropic-version",
                "x-app",
                "x-client-request-id",
                "Connection",
                "Host",
                "Accept-Encoding",
            ]
        );

        let mut event_headers = rewriter.rewrite_headers(
            &empty,
            "/api/event_logging/v2/batch",
            &account,
            ClientType::API,
            "",
            &json!({}),
        );
        event_headers.insert("Authorization".into(), "Bearer redacted".into());
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

        let mut eval_headers = rewriter.rewrite_headers(
            &empty,
            "/api/eval/sdk-zAZezfDKGoZuXXKe",
            &account,
            ClientType::API,
            "",
            &json!({}),
        );
        eval_headers.insert("Authorization".into(), "Bearer redacted".into());
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

        let mut trigger_headers = rewriter.rewrite_headers(
            &empty,
            "/v1/code/triggers",
            &account,
            ClientType::API,
            "",
            &json!({}),
        );
        trigger_headers.insert("Authorization".into(), "Bearer redacted".into());
        assert_eq!(
            ordered_header_names("/v1/code/triggers", &trigger_headers),
            vec![
                "Accept",
                "Accept-Encoding",
                "Authorization",
                "Content-Type",
                "User-Agent",
                "anthropic-beta",
                "anthropic-client-platform",
                "anthropic-version",
                "x-organization-uuid",
                "Connection",
                "Host",
            ]
        );

        let mut mcp_headers = rewriter.rewrite_headers(
            &empty,
            "/v1/mcp_servers",
            &account,
            ClientType::API,
            "",
            &json!({}),
        );
        mcp_headers.insert("Authorization".into(), "Bearer redacted".into());
        assert_eq!(
            ordered_header_names("/v1/mcp_servers", &mcp_headers),
            vec![
                "Accept",
                "Accept-Encoding",
                "Authorization",
                "Content-Type",
                "User-Agent",
                "anthropic-beta",
                "anthropic-mcp-client-capabilities",
                "anthropic-version",
                "mcp-protocol-version",
                "Connection",
                "Host",
            ]
        );
    }

    #[test]
    fn get_profile_omits_content_type_when_capture_has_none() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let empty = std::collections::HashMap::new();
        let body = json!({});

        for path in [
            "/api/oauth/account/settings",
            "/api/claude_code_grove",
            "/api/claude_code_penguin_mode",
            "/mcp-registry/v0/servers",
        ] {
            let headers =
                rewriter.rewrite_headers(&empty, path, &account, ClientType::API, "", &body);
            assert!(
                !headers.contains_key("content-type"),
                "{} should not include content-type",
                path
            );
        }
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

    #[test]
    fn growthbook_rewrite_adds_current_attributes() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let body = json!({
            "attributes": {
                "apiBaseUrlHost": "proxy.local",
                "platform": "darwin",
                "appVersion": "old"
            }
        });
        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/api/eval/sdk-zAZezfDKGoZuXXKe",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Off,
        );
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let attrs = parsed.get("attributes").unwrap();
        assert_eq!(attrs.get("id").unwrap(), "device-1");
        assert_eq!(attrs.get("userType").unwrap(), "external");
        assert_eq!(
            attrs.get("rateLimitTier").unwrap(),
            "default_claude_max_20x"
        );
        assert_eq!(attrs.get("entrypoint").unwrap(), "cli");
        assert_eq!(
            attrs.get("appVersion").unwrap(),
            DEFAULT_CLAUDE_CODE_VERSION
        );
        assert!(attrs.get("apiBaseUrlHost").is_none());
    }

    #[test]
    fn event_logging_v2_path_is_rewritten() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let process = {
            let process = json!({
                "uptime": 12.0,
                "rss": 999,
                "heapTotal": 999,
                "heapUsed": 999,
                "external": 999,
                "arrayBuffers": 999,
                "constrainedMemory": 999,
            });
            base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&process).unwrap())
        };
        let body = json!({
            "events": [{
                "event_type": "ClaudeCodeInternalEvent",
                "event_data": {
                    "device_id": "old",
                    "email": "old@example.com",
                    "account_uuid": "old-account",
                    "organization_uuid": "old-org",
                    "env": {},
                    "process": process,
                    "additional_metadata": "",
                    "user_attributes": "{\"id\":\"old\",\"apiBaseUrlHost\":\"proxy.local\"}"
                }
            }]
        });
        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/api/event_logging/v2/batch",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::Off,
            MessageCacheControlRewrite::Off,
        );
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        let event = &parsed["events"][0]["event_data"];
        assert_eq!(event["device_id"], "device-1");
        assert_eq!(event["email"], "user@example.com");
        assert_eq!(event["account_uuid"], "account-uuid");
        assert_eq!(event["organization_uuid"], "org-uuid");
        let attrs: serde_json::Value =
            serde_json::from_str(event["user_attributes"].as_str().unwrap()).unwrap();
        assert_eq!(attrs["userType"], "external");
        assert!(attrs.get("apiBaseUrlHost").is_none());

        let rewritten_process_b64 = event["process"].as_str().unwrap();
        let rewritten_process_bytes = base64::engine::general_purpose::STANDARD
            .decode(rewritten_process_b64)
            .unwrap();
        let rewritten_process: serde_json::Value =
            serde_json::from_slice(&rewritten_process_bytes).unwrap();
        assert_eq!(rewritten_process["uptime"], 12.0);
        assert!(
            rewritten_process["heapUsed"].as_i64().unwrap()
                <= rewritten_process["heapTotal"].as_i64().unwrap()
        );
        assert_eq!(rewritten_process["constrainedMemory"], 0);
    }

    #[test]
    fn cc_version_suffix_uses_string_indices() {
        assert_eq!(
            compute_cc_version_suffix("abcdefghijklmno pqrstuvwxyz", "2.1.156"),
            "c09"
        );
        assert_eq!(
            compute_cc_version_suffix("abcd你fghijklmnopqrstuv", "2.1.156"),
            "45e"
        );
    }

    #[test]
    fn cc_version_suffix_source_uses_last_user_text_block() {
        let body = json!({
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": "cwd: /tmp\nplatform: linux" },
                    { "type": "text", "text": "abcd简fgohijklmnopqrse" }
                ]
            }]
        });

        assert_eq!(extract_first_user_message(&body), "abcd简fgohijklmnopqrse");
        assert_eq!(
            compute_cc_version_suffix(&extract_first_user_message(&body), "2.1.172"),
            "51f"
        );
        assert_eq!(
            compute_cc_version_suffix(&extract_first_user_message(&body), "2.1.173"),
            "262"
        );
    }

    #[test]
    fn random_cc_version_suffix_is_always_three_chars() {
        assert_eq!(super::random_cc_version_suffix([0, 1]), "000");
        assert_eq!(super::random_cc_version_suffix([0x0f, 0xff]), "0ff");
        assert_eq!(super::random_cc_version_suffix([0xff, 0xff]), "fff");
    }

    #[test]
    fn cch_seed_is_versioned() {
        assert_eq!(
            cch_attestation_seed(DEFAULT_CLAUDE_CODE_VERSION),
            0x4D659218E32A3268
        );
        assert_eq!(cch_attestation_seed("2.1.156"), 0x4D659218E32A3268);
        assert_eq!(cch_attestation_seed("2.1.169"), 0x4D659218E32A3268);
        assert_eq!(cch_attestation_seed("2.1.172"), 0x4D659218E32A3268);
        assert_eq!(cch_attestation_seed("2.1.173"), 0x4D659218E32A3268);
        assert_eq!(cch_attestation_seed("2.1.81"), 0x6E52736AC806831E);
        assert_eq!(cch_attestation_seed("2.1.999"), 0x6E52736AC806831E);
    }

    #[test]
    fn cch_rewrite_uses_2169_seed() {
        let body = br#"{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.169.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[]}"#;
        let out = compute_cch_attestation(body.to_vec(), "2.1.169");
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("cch=27300"));
    }

    #[test]
    fn cch_2172_and_2173_opus_normalize_top_level_model_and_max_tokens() {
        let body = br#"{"model":"claude-opus-4-8","max_tokens":64000,"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.173.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[]}"#;
        let normalized = cch_attestation_input(body, "2.1.173");

        assert_eq!(
            normalized,
            br#"{"model":"","system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.173.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[]}"#
        );
        assert_eq!(cch_attestation_input(body, "2.1.172"), normalized);

        let out = compute_cch_attestation(body.to_vec(), "2.1.173");
        let expected_hash = xxhash_rust::xxh64::xxh64(&normalized, cch_attestation_seed("2.1.173"));
        let expected_cch = format!("cch={:05x}", expected_hash & 0xFFFFF);
        assert!(String::from_utf8(out).unwrap().contains(&expected_cch));
    }

    #[test]
    fn cch_2172_and_2173_fable_normalize_top_level_fallbacks_only() {
        let body = br#"{"model":"claude-fable-5","max_tokens":64000,"tools":[{"name":"Nested","input_schema":{"type":"object","properties":{"fallbacks":{"model":"keep-nested"},"max_tokens":{"type":"integer"}}}}],"fallbacks":[{"model":"claude-opus-4-8"}],"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.173.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[{"role":"user","content":[{"type":"text","text":"nested model and fallbacks stay"}]}]}"#;
        let normalized = String::from_utf8(cch_attestation_input(body, "2.1.173")).unwrap();
        let normalized_2172 = String::from_utf8(cch_attestation_input(body, "2.1.172")).unwrap();

        assert_eq!(normalized_2172, normalized);
        assert!(normalized.contains(r#""model":"""#));
        assert!(!normalized.contains(r#","max_tokens":64000"#));
        assert!(!normalized.contains(r#","fallbacks":[{"model":"claude-opus-4-8"}]"#));
        assert!(normalized.contains(r#""fallbacks":{"model":"keep-nested"}"#));
        assert!(normalized.contains(r#""max_tokens":{"type":"integer"}"#));
        assert!(normalized.contains("nested model and fallbacks stay"));
    }

    #[test]
    fn cch_2169_keeps_full_body_profile() {
        let body = br#"{"model":"claude-opus-4-8","max_tokens":64000,"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.169.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[]}"#;

        assert_eq!(cch_attestation_input(body, "2.1.169"), body);
    }

    #[test]
    fn cch_rewrite_keeps_legacy_seed_for_old_versions() {
        let body = br#"{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.81.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[]}"#;
        let out = compute_cch_attestation(body.to_vec(), "2.1.81");
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("cch=afd26"));
    }

    #[test]
    fn cch_refresh_recomputes_existing_value_after_retry_body_change() {
        let body = br#"{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.156.b94; cc_entrypoint=cli; cch=40943;"}],"messages":[{"role":"assistant","content":[{"type":"text","text":"sanitized"}]}]}"#;
        let out = super::refresh_cch_attestation(body.to_vec(), DEFAULT_CLAUDE_CODE_VERSION);
        let text = String::from_utf8(out).unwrap();

        assert!(!text.contains("cch=40943"));
        assert!(!text.contains("cch=00000"));
        assert!(text.contains("cch="));
    }

    #[test]
    fn api_cch_refresh_ignores_account_billing_mode_after_retry_body_change() {
        let mut account = test_account();
        account.billing_mode = BillingMode::Strip;
        let rewriter = Rewriter::new();
        let body = br#"{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.156.b94; cc_entrypoint=cli; cch=40943;"}],"messages":[{"role":"assistant","content":[{"type":"text","text":"api sanitized"}]}]}"#;
        let out = rewriter.refresh_cch_attestation(body.to_vec(), &account, ClientType::API);
        let text = String::from_utf8(out.clone()).unwrap();

        assert!(!text.contains("cch=40943"));
        assert!(!text.contains("cch=00000"));

        let actual = super::CCH_VALUE_REGEX
            .find(&text)
            .map(|m| m.as_str().to_string())
            .expect("cch value exists");
        let mut placeholder_body = out;
        let cch_pos = placeholder_body
            .windows(super::CCH_PLACEHOLDER.len())
            .position(|window| window.starts_with(b"cch="))
            .expect("cch value position");
        placeholder_body[cch_pos + 4..cch_pos + 9].copy_from_slice(b"00000");
        let expected = String::from_utf8(compute_cch_attestation(
            placeholder_body,
            DEFAULT_CLAUDE_CODE_VERSION,
        ))
        .unwrap();

        assert!(expected.contains(&actual));
    }

    #[test]
    fn message_cache_control_legacy_recomputes_cch_after_final_body_changes() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let body = json!({
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.156.abc; cc_entrypoint=cli; cch=12345;"
            }],
            "messages": [
                {
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "hello"
                    }]
                },
                {
                    "role": "assistant",
                    "content": [{
                        "type": "text",
                        "text": "ok"
                    }]
                },
                {
                    "role": "user",
                    "content": [{
                        "type": "text",
                        "text": "again",
                        "cache_control": { "type": "ephemeral", "ttl": "1h" }
                    }]
                },
                {
                    "role": "assistant",
                    "content": [{
                        "type": "text",
                        "text": "done"
                    }]
                }
            ]
        });

        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::FiveMinutes,
            MessageCacheControlRewrite::Auto,
        );
        let text = String::from_utf8(out.clone()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();

        assert!(!text.contains("cch=12345"));
        assert!(!text.contains("cch=00000"));
        assert_eq!(
            parsed["messages"][0]["content"][0]["cache_control"]["ttl"],
            json!("5m")
        );
        assert_eq!(
            parsed["messages"][3]["content"][0]["cache_control"]["ttl"],
            json!("5m")
        );

        let actual = super::CCH_VALUE_REGEX
            .find(&text)
            .map(|m| m.as_str().to_string())
            .expect("cch value exists");
        let mut placeholder_body = out;
        let cch_pos = placeholder_body
            .windows(super::CCH_PLACEHOLDER.len())
            .position(|window| window.starts_with(b"cch="))
            .expect("cch value position");
        placeholder_body[cch_pos + 4..cch_pos + 9].copy_from_slice(b"00000");
        let expected = String::from_utf8(compute_cch_attestation(
            placeholder_body,
            DEFAULT_CLAUDE_CODE_VERSION,
        ))
        .unwrap();

        assert!(expected.contains(&actual));
    }

    #[test]
    fn message_cache_control_auto_recomputes_cch_after_final_body_changes() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let messages: Vec<serde_json::Value> = (0..60)
            .map(|idx| {
                json!({
                    "role": if idx % 2 == 0 { "user" } else { "assistant" },
                    "content": [{
                        "type": "text",
                        "text": format!("block-{}", idx)
                    }]
                })
            })
            .collect();
        let body = json!({
            "system": [{
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.156.abc; cc_entrypoint=cli; cch=12345;"
            }],
            "messages": messages
        });

        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::FiveMinutes,
            MessageCacheControlRewrite::Auto,
        );
        let text = String::from_utf8(out.clone()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();

        assert!(!text.contains("cch=12345"));
        assert!(!text.contains("cch=00000"));
        assert_eq!(message_cache_control_positions(&parsed).len(), 4);

        let actual = super::CCH_VALUE_REGEX
            .find(&text)
            .map(|m| m.as_str().to_string())
            .expect("cch value exists");
        let mut placeholder_body = out;
        let cch_pos = placeholder_body
            .windows(super::CCH_PLACEHOLDER.len())
            .position(|window| window.starts_with(b"cch="))
            .expect("cch value position");
        placeholder_body[cch_pos + 4..cch_pos + 9].copy_from_slice(b"00000");
        let expected = String::from_utf8(compute_cch_attestation(
            placeholder_body,
            DEFAULT_CLAUDE_CODE_VERSION,
        ))
        .unwrap();

        assert!(expected.contains(&actual));
    }

    #[test]
    fn message_cache_control_stateful_recomputes_cch_after_final_body_changes() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let mut body = stateful_session_body("session-cch", 60);
        body.as_object_mut().unwrap().insert(
            "system".into(),
            json!([{
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.156.abc; cc_entrypoint=cli; cch=12345;"
            }]),
        );

        let out = rewriter.rewrite_body(
            &serde_json::to_vec(&body).unwrap(),
            "/v1/messages",
            &account,
            ClientType::ClaudeCode,
            EnvPassthrough::default(),
            CacheControlTtlRewrite::FiveMinutes,
            MessageCacheControlRewrite::Stateful,
        );
        let text = String::from_utf8(out.clone()).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();

        assert!(!text.contains("cch=12345"));
        assert!(!text.contains("cch=00000"));
        assert_eq!(message_cache_control_positions(&parsed).len(), 4);

        let actual = super::CCH_VALUE_REGEX
            .find(&text)
            .map(|m| m.as_str().to_string())
            .expect("cch value exists");
        let mut placeholder_body = out;
        let cch_pos = placeholder_body
            .windows(super::CCH_PLACEHOLDER.len())
            .position(|window| window.starts_with(b"cch="))
            .expect("cch value position");
        placeholder_body[cch_pos + 4..cch_pos + 9].copy_from_slice(b"00000");
        let expected = String::from_utf8(compute_cch_attestation(
            placeholder_body,
            DEFAULT_CLAUDE_CODE_VERSION,
        ))
        .unwrap();

        assert!(expected.contains(&actual));
    }

    #[test]
    fn strip_token_in_middle() {
        let got = strip_beta_token(
            "oauth-2025-04-20,context-1m-2025-08-07,interleaved-thinking-2025-05-14",
            CTX_1M,
        );
        assert_eq!(got, "oauth-2025-04-20,interleaved-thinking-2025-05-14");
    }

    #[test]
    fn strip_token_at_start() {
        let got = strip_beta_token(
            "context-1m-2025-08-07,oauth-2025-04-20,interleaved-thinking-2025-05-14",
            CTX_1M,
        );
        assert_eq!(got, "oauth-2025-04-20,interleaved-thinking-2025-05-14");
    }

    #[test]
    fn strip_token_at_end() {
        let got = strip_beta_token(
            "oauth-2025-04-20,interleaved-thinking-2025-05-14,context-1m-2025-08-07",
            CTX_1M,
        );
        assert_eq!(got, "oauth-2025-04-20,interleaved-thinking-2025-05-14");
    }

    #[test]
    fn strip_token_not_present() {
        let got = strip_beta_token("oauth-2025-04-20,interleaved-thinking-2025-05-14", CTX_1M);
        assert_eq!(got, "oauth-2025-04-20,interleaved-thinking-2025-05-14");
    }

    #[test]
    fn strip_token_with_whitespace() {
        // 客户端常发带空格格式；trim 后仍能匹配。
        let got = strip_beta_token(
            "oauth-2025-04-20, context-1m-2025-08-07 , interleaved-thinking-2025-05-14",
            CTX_1M,
        );
        assert_eq!(got, "oauth-2025-04-20,interleaved-thinking-2025-05-14");
    }

    #[test]
    fn strip_token_empty_header() {
        assert_eq!(strip_beta_token("", CTX_1M), "");
    }

    #[test]
    fn strip_token_header_is_only_the_token() {
        assert_eq!(strip_beta_token(CTX_1M, CTX_1M), "");
    }

    #[test]
    fn strip_token_multiple_occurrences() {
        // 理论上不应该出现，但工具必须全部剥干净。
        let got = strip_beta_token(
            "context-1m-2025-08-07,oauth-2025-04-20,context-1m-2025-08-07",
            CTX_1M,
        );
        assert_eq!(got, "oauth-2025-04-20");
    }

    #[test]
    fn strip_token_ignores_empty_segments() {
        // ",,context-1m-2025-08-07,,oauth-2025-04-20,," 这种脏数据也能处理。
        let got = strip_beta_token(",,context-1m-2025-08-07,,oauth-2025-04-20,,", CTX_1M);
        assert_eq!(got, "oauth-2025-04-20");
    }

    // ---- matches_1m_whitelist ----

    #[test]
    fn whitelist_default_opus_matches_opus_models() {
        assert!(matches_1m_whitelist("claude-opus-4-7", "opus"));
        assert!(matches_1m_whitelist("claude-opus-4-6", "opus"));
        // 大小写不敏感
        assert!(matches_1m_whitelist("Claude-Opus-4-7", "opus"));
        assert!(matches_1m_whitelist("claude-opus-4-7", "OPUS"));
    }

    #[test]
    fn whitelist_default_opus_rejects_non_opus() {
        assert!(!matches_1m_whitelist("claude-sonnet-4-5", "opus"));
        assert!(!matches_1m_whitelist("claude-haiku-4-5", "opus"));
    }

    #[test]
    fn whitelist_empty_means_block_everyone() {
        // 运维显式置空 = 所有模型都不放行
        assert!(!matches_1m_whitelist("claude-opus-4-7", ""));
        assert!(!matches_1m_whitelist("claude-sonnet-4-5", ""));
    }

    #[test]
    fn whitelist_multiple_patterns() {
        // 运维配置 "opus,sonnet" → opus 和 sonnet 都放行
        assert!(matches_1m_whitelist("claude-opus-4-7", "opus,sonnet"));
        assert!(matches_1m_whitelist("claude-sonnet-4-5", "opus,sonnet"));
        assert!(!matches_1m_whitelist("claude-haiku-4-5", "opus,sonnet"));
    }

    #[test]
    fn whitelist_ignores_whitespace_and_empty_segments() {
        // 脏输入："opus, , ,sonnet,," 也能解析
        assert!(matches_1m_whitelist("claude-opus-4-7", "opus, , ,sonnet,,"));
        assert!(matches_1m_whitelist(
            "claude-sonnet-4-5",
            "opus, , ,sonnet,,"
        ));
    }

    #[test]
    fn whitelist_precise_model_id_match() {
        // 配置可以用完整 model id 做精确控制
        assert!(matches_1m_whitelist("claude-opus-4-7", "claude-opus-4-7"));
        assert!(!matches_1m_whitelist("claude-opus-4-6", "claude-opus-4-7"));
    }
}
