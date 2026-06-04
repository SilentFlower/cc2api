use base64::Engine;
use once_cell::sync::Lazy;
use rand::Rng;
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::model::account::{Account, BillingMode, CanonicalEnvData, CanonicalPromptEnvData};
use crate::model::identity::{
    device_profile, process_snapshot, process_snapshot_json, request_profile, DeviceProfile,
};
use crate::service::version_profile::{
    claude_cli_user_agent, claude_code_user_agent, growthbook_user_agent, is_event_logging_path,
    normalize_version, CODE_TRIGGERS_BETA_TOKEN, DEFAULT_CLAUDE_CODE_VERSION,
    MCP_SERVERS_BETA_TOKEN, MESSAGE_BETA_TOKENS, OAUTH_BETA_TOKEN, STAINLESS_PACKAGE_VERSION,
    STAINLESS_RUNTIME_VERSION,
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

/// 从逗号分隔的 anthropic-beta header 中剥离指定 token，保留其它 token 和相对顺序。
///
/// 对应 sub2api 的 `stripBetaTokensWithSet`：对非白名单模型默认过滤掉
/// `context-1m-2025-08-07` 这类受控 beta，防止 Sonnet/Haiku 误开 1M 档计费。
fn strip_beta_token(beta: &str, token: &str) -> String {
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
fn matches_1m_whitelist(model_id: &str, allow_1m_models: &str) -> bool {
    let m = model_id.to_lowercase();
    allow_1m_models
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .any(|pat| m.contains(pat.to_lowercase().as_str()))
}

/// 合并必需的 beta 令牌与客户端传入的 beta 令牌。
fn merge_anthropic_beta(required: &str, incoming: &str) -> String {
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

/// 根据模型返回正确的 anthropic-beta 值。
fn beta_header_for_model(model_id: &str) -> &'static str {
    let _ = model_id;
    MESSAGE_BETA_TOKENS
}

/// 根据 endpoint 返回 Claude Code 2.1.156 的必需 beta token。
fn beta_header_for_path(path: &str, model_id: &str) -> &'static str {
    if is_event_logging_path(path)
        || path.starts_with("/api/eval/")
        || path.starts_with("/api/oauth/")
        || path.starts_with("/api/claude_cli/bootstrap")
        || path.starts_with("/api/claude_code_grove")
        || path.starts_with("/api/claude_code_penguin_mode")
    {
        OAUTH_BETA_TOKEN
    } else if path.starts_with("/v1/code/triggers") {
        CODE_TRIGGERS_BETA_TOKEN
    } else if path.starts_with("/v1/mcp_servers") {
        MCP_SERVERS_BETA_TOKEN
    } else {
        beta_header_for_model(model_id)
    }
}

/// 判断该 endpoint 是否应该发送 anthropic-beta。
fn requires_anthropic_beta(path: &str) -> bool {
    !path.starts_with("/mcp-registry/") && path != "/"
}

/// 处理所有请求的反检测改写。
pub struct Rewriter;

impl Rewriter {
    pub fn new() -> Self {
        Self
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
            // API 模式：使用按 endpoint 拆分的 Claude Code 2.1.156 header profile。
            out.insert("Accept".into(), "application/json".into());
            if requires_anthropic_beta(path) {
                out.insert(
                    "anthropic-beta".into(),
                    beta_header_for_path(path, model_id).into(),
                );
            }
            out.insert("content-type".into(), "application/json".into());
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
                out.insert("anthropic-version".into(), "2023-06-01".into());
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
                strip_beta_token(&existing_beta, "context-1m-2025-08-07")
            };
            if requires_anthropic_beta(path) {
                out.insert(
                    "anthropic-beta".into(),
                    merge_anthropic_beta(beta_header_for_path(path, model_id), &filtered_existing),
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
    ) -> Vec<u8> {
        if body.is_empty() {
            return body.to_vec();
        }

        let mut parsed: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => return body.to_vec(), // 非 JSON，直接透传
        };

        if path.starts_with("/v1/messages") {
            strip_empty_text_blocks(&mut parsed);
            self.rewrite_messages(&mut parsed, account, client_type);
        } else if is_event_logging_path(path) {
            self.rewrite_event_batch(&mut parsed, account);
        } else if path.starts_with("/api/eval/") {
            self.rewrite_growthbook_eval(&mut parsed, account);
        } else {
            self.rewrite_generic_identity(&mut parsed, account);
        }

        let mut output = serde_json::to_vec(&parsed).unwrap_or_else(|_| body.to_vec());

        let profile = device_profile(account);
        let version = normalize_version(&profile.env.version);
        if path.starts_with("/v1/messages") && account.billing_mode == BillingMode::Rewrite {
            output = compute_cch_attestation(output, version);
        }

        output
    }

    /// 处理 /v1/messages 请求体。
    fn rewrite_messages(
        &self,
        body: &mut serde_json::Value,
        account: &Account,
        client_type: ClientType,
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
            );
            scrub_git_user_in_reminders(body, &account.name);
        } else {
            // 注入模式
            let session_id = self.inject_metadata_user_id(body, &profile, account);
            if let Some(sid) = &session_id {
                if let Some(metadata) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
                    metadata.insert("_session_id".into(), serde_json::Value::String(sid.clone()));
                }
            }

            // 剥离 Claude Code 不会发送的字段
            if let Some(obj) = body.as_object_mut() {
                obj.remove("temperature");
                obj.remove("top_k");
                obj.remove("top_p");
                obj.remove("stop_sequences");
                obj.remove("tool_choice");

                // 确保 tools 字段存在
                obj.entry("tools")
                    .or_insert(serde_json::Value::Array(vec![]));

                // 确保 stream 为 true
                obj.insert("stream".into(), serde_json::Value::Bool(true));
            }

            // 剥离 system 块中的 cache_control
            strip_cache_control(body);

            // 规范化 max_tokens
            if let Some(max_tokens) = body.get("max_tokens").and_then(|v| v.as_f64()) {
                if max_tokens > 32768.0 {
                    body.as_object_mut()
                        .unwrap()
                        .insert("max_tokens".into(), serde_json::json!(16384));
                }
            }

            // 注入 Claude Code 系统提示词
            self.inject_system_prompt(body);
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
        // 确保 metadata 存在
        if body.get("metadata").is_none() {
            body.as_object_mut()
                .unwrap()
                .insert("metadata".into(), serde_json::json!({}));
        }

        // 已有 user_id，改为改写
        if body
            .get("metadata")
            .and_then(|m| m.get("user_id"))
            .is_some()
        {
            self.rewrite_metadata_user_id(body, profile);
            return None;
        }

        let request = request_profile(account, None);
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

    /// 将 Claude Code 系统提示词添加到请求体前面（仅 API 注入模式）。
    fn inject_system_prompt(&self, body: &mut serde_json::Value) {
        let banner_block = serde_json::json!({
            "type": "text",
            "text": CLAUDE_CODE_SYSTEM_PROMPT,
            "cache_control": { "type": "ephemeral" }
        });

        match body.get("system") {
            None => {
                body.as_object_mut().unwrap().insert(
                    "system".into(),
                    serde_json::Value::Array(vec![banner_block]),
                );
            }
            Some(serde_json::Value::String(sys)) => {
                if sys.starts_with(CLAUDE_CODE_SYSTEM_PROMPT) {
                    return;
                }
                let user_block = serde_json::json!({
                    "type": "text",
                    "text": sys,
                });
                body.as_object_mut().unwrap().insert(
                    "system".into(),
                    serde_json::Value::Array(vec![banner_block, user_block]),
                );
            }
            Some(serde_json::Value::Array(arr)) => {
                if let Some(first) = arr.first() {
                    if let Some(text) = first.get("text").and_then(|t| t.as_str()) {
                        if text.starts_with(CLAUDE_CODE_SYSTEM_PROMPT) {
                            return;
                        }
                    }
                }
                let mut new_arr = vec![banner_block];
                new_arr.extend(arr.iter().cloned());
                body.as_object_mut()
                    .unwrap()
                    .insert("system".into(), serde_json::Value::Array(new_arr));
            }
            _ => {}
        }
    }

    // --- 系统提示词改写（仅 CC 客户端模式）---

    fn rewrite_system_prompt(
        &self,
        body: &mut serde_json::Value,
        pe: &CanonicalPromptEnvData,
        version: &str,
        billing_mode: &BillingMode,
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
                format!("{:x}", u16::from_be_bytes(bytes))[..3].to_string()
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
            text = SHELL_REGEX
                .replace_all(&text, &format!("Shell: {}", pe.shell))
                .to_string();
            text = OS_VERSION_REGEX
                .replace_all(&text, &format!("OS Version: {}", pe.os_version))
                .to_string();
            text = WORKING_DIR_REGEX
                .replace_all(&text, &format!("${{1}}{}", pe.working_dir))
                .to_string();
            let home_prefix = if let Some(idx) = nth_index(&pe.working_dir, '/', 3) {
                &pe.working_dir[..idx + 1]
            } else {
                &pe.working_dir
            };
            text = HOME_PATH_REGEX.replace_all(&text, home_prefix).to_string();
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
    Lazy::new(|| Regex::new(r"((?:Primary )?[Ww]orking directory:\s*)/\S+").unwrap());
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
        let hash = xxhash_rust::xxh64::xxh64(&body, cch_attestation_seed(version));
        let cch = format!("{:05x}", hash & 0xFFFFF);
        // "cch=" 占 4 字节，后续 5 字节是 "00000"。
        body[pos + 4..pos + 9].copy_from_slice(cch.as_bytes());
    }
    body
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

/// 返回指定 Claude Code 版本使用的 CCH attestation seed。
fn cch_attestation_seed(version: &str) -> u64 {
    match normalize_version(version) {
        DEFAULT_CLAUDE_CODE_VERSION => CCH_ATTESTATION_SEED_2156,
        _ => CCH_ATTESTATION_SEED_LEGACY,
    }
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
                for item in arr {
                    if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                        return text.to_string();
                    }
                }
            }
            _ => {}
        }
    }
    String::new()
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

/// 移除消息和 system 中的空文本内容块。
fn strip_empty_text_blocks(body: &mut serde_json::Value) {
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
        // Handle tool_result nested content
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

/// 从注入模式 body 中获取暂存的 _session_id。
pub fn extract_session_id_from_body(body: &serde_json::Value) -> Option<String> {
    body.get("metadata")
        .and_then(|m| m.get("_session_id"))
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

/// 清理 body 中的内部 _session_id 标记。
pub fn clean_session_id_from_body(body: &mut serde_json::Value) {
    if let Some(metadata) = body.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        metadata.remove("_session_id");
    }
}

/// 判断请求来自 Claude Code 还是纯 API。
pub fn detect_client_type(user_agent: &str, body: &serde_json::Value) -> ClientType {
    let ua_lower = user_agent.to_lowercase();
    if ua_lower.starts_with("claude-code/") || ua_lower.starts_with("claude-cli/") {
        return ClientType::ClaudeCode;
    }
    if let Some(metadata) = body.get("metadata").and_then(|m| m.as_object()) {
        if metadata.contains_key("user_id") {
            return ClientType::ClaudeCode;
        }
    }
    ClientType::API
}

const CLAUDE_CODE_SYSTEM_PROMPT: &str = "You are Claude Code, Anthropic's official CLI for Claude.";

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
        cch_attestation_seed, compute_cc_version_suffix, compute_cch_attestation,
        matches_1m_whitelist, strip_beta_token, ClientType, Rewriter,
    };
    use crate::model::account::{
        Account, AccountAuthType, AccountStatus, BillingMode, CanonicalEnvData,
        CanonicalProcessData, CanonicalPromptEnvData,
    };
    use crate::service::version_profile::{
        DEFAULT_CLAUDE_CODE_BUILD_TIME, DEFAULT_CLAUDE_CODE_VERSION,
        DEFAULT_CLAUDE_CODE_VERSION_BASE, MESSAGE_BETA_TOKENS, STAINLESS_PACKAGE_VERSION,
        STAINLESS_RUNTIME_VERSION,
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

    #[test]
    fn api_messages_headers_use_2156_profile() {
        let account = test_account();
        let rewriter = Rewriter::new();
        let body = json!({"metadata": {"_session_id": "session-1"}});
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
            "claude-cli/2.1.156 (external, cli)"
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
            "claude-code/2.1.156"
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
            "claude-cli/2.1.156 (external, cli)"
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
    fn growthbook_rewrite_adds_2156_attributes() {
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
    fn cch_seed_is_versioned() {
        assert_eq!(
            cch_attestation_seed(DEFAULT_CLAUDE_CODE_VERSION),
            0x4D659218E32A3268
        );
        assert_eq!(cch_attestation_seed("2.1.81"), 0x6E52736AC806831E);
        assert_eq!(cch_attestation_seed("2.1.999"), 0x6E52736AC806831E);
    }

    #[test]
    fn cch_rewrite_uses_2156_seed() {
        let body = br#"{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.156.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[]}"#;
        let out = compute_cch_attestation(body.to_vec(), DEFAULT_CLAUDE_CODE_VERSION);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("cch=40943"));
    }

    #[test]
    fn cch_rewrite_keeps_legacy_seed_for_old_versions() {
        let body = br#"{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.81.b94; cc_entrypoint=cli; cch=00000;"}],"messages":[]}"#;
        let out = compute_cch_attestation(body.to_vec(), "2.1.81");
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("cch=afd26"));
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
