use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::error::AppError;

/// 默认允许的 Claude Code / Claude CLI 版本范围。
pub const DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS: &str =
    crate::service::version_profile::DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS;
/// 默认禁止的 Claude Code / Claude CLI 版本范围。
pub const DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS: &str = "";
/// 默认允许的非 Claude Code 客户端 User-Agent。
pub const DEFAULT_ALLOWED_USER_AGENTS: &str = "AI-Hub-Monitor*\npython-httpx*";

#[derive(Debug, Clone, Default)]
pub struct AccessPolicy {
    version_rules: Vec<VersionRule>,
    blocked_version_rules: Vec<VersionRule>,
    ua_patterns: Vec<String>,
    raw_versions: String,
    raw_blocked_versions: String,
}

#[derive(Debug, Clone)]
enum VersionRule {
    Exact(Vec<u32>),
    Wildcard(Vec<u32>),
    Range(Vec<u32>, Vec<u32>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessPolicyRejection {
    pub setting: &'static str,
    pub reason: String,
}

impl AccessPolicy {
    /// 从 settings 字符串构造访问策略。
    ///
    /// @param allowed_versions 允许的 Claude Code / Claude CLI 版本范围配置。
    /// @param blocked_versions 禁止的 Claude Code / Claude CLI 版本范围配置。
    /// @param allowed_user_agents 允许的非 Claude Code 客户端 User-Agent pattern 配置。
    /// @return 解析成功返回访问策略，格式非法时返回业务错误。
    pub fn parse(
        allowed_versions: &str,
        blocked_versions: &str,
        allowed_user_agents: &str,
    ) -> Result<Self, AppError> {
        Ok(Self {
            version_rules: parse_version_rules("allowed_claude_code_versions", allowed_versions)?,
            blocked_version_rules: parse_version_rules(
                "blocked_claude_code_versions",
                blocked_versions,
            )?,
            ua_patterns: parse_ua_patterns(allowed_user_agents)?,
            raw_versions: allowed_versions.trim().to_string(),
            raw_blocked_versions: blocked_versions.trim().to_string(),
        })
    }

    /// 校验原始客户端 User-Agent 是否允许访问。
    ///
    /// @param user_agent 请求头中的原始 User-Agent。
    /// @return 允许访问返回 `Ok(())`，否则返回可序列化的拒绝原因。
    pub fn check_user_agent(&self, user_agent: &str) -> Result<(), AccessPolicyRejection> {
        if let Some(version) = extract_claude_code_version(user_agent) {
            let Some(version) = version else {
                if self.version_rules.is_empty() && self.blocked_version_rules.is_empty() {
                    return Ok(());
                }
                return Err(AccessPolicyRejection {
                    setting: self.missing_version_setting(),
                    reason: "Claude Code User-Agent 缺少版本号".to_string(),
                });
            };
            if version_rules_match(&self.blocked_version_rules, version) {
                return Err(AccessPolicyRejection {
                    setting: "blocked_claude_code_versions",
                    reason: format!(
                        "Claude Code 版本 '{}' 命中禁止范围；禁止范围：{}",
                        version, self.raw_blocked_versions
                    ),
                });
            }
            if self.version_rules.is_empty() {
                return Ok(());
            }
            if version_rules_match(&self.version_rules, version) {
                return Ok(());
            }
            return Err(AccessPolicyRejection {
                setting: "allowed_claude_code_versions",
                reason: format!(
                    "Claude Code 版本 '{}' 不在允许范围内；允许范围：{}",
                    version, self.raw_versions
                ),
            });
        }

        if self.ua_patterns.is_empty() {
            return Ok(());
        }
        if user_agent.is_empty() {
            return Err(AccessPolicyRejection {
                setting: "allowed_user_agents",
                reason: "访问策略要求请求携带 User-Agent".to_string(),
            });
        }
        if self
            .ua_patterns
            .iter()
            .any(|pattern| wildcard_match(pattern, user_agent))
        {
            return Ok(());
        }

        Err(AccessPolicyRejection {
            setting: "allowed_user_agents",
            reason: format!("当前 User-Agent '{}' 不允许访问", user_agent),
        })
    }

    fn missing_version_setting(&self) -> &'static str {
        if !self.blocked_version_rules.is_empty() {
            "blocked_claude_code_versions"
        } else {
            "allowed_claude_code_versions"
        }
    }
}

/// 校验 Claude Code 版本范围配置格式。
///
/// @param raw 原始 settings 字符串。
/// @return 格式合法返回 `Ok(())`。
pub fn validate_claude_code_versions(raw: &str) -> Result<(), AppError> {
    parse_version_rules("allowed_claude_code_versions", raw).map(|_| ())
}

/// 校验禁止的 Claude Code 版本范围配置格式。
///
/// @param raw 原始 settings 字符串。
/// @return 格式合法返回 `Ok(())`。
pub fn validate_blocked_claude_code_versions(raw: &str) -> Result<(), AppError> {
    parse_version_rules("blocked_claude_code_versions", raw).map(|_| ())
}

/// 校验 User-Agent pattern 配置格式。
///
/// @param raw 原始 settings 字符串。
/// @return 格式合法返回 `Ok(())`。
pub fn validate_user_agent_patterns(raw: &str) -> Result<(), AppError> {
    parse_ua_patterns(raw).map(|_| ())
}

/// 构造访问策略拒绝响应。
///
/// @param rejection 拒绝原因。
/// @return HTTP 403 JSON 响应。
pub fn access_policy_error_response(rejection: &AccessPolicyRejection) -> Response {
    (
        StatusCode::FORBIDDEN,
        axum::Json(json!({
            "type": "error",
            "error": {
                "type": "invalid_request_error",
                "message": rejection.reason,
                "code": rejection.setting,
            },
            "setting": rejection.setting,
            "reason": rejection.reason,
        })),
    )
        .into_response()
}

fn parse_list(raw: &str) -> Vec<String> {
    raw.split([',', '\n', '\r'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_ua_patterns(raw: &str) -> Result<Vec<String>, AppError> {
    let patterns = parse_list(raw);
    for pattern in &patterns {
        if !pattern.chars().all(|c| c.is_ascii_graphic() || c == ' ') {
            return Err(AppError::BadRequest(format!(
                "'allowed_user_agents' 包含非法字符: {}",
                pattern
            )));
        }
    }
    Ok(patterns)
}

fn parse_version_rules(setting: &'static str, raw: &str) -> Result<Vec<VersionRule>, AppError> {
    let mut rules = Vec::new();
    for item in parse_list(raw) {
        let rule = if let Some((start, end)) = item.split_once('-') {
            let start = parse_version_parts(setting, start)?;
            let end = parse_version_parts(setting, end)?;
            if compare_versions(&start, &end).is_gt() {
                return Err(AppError::BadRequest(format!(
                    "'{}' 区间起点不能大于终点: {}",
                    setting, item
                )));
            }
            VersionRule::Range(start, end)
        } else if let Some(prefix) = item.strip_suffix(".*") {
            VersionRule::Wildcard(parse_version_parts(setting, prefix)?)
        } else {
            VersionRule::Exact(parse_version_parts(setting, &item)?)
        };
        rules.push(rule);
    }
    Ok(rules)
}

fn parse_version_parts(setting: &'static str, raw: &str) -> Result<Vec<u32>, AppError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(format!("'{}' 包含空版本号", setting)));
    }

    let mut parts = Vec::new();
    for part in trimmed.split('.') {
        if part.is_empty() || !part.chars().all(|c| c.is_ascii_digit()) {
            return Err(AppError::BadRequest(format!(
                "'{}' 包含非法版本号: {}",
                setting, raw
            )));
        }
        parts.push(part.parse::<u32>().map_err(|_| {
            AppError::BadRequest(format!("'{}' 包含过大的版本号: {}", setting, raw))
        })?);
    }
    Ok(parts)
}

fn extract_claude_code_version(user_agent: &str) -> Option<Option<&str>> {
    let lower = user_agent.to_ascii_lowercase();
    let prefix = if lower.starts_with("claude-code/") {
        "claude-code/"
    } else if lower.starts_with("claude-cli/") {
        "claude-cli/"
    } else {
        return None;
    };
    Some(
        user_agent[prefix.len()..]
            .split_whitespace()
            .next()
            .filter(|s| !s.is_empty()),
    )
}

fn version_rules_match(rules: &[VersionRule], version: &str) -> bool {
    let Ok(parts) = parse_version_parts("allowed_claude_code_versions", version) else {
        return false;
    };
    rules.iter().any(|rule| match rule {
        VersionRule::Exact(exact) => compare_versions(&parts, exact).is_eq(),
        VersionRule::Wildcard(prefix) => version_prefix_matches(&parts, prefix),
        VersionRule::Range(start, end) => {
            !compare_versions(&parts, start).is_lt() && !compare_versions(&parts, end).is_gt()
        }
    })
}

fn version_prefix_matches(version: &[u32], prefix: &[u32]) -> bool {
    version.len() >= prefix.len()
        && version
            .iter()
            .zip(prefix.iter())
            .all(|(left, right)| left == right)
}

fn compare_versions(left: &[u32], right: &[u32]) -> std::cmp::Ordering {
    let len = left.len().max(right.len());
    for idx in 0..len {
        let l = left.get(idx).copied().unwrap_or(0);
        let r = right.get(idx).copied().unwrap_or(0);
        match l.cmp(&r) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
    }
    std::cmp::Ordering::Equal
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }

    let mut rest = value;
    if let Some(first) = parts.first() {
        if !first.is_empty() {
            let Some(stripped) = rest.strip_prefix(first) else {
                return false;
            };
            rest = stripped;
        }
    }

    for part in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
        if part.is_empty() {
            continue;
        }
        let Some(idx) = rest.find(part) else {
            return false;
        };
        rest = &rest[idx + part.len()..];
    }

    if let Some(last) = parts.last() {
        last.is_empty() || rest.ends_with(last)
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccessPolicy, AccessPolicyRejection, DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS,
        DEFAULT_ALLOWED_USER_AGENTS, access_policy_error_response,
        validate_blocked_claude_code_versions, validate_claude_code_versions,
    };
    use axum::http::StatusCode;
    use serde_json::Value;

    #[test]
    fn default_policy_allows_configured_claude_code_range() {
        let policy = AccessPolicy::parse(
            DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS,
            "",
            DEFAULT_ALLOWED_USER_AGENTS,
        )
        .unwrap();

        assert!(policy.check_user_agent("claude-code/2.1.89").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.156").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.169").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.172").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.173").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.185").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.187").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.195").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.197").is_ok());
        assert!(
            policy
                .check_user_agent("claude-cli/2.1.120 (external, cli)")
                .is_ok()
        );
        assert!(policy.check_user_agent("claude-code/2.1.88").is_err());
        assert!(policy.check_user_agent("claude-code/2.1.198").is_err());
        assert!(policy.check_user_agent("claude-code/").is_err());
        assert!(policy.check_user_agent("AI-Hub-Monitor/1.0.0").is_ok());
        assert!(policy.check_user_agent("python-httpx/0.28.1").is_ok());
    }

    #[test]
    fn ua_patterns_only_apply_to_non_claude_code_clients() {
        let policy = AccessPolicy::parse(
            "2.1.89-2.1.156",
            "",
            "AI-Hub-Monitor*,python-httpx*,MyClient/1.*",
        )
        .unwrap();

        assert!(policy.check_user_agent("AI-Hub-Monitor").is_ok());
        assert!(policy.check_user_agent("AI-Hub-Monitor/1.0.0").is_ok());
        assert!(policy.check_user_agent("AI-Hub-Monitor 1.0.0").is_ok());
        assert!(policy.check_user_agent("python-httpx").is_ok());
        assert!(policy.check_user_agent("python-httpx/0.28.1").is_ok());
        assert!(policy.check_user_agent("MyClient/1.2").is_ok());
        assert!(policy.check_user_agent("MyClient/2.0").is_err());
        assert!(policy.check_user_agent("curl/8.0").is_err());
    }

    #[test]
    fn empty_policy_keeps_backward_compatibility() {
        let policy = AccessPolicy::parse("", "", "").unwrap();

        assert!(policy.check_user_agent("").is_ok());
        assert!(policy.check_user_agent("curl/8.0").is_ok());
        assert!(policy.check_user_agent("claude-code/not-a-version").is_ok());
    }

    #[test]
    fn wildcard_version_rules_match_numeric_prefix() {
        let policy = AccessPolicy::parse("2.1.*,2.2.1", "", "").unwrap();

        assert!(policy.check_user_agent("claude-code/2.1.9").is_ok());
        assert!(policy.check_user_agent("claude-code/2.1.156").is_ok());
        assert!(policy.check_user_agent("claude-code/2.2.1").is_ok());
        assert!(policy.check_user_agent("claude-code/2.2.2").is_err());
    }

    #[test]
    fn claude_code_version_rejection_keeps_allowed_range() {
        let policy = AccessPolicy::parse("2.1.89-2.1.156", "", "").unwrap();
        let rejection = policy.check_user_agent("claude-code/2.1.37").unwrap_err();

        assert_eq!(rejection.setting, "allowed_claude_code_versions");
        assert_eq!(
            rejection.reason,
            "Claude Code 版本 '2.1.37' 不在允许范围内；允许范围：2.1.89-2.1.156"
        );
    }

    #[test]
    fn version_range_validation_rejects_reversed_range() {
        assert!(validate_claude_code_versions("2.1.156-2.1.89").is_err());
    }

    #[test]
    fn blocked_versions_reject_exact_range_and_wildcard() {
        let exact = AccessPolicy::parse("2.1.89-2.1.187", "2.1.156", "").unwrap();
        let range = AccessPolicy::parse("2.1.89-2.1.187", "2.1.170-2.1.173", "").unwrap();
        let wildcard = AccessPolicy::parse("2.1.89-2.2.9", "2.2.*", "").unwrap();

        assert_eq!(
            exact
                .check_user_agent("claude-code/2.1.156")
                .unwrap_err()
                .setting,
            "blocked_claude_code_versions"
        );
        assert_eq!(
            range
                .check_user_agent("claude-cli/2.1.172 (external, cli)")
                .unwrap_err()
                .setting,
            "blocked_claude_code_versions"
        );
        assert_eq!(
            wildcard
                .check_user_agent("claude-code/2.2.1")
                .unwrap_err()
                .setting,
            "blocked_claude_code_versions"
        );
        assert!(exact.check_user_agent("claude-code/2.1.157").is_ok());
    }

    #[test]
    fn blocked_versions_take_priority_over_allowed_versions() {
        let policy = AccessPolicy::parse("2.1.89-2.1.187", "2.1.187", "").unwrap();
        let rejection = policy.check_user_agent("claude-code/2.1.187").unwrap_err();

        assert_eq!(rejection.setting, "blocked_claude_code_versions");
        assert_eq!(
            rejection.reason,
            "Claude Code 版本 '2.1.187' 命中禁止范围；禁止范围：2.1.187"
        );
    }

    #[test]
    fn blocked_versions_apply_when_allowed_versions_are_empty() {
        let policy = AccessPolicy::parse("", "2.1.187", "").unwrap();

        assert!(policy.check_user_agent("claude-code/2.1.186").is_ok());
        assert_eq!(
            policy
                .check_user_agent("claude-code/2.1.187")
                .unwrap_err()
                .setting,
            "blocked_claude_code_versions"
        );
        assert!(policy.check_user_agent("claude-code/not-a-version").is_ok());
    }

    #[test]
    fn blocked_versions_do_not_apply_to_non_claude_user_agents() {
        let policy = AccessPolicy::parse("", "2.1.*", "curl/*").unwrap();

        assert!(policy.check_user_agent("curl/8.0").is_ok());
        assert!(policy.check_user_agent("python-httpx/0.28.1").is_err());
    }

    #[test]
    fn blocked_version_validation_reports_blocked_setting() {
        let err = validate_blocked_claude_code_versions("2.1.156-2.1.89")
            .expect_err("reversed range should fail");

        assert!(err.to_string().contains("'blocked_claude_code_versions'"));
    }

    #[tokio::test]
    async fn access_policy_error_response_uses_standard_error_object() {
        let rejection = AccessPolicyRejection {
            setting: "allowed_claude_code_versions",
            reason: "Claude Code 版本 '2.1.37' 不在允许范围内；允许范围：2.1.89-2.1.156"
                .to_string(),
        };

        let response = access_policy_error_response(&rejection);
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(value["type"], "error");
        assert!(value["error"].is_object());
        assert_eq!(value["error"]["type"], "invalid_request_error");
        assert_eq!(value["error"]["message"], rejection.reason);
        assert_eq!(value["error"]["code"], rejection.setting);
        assert_eq!(value["setting"], rejection.setting);
        assert_eq!(value["reason"], rejection.reason);
    }

    #[tokio::test]
    async fn rejected_non_claude_user_agent_response_hides_allowed_patterns() {
        let policy =
            AccessPolicy::parse("", "", "AI-Hub-Monitor*,python-httpx*,PrivateClient/1.*").unwrap();
        let rejection = policy.check_user_agent("curl/8.0").unwrap_err();

        assert_eq!(rejection.setting, "allowed_user_agents");
        assert_eq!(rejection.reason, "当前 User-Agent 'curl/8.0' 不允许访问");

        let response = access_policy_error_response(&rejection);
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let body_text = String::from_utf8(body.to_vec()).unwrap();
        let value: Value = serde_json::from_str(&body_text).unwrap();

        assert_eq!(
            value["error"]["message"],
            "当前 User-Agent 'curl/8.0' 不允许访问"
        );
        assert_eq!(value["error"]["code"], "allowed_user_agents");
        assert_eq!(value["setting"], "allowed_user_agents");
        assert_eq!(value["reason"], "当前 User-Agent 'curl/8.0' 不允许访问");
        assert!(body_text.contains("curl/8.0"));
        assert!(!body_text.contains("AI-Hub-Monitor"));
        assert!(!body_text.contains("python-httpx"));
        assert!(!body_text.contains("PrivateClient"));
        assert!(!body_text.contains("允许规则"));
    }
}
