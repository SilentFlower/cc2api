use crate::error::AppError;
use crate::model::account::CanonicalEnvData;
use crate::service::version_profile::{
    OAUTH_BETA_TOKEN, claude_cli_user_agent, claude_code_user_agent, normalize_version,
    profile_for_version,
};
use crate::tlsfp::get_request_client;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub(crate) const OAUTH_TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
const OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const OAUTH_ERROR_BODY_LIMIT: usize = 4 * 1024;
const OAUTH_ERROR_CODE_LIMIT: usize = 64;
const OAUTH_SCOPES: &[&str] = &[
    "user:profile",
    "user:inference",
    "user:sessions:claude_code",
    "user:mcp_servers",
    "user:file_upload",
];

fn oauth_request_client(proxy_url: &str) -> Result<reqwest::Client, AppError> {
    get_request_client(proxy_url)
        .map_err(|_| AppError::Internal("OAuth request client unavailable".into()))
}

#[derive(Debug, Clone)]
pub struct RefreshedOAuthTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct OAuthRefreshResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    expires_in: i64,
}

#[derive(Deserialize)]
struct OAuthErrorResponse {
    #[serde(default)]
    error: String,
}

async fn read_oauth_error_body(mut response: reqwest::Response) -> Vec<u8> {
    let mut body = Vec::new();
    while body.len() < OAUTH_ERROR_BODY_LIMIT {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) | Err(_) => break,
        };
        let remaining = OAUTH_ERROR_BODY_LIMIT - body.len();
        body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    body
}

fn parse_oauth_error_code(body: &[u8]) -> Option<String> {
    let code = serde_json::from_slice::<OAuthErrorResponse>(body)
        .ok()?
        .error;
    let code = code.trim();
    if code.is_empty()
        || code.len() > OAUTH_ERROR_CODE_LIMIT
        || !code
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
    {
        return None;
    }
    Some(code.to_string())
}

fn classify_oauth_refresh_error(status: reqwest::StatusCode, body: &[u8]) -> AppError {
    let error_code = parse_oauth_error_code(body);
    let summary = match error_code.as_deref() {
        Some(code) => format!("OAuth refresh rejected: status {} error={}", status, code),
        None => format!("OAuth refresh rejected: status {}", status),
    };
    let permanent_invalid_grant = status.is_client_error()
        && status != reqwest::StatusCode::TOO_MANY_REQUESTS
        && error_code.as_deref() == Some("invalid_grant");
    if permanent_invalid_grant {
        AppError::PermanentCredential(summary)
    } else {
        AppError::Internal(summary)
    }
}

/// 通过轻量级 API 调用验证 Setup Token。
pub struct TokenTester;

impl TokenTester {
    /// 创建 Setup Token 验证器。
    ///
    /// @return 返回无状态的 TokenTester 实例。
    pub fn new() -> Self {
        Self
    }

    /// 通过发送最小消息请求验证 Setup Token 有效性。
    pub async fn test_token(
        &self,
        token: &str,
        proxy_url: &str,
        canonical_env: &Value,
    ) -> Result<(), AppError> {
        let client = oauth_request_client(proxy_url)?;
        let request = build_test_token_request(&client, token, canonical_env)?;

        let resp = client
            .execute(request)
            .await
            .map_err(|e| AppError::Internal(format!("request failed: {:?}", e)))?;

        if resp.status() != 200 {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(AppError::Internal(format!(
                "token test failed: status {} {}",
                status, text
            )));
        }
        Ok(())
    }
}

fn build_test_token_request(
    client: &reqwest::Client,
    token: &str,
    canonical_env: &Value,
) -> Result<reqwest::Request, AppError> {
    let env: CanonicalEnvData = serde_json::from_value(canonical_env.clone()).unwrap_or_default();
    let version = normalize_version(&env.version);
    let version_profile = profile_for_version(version);
    let stainless_os = match env.platform.as_str() {
        "darwin" => "Mac OS X",
        "win32" => "Windows",
        _ => "Linux",
    };
    let body = serde_json::json!({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1,
        "messages": [{"role": "user", "content": "hi"}]
    });

    client
        .post("https://api.anthropic.com/v1/messages?beta=true")
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .header("anthropic-version", "2023-06-01")
        .header(
            "anthropic-beta",
            version_profile.request.haiku_probe_beta_tokens,
        )
        .header("anthropic-dangerous-direct-browser-access", "true")
        .header("User-Agent", claude_cli_user_agent(version))
        .header("x-app", "cli")
        .header("accept-encoding", "gzip, deflate, br, zstd")
        .header("X-Stainless-Lang", "js")
        .header(
            "X-Stainless-Package-Version",
            version_profile.identity.stainless_package_version,
        )
        .header("X-Stainless-OS", stainless_os)
        .header("X-Stainless-Arch", &env.arch)
        .header("X-Stainless-Runtime", "node")
        .header(
            "X-Stainless-Runtime-Version",
            version_profile.identity.stainless_runtime_version,
        )
        .header("X-Stainless-Retry-Count", "0")
        .header("X-Stainless-Timeout", "600")
        .json(&body)
        .build()
        .map_err(|e| AppError::Internal(format!("request build failed: {:?}", e)))
}

/// 使用 refresh token 刷新 OAuth access token。
pub async fn refresh_oauth_token(
    refresh_token: &str,
    proxy_url: &str,
) -> Result<RefreshedOAuthTokens, AppError> {
    refresh_oauth_token_at(refresh_token, proxy_url, OAUTH_TOKEN_URL).await
}

/// 使用指定端点刷新 OAuth access token。
///
/// @param refresh_token OAuth refresh token。
/// @param proxy_url 账号代理地址，空字符串表示直连。
/// @param token_url OAuth token 端点。
/// @return 返回刷新后的 access token、refresh token 和过期时间。
pub(crate) async fn refresh_oauth_token_at(
    refresh_token: &str,
    proxy_url: &str,
    token_url: &str,
) -> Result<RefreshedOAuthTokens, AppError> {
    let client = oauth_request_client(proxy_url)?;
    let body = serde_json::json!({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": OAUTH_CLIENT_ID,
        "scope": OAUTH_SCOPES.join(" "),
    });

    let resp = client
        .post(token_url)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("oauth refresh request failed: {}", e)))?;

    if resp.status() != 200 {
        let status = resp.status();
        let body = read_oauth_error_body(resp).await;
        return Err(classify_oauth_refresh_error(status, &body));
    }

    let data: OAuthRefreshResponse = resp
        .json()
        .await
        .map_err(|e| AppError::Internal(format!("oauth refresh parse failed: {}", e)))?;

    let expires_in = if data.expires_in > 0 {
        data.expires_in
    } else {
        3600
    };
    let expires_at = Utc::now() + chrono::Duration::seconds(expires_in);

    Ok(RefreshedOAuthTokens {
        access_token: data.access_token,
        refresh_token: if data.refresh_token.is_empty() {
            refresh_token.to_string()
        } else {
            data.refresh_token
        },
        expires_at,
    })
}

/// 从 Anthropic OAuth API 获取账号用量数据。
pub async fn fetch_usage(token: &str, proxy_url: &str) -> Result<Value, AppError> {
    let client = oauth_request_client(proxy_url)?;

    let resp = client
        .get("https://api.anthropic.com/api/oauth/usage")
        .header("Authorization", format!("Bearer {}", token))
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("anthropic-beta", OAUTH_BETA_TOKEN)
        .header("User-Agent", claude_code_user_agent(""))
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("usage request failed: {}", e)))?;

    let status = resp.status();
    if status != 200 {
        let text = resp.text().await.unwrap_or_default();
        return Err(match status.as_u16() {
            401 | 403 => AppError::BadRequest(format!(
                "usage fetch failed: status {} — token may be expired or invalid: {}",
                status, text
            )),
            429 => AppError::TooManyRequests(format!(
                "usage endpoint rate limited (429), try again later: {}",
                text
            )),
            _ => AppError::Internal(format!("usage fetch failed: status {} {}", status, text)),
        });
    }

    let data: Value = resp
        .json()
        .await
        .map_err(|e| AppError::Internal(format!("usage parse failed: {}", e)))?;
    Ok(normalize_usage_response(data))
}

/// 规范化 OAuth usage 返回，补齐前端稳定展示字段。
fn normalize_usage_response(mut data: Value) -> Value {
    let seven_day_fable = scoped_weekly_usage_window(&data, "Fable");
    if let Some(obj) = data.as_object_mut() {
        if !matches!(obj.get("seven_day_fable"), Some(Value::Object(_))) {
            if let Some(window) = seven_day_fable {
                obj.insert("seven_day_fable".into(), window);
            }
        }
    }
    data
}

/// 从新版 `limits` scoped 结构里提取指定模型的周用量窗口。
fn scoped_weekly_usage_window(usage: &Value, model_name: &str) -> Option<Value> {
    let limits = usage.get("limits")?.as_array()?;
    let expected = model_name.to_ascii_lowercase();
    for item in limits {
        let Some(model) = item.get("scope").and_then(|scope| scope.get("model")) else {
            continue;
        };
        let display_name = model.get("display_name").and_then(Value::as_str);
        let model_id = model.get("id").and_then(Value::as_str);
        let matches_name = display_name
            .map(|value| value.eq_ignore_ascii_case(model_name))
            .unwrap_or(false);
        let matches_id = model_id
            .map(|value| value.to_ascii_lowercase().contains(&expected))
            .unwrap_or(false);
        if !matches_name && !matches_id {
            continue;
        }
        let kind = item.get("kind").and_then(Value::as_str);
        let group = item.get("group").and_then(Value::as_str);
        if kind != Some("weekly_scoped") && group != Some("weekly") {
            continue;
        }
        let Some(percent) = item.get("percent").and_then(Value::as_f64) else {
            continue;
        };
        let reset_at = item.get("resets_at").and_then(Value::as_str);
        let mut window = Map::new();
        window.insert("utilization".into(), json!(percent));
        window.insert(
            "resets_at".into(),
            reset_at.map(|value| json!(value)).unwrap_or(Value::Null),
        );
        return Some(Value::Object(window));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request_header<'a>(request: &'a reqwest::Request, name: &str) -> &'a str {
        request
            .headers()
            .get(name)
            .unwrap_or_else(|| panic!("缺少请求头: {}", name))
            .to_str()
            .expect("请求头必须是有效字符串")
    }

    fn test_canonical_env(version: &str) -> Value {
        json!({
            "platform": "linux",
            "platform_raw": "linux",
            "arch": "x64",
            "node_version": "v26.3.0",
            "terminal": "",
            "package_managers": "",
            "runtimes": "node",
            "version": version,
            "version_base": version,
            "build_time": "",
            "deployment_environment": "local",
            "vcs": "git"
        })
    }

    #[test]
    fn token_test_request_uses_selected_version_profile_headers() {
        let client = get_request_client("").expect("构造无代理请求客户端");
        let current =
            build_test_token_request(&client, "test-token", &test_canonical_env("2.1.260"))
                .expect("构造当前画像请求");
        let previous =
            build_test_token_request(&client, "test-token", &test_canonical_env("2.1.257"))
                .expect("构造上一画像请求");
        let rollback =
            build_test_token_request(&client, "test-token", &test_canonical_env("2.1.220"))
                .expect("构造回滚画像请求");

        assert_eq!(
            request_header(&current, "user-agent"),
            "claude-cli/2.1.260 (external, cli)"
        );
        assert_eq!(
            request_header(&current, "x-stainless-package-version"),
            "0.112.1"
        );
        assert_eq!(
            request_header(&current, "x-stainless-runtime-version"),
            "v26.3.0"
        );
        assert_eq!(
            request_header(&current, "anthropic-beta"),
            crate::service::version_profile::HAIKU_PROBE_BETA_TOKENS
        );

        assert_eq!(
            request_header(&previous, "user-agent"),
            "claude-cli/2.1.257 (external, cli)"
        );
        assert_eq!(
            request_header(&previous, "x-stainless-package-version"),
            "0.112.1"
        );
        assert_eq!(
            request_header(&previous, "x-stainless-runtime-version"),
            "v26.3.0"
        );
        assert_eq!(
            request_header(&previous, "anthropic-beta"),
            crate::service::version_profile::HAIKU_PROBE_BETA_TOKENS
        );

        assert_eq!(
            request_header(&rollback, "user-agent"),
            "claude-cli/2.1.220 (external, cli)"
        );
        assert_eq!(
            request_header(&rollback, "x-stainless-package-version"),
            "0.94.0"
        );
        assert_eq!(
            request_header(&rollback, "x-stainless-runtime-version"),
            "v26.3.0"
        );
        assert_eq!(
            request_header(&rollback, "anthropic-beta"),
            crate::service::version_profile::HAIKU_PROBE_BETA_TOKENS
        );
    }

    #[test]
    fn invalid_grant_is_classified_as_permanent_without_description() {
        let error = classify_oauth_refresh_error(
            reqwest::StatusCode::BAD_REQUEST,
            br#"{"error":"invalid_grant","error_description":"fixture secret detail"}"#,
        );

        assert!(matches!(&error, AppError::PermanentCredential(_)));
        let message = error.to_string();
        assert!(message.contains("status 400"));
        assert!(message.contains("invalid_grant"));
        assert!(!message.contains("fixture secret detail"));
    }

    #[test]
    fn unclassified_oauth_error_remains_transient_and_sanitized() {
        let error = classify_oauth_refresh_error(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            br#"{"error":"temporarily_unavailable","error_description":"fixture response body"}"#,
        );

        assert!(matches!(&error, AppError::Internal(_)));
        let message = error.to_string();
        assert!(message.contains("status 429"));
        assert!(message.contains("temporarily_unavailable"));
        assert!(!message.contains("fixture response body"));
    }

    #[test]
    fn transient_http_statuses_override_invalid_grant_code() {
        for status in [
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let error = classify_oauth_refresh_error(
                status,
                br#"{"error":"invalid_grant","error_description":"fixture transient detail"}"#,
            );

            assert!(matches!(&error, AppError::Internal(_)));
            let message = error.to_string();
            assert!(message.contains("invalid_grant"));
            assert!(!message.contains("fixture transient detail"));
        }
    }

    #[test]
    fn normalize_usage_response_adds_fable_from_scoped_limit() {
        let data = json!({
            "five_hour": {"utilization": 10, "resets_at": "2026-07-02T05:20:00Z"},
            "limits": [
                {
                    "kind": "session",
                    "group": "session",
                    "percent": 10,
                    "resets_at": "2026-07-02T05:20:00Z",
                    "scope": null
                },
                {
                    "kind": "weekly_all",
                    "group": "weekly",
                    "percent": 2,
                    "resets_at": "2026-07-07T06:00:00Z",
                    "scope": null
                },
                {
                    "kind": "weekly_scoped",
                    "group": "weekly",
                    "percent": 37.5,
                    "resets_at": "2026-07-07T06:00:00Z",
                    "scope": {
                        "model": {
                            "id": null,
                            "display_name": "Fable"
                        },
                        "surface": null
                    }
                }
            ]
        });

        let normalized = normalize_usage_response(data);

        assert_eq!(normalized["seven_day_fable"]["utilization"], json!(37.5));
        assert_eq!(
            normalized["seven_day_fable"]["resets_at"],
            json!("2026-07-07T06:00:00Z")
        );
    }

    #[test]
    fn normalize_usage_response_keeps_existing_fable_window() {
        let data = json!({
            "seven_day_fable": {"utilization": 12, "resets_at": null},
            "limits": [
                {
                    "kind": "weekly_scoped",
                    "group": "weekly",
                    "percent": 90,
                    "resets_at": null,
                    "scope": {
                        "model": {
                            "id": null,
                            "display_name": "Fable"
                        }
                    }
                }
            ]
        });

        let normalized = normalize_usage_response(data);

        assert_eq!(normalized["seven_day_fable"]["utilization"], json!(12));
    }

    #[test]
    fn normalize_usage_response_ignores_other_scoped_limits() {
        let data = json!({
            "limits": [
                {
                    "kind": "weekly_scoped",
                    "group": "weekly",
                    "percent": 80,
                    "resets_at": null,
                    "scope": {
                        "model": {
                            "id": null,
                            "display_name": "Sonnet"
                        }
                    }
                }
            ]
        });

        let normalized = normalize_usage_response(data);

        assert!(normalized.get("seven_day_fable").is_none());
    }
}
