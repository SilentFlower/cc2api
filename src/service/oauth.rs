use crate::error::AppError;
use crate::model::account::CanonicalEnvData;
use crate::service::version_profile::{
    MESSAGE_BETA_TOKENS, OAUTH_BETA_TOKEN, STAINLESS_PACKAGE_VERSION, STAINLESS_RUNTIME_VERSION,
    claude_cli_user_agent, claude_code_user_agent, normalize_version,
};
use crate::tlsfp::get_request_client;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value, json};

const OAUTH_TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
const OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const OAUTH_SCOPES: &[&str] = &[
    "user:profile",
    "user:inference",
    "user:sessions:claude_code",
    "user:mcp_servers",
    "user:file_upload",
];

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

/// 通过轻量级 API 调用验证 Setup Token。
pub struct TokenTester;

impl TokenTester {
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
        let env: CanonicalEnvData =
            serde_json::from_value(canonical_env.clone()).unwrap_or_default();
        let version = normalize_version(&env.version);
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

        let client = get_request_client(proxy_url);

        let resp = client
            .post("https://api.anthropic.com/v1/messages?beta=true")
            .header("Authorization", format!("Bearer {}", token))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("anthropic-version", "2023-06-01")
            .header("anthropic-beta", MESSAGE_BETA_TOKENS)
            .header("anthropic-dangerous-direct-browser-access", "true")
            .header("User-Agent", claude_cli_user_agent(version))
            .header("x-app", "cli")
            .header("accept-encoding", "gzip, deflate, br, zstd")
            .header("X-Stainless-Lang", "js")
            .header("X-Stainless-Package-Version", STAINLESS_PACKAGE_VERSION)
            .header("X-Stainless-OS", stainless_os)
            .header("X-Stainless-Arch", &env.arch)
            .header("X-Stainless-Runtime", "node")
            .header("X-Stainless-Runtime-Version", STAINLESS_RUNTIME_VERSION)
            .header("X-Stainless-Retry-Count", "0")
            .header("X-Stainless-Timeout", "600")
            .json(&body)
            .send()
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

/// 使用 refresh token 刷新 OAuth access token。
pub async fn refresh_oauth_token(
    refresh_token: &str,
    proxy_url: &str,
) -> Result<RefreshedOAuthTokens, AppError> {
    let client = get_request_client(proxy_url);
    let body = serde_json::json!({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": OAUTH_CLIENT_ID,
        "scope": OAUTH_SCOPES.join(" "),
    });

    let resp = client
        .post(OAUTH_TOKEN_URL)
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("oauth refresh request failed: {}", e)))?;

    if resp.status() != 200 {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(AppError::Internal(format!(
            "oauth refresh failed: status {} {}",
            status, text
        )));
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
    let client = get_request_client(proxy_url);

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
