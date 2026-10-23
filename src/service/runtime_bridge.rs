//! v3 管理桥：管理员交换短会话，浏览器不接触调度器服务秘密。

use axum::body::{Body, Bytes};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{OriginalUri, WebSocketUpgrade};
use axum::http::{HeaderMap, Method, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use reqwest::redirect::Policy;
use std::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as UpstreamMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::error::AppError;
use crate::handler::router::AppState;
use crate::service::runtime_lease::RuntimeLease;

/// 配置事务持有门禁时收口对应账号实例，阻止已解析的旧请求跨过修改。
/// @param account_id 修改的账号 ID。
/// @param lease 当前配置事务的门禁证明。
/// @return 空闲实例已停止；忙或未知时拒绝修改。
pub async fn quiesce(account_id: i64, lease: &RuntimeLease) -> Result<(), AppError> {
    let (base, token, client) = config()?;
    let response = client
        .post(format!("{base}/internal/v3/quiesce/{account_id}"))
        .bearer_auth(token)
        .json(&serde_json::json!({"owner": lease.owner, "generation": lease.generation}))
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("account maintenance unavailable".into()))?;
    if !response.status().is_success() {
        return Err(AppError::BadRequest(
            "account runtime is busy or requires reconciliation".into(),
        ));
    }
    Ok(())
}

fn config() -> Result<(String, String, reqwest::Client), AppError> {
    let base = std::env::var("V3_RUNTIME_URL").unwrap_or_default();
    let token = std::env::var("V3_INTERNAL_TOKEN").unwrap_or_default();
    if base.is_empty() || token.len() < 32 {
        return Err(AppError::ServiceUnavailable(
            "v3 management is not configured".into(),
        ));
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .build()
        .map_err(|_| AppError::ServiceUnavailable("v3 management unavailable".into()))?;
    Ok((base.trim_end_matches('/').into(), token, client))
}

fn cookie(headers: &HeaderMap) -> &str {
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
}

fn check_origin(headers: &HeaderMap) -> Result<(), AppError> {
    if let Some(origin) = headers.get(header::ORIGIN) {
        let origin = origin
            .to_str()
            .ok()
            .and_then(|value| reqwest::Url::parse(value).ok())
            .ok_or(AppError::Unauthorized)?;
        let host = headers
            .get(header::HOST)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| reqwest::Url::parse(&format!("{}://{value}", origin.scheme())).ok())
            .ok_or(AppError::Unauthorized)?;
        if origin.origin() != host.origin() {
            return Err(AppError::Unauthorized);
        }
    }
    Ok(())
}

async fn authenticated(headers: &HeaderMap) -> Result<(), AppError> {
    let (base, token, client) = config()?;
    check_origin(headers)?;
    let response = client
        .get(format!("{base}/internal/v3/session/verify"))
        .bearer_auth(token)
        .header(header::COOKIE, cookie(headers))
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("v3 session unavailable".into()))?;
    if !response.status().is_success() {
        return Err(AppError::Unauthorized);
    }
    Ok(())
}

fn response(upstream: reqwest::Response) -> Response {
    let status = upstream.status();
    let headers = upstream.headers().clone();
    let mut output = Response::new(Body::from_stream(upstream.bytes_stream()));
    *output.status_mut() = status;
    *output.headers_mut() = headers;
    for key in ["connection", "transfer-encoding", "upgrade", "keep-alive"] {
        output.headers_mut().remove(key);
    }
    output
}

/// 已经通过 admin_auth 的管理员换取独立 v3 浏览器会话。
/// @return HttpOnly cookie；响应不包含管理密码或服务秘密。
pub async fn grant_session() -> Result<Response, AppError> {
    let (base, token, client) = config()?;
    let upstream = client
        .post(format!("{base}/internal/v3/session"))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("v3 session unavailable".into()))?;
    Ok(response(upstream))
}

/// 统一退出时撤销当前 v3 会话；未启用 v3 的旧部署保持兼容。
/// @param headers 含浏览器 Cookie 的管理请求头。
/// @return 撤销结果与清除 Cookie；服务失败时不假报退出成功。
pub async fn revoke_session(headers: HeaderMap) -> Result<Response, AppError> {
    if std::env::var("V3_RUNTIME_URL")
        .unwrap_or_default()
        .is_empty()
    {
        return Ok(Json(serde_json::json!({"ok": true})).into_response());
    }
    check_origin(&headers)?;
    let (base, token, client) = config()?;
    let upstream = client
        .post(format!("{base}/api/auth/logout"))
        .bearer_auth(token)
        .header(header::COOKIE, cookie(&headers))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("v3 logout unavailable".into()))?;
    if !upstream.status().is_success() {
        return Err(AppError::ServiceUnavailable("v3 logout unavailable".into()));
    }
    Ok(response(upstream))
}

fn browser_path_allowed(method: &Method, path: &str) -> bool {
    // URL 库会规整点段和编码；先拒绝这些形式，防止白名单校验后跳入内部路径。
    if path.contains(['%', '\\']) || path.split('/').any(|part| part == "." || part == "..") {
        return false;
    }
    if matches!(*method, Method::GET | Method::HEAD)
        && matches!(
            path,
            "" | "/"
                | "/index.html"
                | "/app.js"
                | "/projects.js"
                | "/style.css"
                | "/vendor/xterm.js"
                | "/vendor/xterm.css"
                | "/vendor/xterm-addon-fit.js"
        )
    {
        return true;
    }
    if path == "/api/auth/me" {
        return *method == Method::GET;
    }
    if path == "/api/auth/logout" {
        return *method == Method::POST;
    }
    let Some(api_path) = path.strip_prefix("/api/") else {
        return false;
    };
    matches!(
        *method,
        Method::GET | Method::POST | Method::PUT | Method::DELETE
    ) && matches!(
        api_path.split('/').next().unwrap_or_default(),
        "accounts"
            | "cc2api"
            | "settings"
            | "topics"
            | "tasks"
            | "task-batches"
            | "captures"
            | "runs"
            | "runs-stream"
            | "run-continue"
            | "terminal-ticket"
            | "environments"
            | "project-catalog"
            | "project-iteration-batches"
            | "project-instances"
            | "project-step-attempts"
            | "project-iterations-stream"
            | "projects"
    )
}

async fn http_bridge(
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let browser_path = uri
        .path()
        .strip_prefix("/runtime")
        .ok_or(AppError::NotFound)?;
    if !browser_path_allowed(&method, browser_path) {
        return Err(AppError::NotFound);
    }
    authenticated(&headers).await?;
    let (base, token, client) = config()?;
    let path = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/runtime/")
        .strip_prefix("/runtime")
        .ok_or(AppError::NotFound)?;
    let mut request = client
        .request(
            method,
            format!("{base}{}", if path.is_empty() { "/" } else { path }),
        )
        .bearer_auth(token)
        .header(header::COOKIE, cookie(&headers));
    for key in [header::CONTENT_TYPE, header::ACCEPT, header::CACHE_CONTROL] {
        if let Some(value) = headers.get(&key) {
            request = request.header(key, value);
        }
    }
    let upstream = request
        .body(body)
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("v3 management request failed".into()))?;
    Ok(response(upstream))
}

async fn ws_bridge(
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, AppError> {
    authenticated(&headers).await?;
    let (base, token, _) = config()?;
    let path = uri
        .path_and_query()
        .unwrap()
        .as_str()
        .strip_prefix("/runtime")
        .unwrap()
        .to_owned();
    let upstream = base.replacen("http", "ws", 1) + &path;
    let cookie = cookie(&headers).to_owned();
    Ok(upgrade.on_upgrade(move |socket| websocket_bridge(socket, upstream, base, token, cookie)))
}

async fn websocket_bridge(
    socket: WebSocket,
    url: String,
    base: String,
    token: String,
    cookie: String,
) {
    let session_token = token.clone();
    let session_cookie = cookie.clone();
    let Ok(mut request) = url.into_client_request() else {
        return;
    };
    let Ok(authorization) = format!("Bearer {token}").parse() else {
        return;
    };
    let Ok(cookie) = cookie.parse() else {
        return;
    };
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, authorization);
    request.headers_mut().insert(header::COOKIE, cookie);
    let Ok((upstream, _)) = connect_async(request).await else {
        return;
    };
    let (mut browser_out, mut browser_in) = socket.split();
    let (mut upstream_out, mut upstream_in) = upstream.split();
    let to_upstream = async {
        while let Some(Ok(message)) = browser_in.next().await {
            let message = match message {
                Message::Text(value) => UpstreamMessage::Text(value),
                Message::Binary(value) => UpstreamMessage::Binary(value),
                Message::Ping(value) => UpstreamMessage::Ping(value),
                Message::Pong(value) => UpstreamMessage::Pong(value),
                Message::Close(_) => break,
            };
            if upstream_out.send(message).await.is_err() {
                break;
            }
        }
    };
    let to_browser = async {
        while let Some(Ok(message)) = upstream_in.next().await {
            let message = match message {
                UpstreamMessage::Text(value) => Message::Text(value),
                UpstreamMessage::Binary(value) => Message::Binary(value),
                UpstreamMessage::Ping(value) => Message::Ping(value),
                UpstreamMessage::Pong(value) => Message::Pong(value),
                _ => break,
            };
            if browser_out.send(message).await.is_err() {
                break;
            }
        }
    };
    let session_valid = async {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .build();
        let Ok(client) = client else {
            return;
        };
        loop {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let result = client
                .get(format!("{base}/internal/v3/session/verify"))
                .bearer_auth(&session_token)
                .header(header::COOKIE, &session_cookie)
                .timeout(Duration::from_secs(5))
                .send()
                .await;
            if !matches!(result, Ok(response) if response.status().is_success()) {
                return;
            }
        }
    };
    tokio::select! { _ = to_upstream => (), _ = to_browser => (), _ = session_valid => () }
}

/// 构建 HTTP/SSE/WS 管理路由，使用独立浏览器会话验证。
/// @return 可合入主路由的 v3 管理桥。
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/runtime", any(http_bridge))
        .route("/runtime/", any(http_bridge))
        .route("/runtime/*rest", any(http_bridge))
        .route("/runtime/api/accounts/login/ws/:sid", get(ws_bridge))
        .route("/runtime/api/run-continue/ws/:sid", get(ws_bridge))
}

#[cfg(test)]
mod tests {
    use super::browser_path_allowed;
    use axum::http::Method;

    #[test]
    fn management_bridge_rejects_internal_and_encoded_paths() {
        for path in [
            "/internal/v3/session",
            "/internal/v3/transport/1",
            "/api/../internal/v3/session",
            "/api/%2e%2e/internal/v3/session",
            "/api/%252e%252e/internal/v3/session",
            "/api\\..\\internal/v3/session",
            "/api/auth/login",
            "/unknown",
        ] {
            assert!(!browser_path_allowed(&Method::GET, path), "{path}");
            assert!(!browser_path_allowed(&Method::POST, path), "{path}");
        }
        assert!(browser_path_allowed(&Method::GET, "/api/runs-stream"));
        assert!(browser_path_allowed(
            &Method::POST,
            "/api/accounts/login/start"
        ));
        assert!(browser_path_allowed(&Method::GET, "/vendor/xterm.js"));
        assert!(browser_path_allowed(&Method::GET, "/projects.js"));
        assert!(!browser_path_allowed(&Method::POST, "/projects.js"));
        assert!(!browser_path_allowed(&Method::POST, "/app.js"));
    }

    /// 项目模式仅增加管理资源，编码穿越和内部服务入口继续拒绝。
    #[test]
    fn project_iteration_resources_use_authenticated_management_bridge() {
        for path in [
            "/api/project-catalog",
            "/api/project-catalog/project-001",
            "/api/project-instances/1/usage",
            "/api/project-step-attempts/1/artifacts",
            "/api/project-iterations-stream",
            "/api/projects",
            "/api/projects/catalog",
            "/api/projects/agents/1/events",
            "/api/projects/stream?account_id=1",
        ] {
            assert!(browser_path_allowed(&Method::GET, path), "{path}");
        }
        assert!(browser_path_allowed(
            &Method::POST,
            "/api/project-iteration-batches/1/start"
        ));
        for path in [
            "/api/project-instances/../internal/v3/transport/1",
            "/api/project-instances/%2e%2e/internal/v3/session",
            "/internal/v3/project-catalog",
            "/api/projects/%2e%2e/internal/v3/session",
        ] {
            assert!(!browser_path_allowed(&Method::GET, path), "{path}");
        }
    }
}
