//! v3 账号容器传输边界：复用原 HTTP/TLS 发送规则，不负责协议改写或重试。

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::iter::once;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use uuid::Uuid;

use axum::Json;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, Method};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures_util::StreamExt;
use futures_util::stream::unfold;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::AppError;
use crate::model::account::Account;
use crate::tlsfp::get_request_client;

const META_HEADER: &str = "x-v3-request";
const ERROR_HEADER: &str = "x-v3-transport-error";

/// 由调度器登记的单账号传输实例；秘密仅用于服务间鉴权。
#[derive(Clone, Deserialize, Serialize)]
pub struct TransportBinding {
    pub account_id: i64,
    pub environment_id: String,
    pub generation: u64,
    pub endpoint: String,
    pub secret: String,
    #[serde(default)]
    pub release_id: String,
}

/// 请求元信息与原始 body 分开传输，避免重新序列化改变协议字节。
#[derive(Clone, Deserialize, Serialize)]
pub struct PreparedRequest {
    pub account_id: i64,
    pub environment_id: String,
    pub generation: u64,
    pub request_id: String,
    pub deadline_unix_ms: u64,
    pub method: String,
    pub target_url: String,
    pub headers: Vec<(String, String)>,
    pub preserve_header_casing: bool,
}

/// 账号进程的受限配置；由管理进程原子写入，CLI 用户不可读取。
#[derive(Clone, Deserialize, Serialize)]
pub struct SenderConfig {
    pub binding: TransportBinding,
    pub proxy_url: String,
    pub allowed_origin: String,
    #[serde(default)]
    pub evidence_file: Option<String>,
    #[serde(default)]
    pub quota_identity: String,
}

fn record_headers(
    config: &SenderConfig,
    prepared: &PreparedRequest,
    upstream: &reqwest::Response,
) -> Result<(), AppError> {
    let Some(path) = &config.evidence_file else {
        return Ok(());
    };
    let headers: serde_json::Map<String, serde_json::Value> = upstream
        .headers()
        .iter()
        .filter(|(name, _)| {
            name.as_str().starts_with("anthropic-ratelimit-unified-")
                || name.as_str() == "retry-after"
        })
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), serde_json::json!(value)))
        })
        .collect();
    let record = serde_json::json!({"source": format!("api:{}:{}", config.binding.environment_id, config.quota_identity),
        "identity": config.quota_identity, "origin": "api", "environment_id": config.binding.environment_id,
        "account_id": config.binding.account_id, "generation": config.binding.generation,
        "request_id": prepared.request_id, "ts": now_ms() as f64 / 1000.0,
        "status": upstream.status().as_u16(), "headers": headers});
    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|_| AppError::Internal("API evidence unavailable".into()))?;
    }
    // 有界保留最近证据；消费者按 inode 重置游标，避免长期账号无限增长。
    if fs::metadata(path)
        .map(|value| value.len() >= 16 * 1024 * 1024)
        .unwrap_or(false)
    {
        fs::rename(path, path.with_extension("jsonl.previous"))
            .map_err(|_| AppError::Internal("API evidence rotation unavailable".into()))?;
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(path)
        .map_err(|_| AppError::Internal("API evidence unavailable".into()))?;
    let mut bytes = serde_json::to_vec(&record)
        .map_err(|_| AppError::Internal("API evidence unavailable".into()))?;
    bytes.push(b'\n');
    file.write_all(&bytes)
        .map_err(|_| AppError::Internal("API evidence unavailable".into()))
}

/// 使用现有客户端发送已经完成改写的请求，不增加重试。
///
/// @param proxy_url 当前账号的上游代理。
/// @param prepared 已冻结的方法、地址和有序请求头。
/// @param body 最终上游 body 字节。
/// @return 上游原始响应；发送失败不表明请求未被上游接受。
pub async fn send_direct(
    proxy_url: &str,
    prepared: &PreparedRequest,
    body: Vec<u8>,
) -> Result<reqwest::Response, AppError> {
    let client = get_request_client(proxy_url)
        .map_err(|_| AppError::BadGateway("upstream request client unavailable".into()))?;
    send_with_client(&client, prepared, body).await
}

async fn send_with_client(
    client: &reqwest::Client,
    prepared: &PreparedRequest,
    body: Vec<u8>,
) -> Result<reqwest::Response, AppError> {
    let method = Method::from_bytes(prepared.method.as_bytes())
        .map_err(|_| AppError::BadRequest("invalid upstream method".into()))?;
    let mut request = client.request(method, &prepared.target_url);
    if prepared.preserve_header_casing {
        request = request.http1_header_casing(
            prepared
                .headers
                .iter()
                .map(|(name, _)| name.as_str())
                .chain(once("Content-Length")),
        );
    }
    for (name, value) in &prepared.headers {
        request = request.header(name, value);
    }
    request
        .body(body)
        .send()
        .await
        .map_err(|_| AppError::BadGateway("upstream submission status unknown".into()))
}

/// 网关发送入口；配置 v3 时只向对应账号容器发送，解析失败绝不直连。
///
/// @param account 已选定账号。
/// @param prepared 已改写请求的传输元信息。
/// @param body 不再修改的原始 body。
/// @param ttfb_timeout 沿用原客户端的响应首字节期限；环境唤醒单独计时。
/// @return 上游响应或不可安全重放的传输错误。
pub async fn send(
    account: &Account,
    mut prepared: PreparedRequest,
    body: &[u8],
    ttfb_timeout: Duration,
) -> Result<reqwest::Response, AppError> {
    let base = std::env::var("V3_RUNTIME_URL").unwrap_or_default();
    if base.is_empty() {
        if !std::env::var("V3_DEPLOYMENT_ID")
            .unwrap_or_default()
            .is_empty()
        {
            return Err(AppError::ServiceUnavailable(
                "v3 runtime is required".into(),
            ));
        }
        return timeout(
            ttfb_timeout,
            send_direct(&account.proxy_url, &prepared, body.to_vec()),
        )
        .await
        .map_err(|_| AppError::BadGateway("upstream TTFB timeout".into()))?;
    }
    let secret = std::env::var("V3_INTERNAL_TOKEN").map_err(|_| {
        AppError::ServiceUnavailable("v3 runtime authentication unavailable".into())
    })?;
    // 内部 HTTP 不继承宿主代理，不使用上游 TLS 画像，也不设置会误杀长流的总超时。
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| AppError::ServiceUnavailable("v3 bridge unavailable".into()))?;
    let binding: TransportBinding = client
        .get(format!(
            "{}/internal/v3/transport/{}",
            base.trim_end_matches('/'),
            account.id
        ))
        .bearer_auth(secret)
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("account environment unavailable".into()))?
        .error_for_status()
        .map_err(|_| AppError::ServiceUnavailable("account environment not ready".into()))?
        .json()
        .await
        .map_err(|_| AppError::ServiceUnavailable("invalid runtime binding".into()))?;
    let deployment = std::env::var("V3_DEPLOYMENT_ID").unwrap_or_default();
    let release = std::env::var("V3_RELEASE").unwrap_or_default();
    validate_binding(&binding, account.id, &deployment, &release)?;
    prepared.request_id = Uuid::new_v4().to_string();
    prepared.deadline_unix_ms = now_ms() + ttfb_timeout.as_millis() as u64;
    prepared.account_id = account.id;
    prepared.environment_id = binding.environment_id;
    prepared.generation = binding.generation;
    let meta = STANDARD.encode(
        serde_json::to_vec(&prepared)
            .map_err(|_| AppError::Internal("serialize runtime request".into()))?,
    );
    let response = timeout(
        ttfb_timeout,
        client
            .post(format!(
                "{}/internal/send",
                binding.endpoint.trim_end_matches('/')
            ))
            .bearer_auth(binding.secret)
            .header(META_HEADER, meta)
            .body(body.to_vec())
            .send(),
    )
    .await
    .map_err(|_| AppError::BadGateway("account submission status unknown".into()))?
    .map_err(|_| AppError::BadGateway("account submission status unknown".into()))?;
    if let Some(error) = response.headers().get(ERROR_HEADER) {
        return Err(if error == "before-send" {
            AppError::ServiceUnavailable("account request rejected before submission".into())
        } else {
            AppError::BadGateway("account submission status unknown".into())
        });
    }
    Ok(response)
}

/// 按调度器的短 DNS 名核对部署归属，防止旧前缀条件拒绝合法账号环境。
fn validate_binding(
    binding: &TransportBinding,
    account_id: i64,
    deployment: &str,
    release: &str,
) -> Result<(), AppError> {
    let endpoint = reqwest::Url::parse(&binding.endpoint)
        .map_err(|_| AppError::ServiceUnavailable("invalid sender endpoint".into()))?;
    let deployment_key = format!("{:x}", Sha256::digest(deployment.as_bytes()));
    if deployment.is_empty()
        || release.is_empty()
        || binding.release_id != release
        || binding.environment_id.len() != 32
        || !binding
            .environment_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || endpoint.host_str()
            != Some(
                format!(
                    "v3-{}-account-{}",
                    &deployment_key[..12],
                    binding.environment_id
                )
                .as_str(),
            )
        || binding.account_id != account_id
        || binding.secret.len() < 32
        || endpoint.scheme() != "http"
        || endpoint.port() != Some(8091)
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(AppError::ServiceUnavailable(
            "runtime binding mismatch".into(),
        ));
    }
    Ok(())
}

/// 账号发送器运行状态；活跃计数覆盖响应流生命周期。
#[derive(Clone)]
struct SenderState {
    config_path: String,
    active: Arc<AtomicU64>,
    evidence_error: Arc<AtomicBool>,
    evidence_lock: Arc<Mutex<()>>,
}

struct ActiveRequest(Arc<AtomicU64>);

impl Drop for ActiveRequest {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn read_config(path: &str) -> Result<SenderConfig, AppError> {
    let bytes = std::fs::read(path)
        .map_err(|_| AppError::ServiceUnavailable("sender configuration unavailable".into()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| AppError::ServiceUnavailable("invalid sender configuration".into()))
}

fn authenticate(headers: &HeaderMap, config: &SenderConfig) -> Result<(), AppError> {
    let actual = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if config.binding.secret.len() < 32
        || !bool::from(actual.as_bytes().ct_eq(config.binding.secret.as_bytes()))
    {
        return Err(AppError::Unauthorized);
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn validate_request(prepared: &PreparedRequest, config: &SenderConfig) -> Result<(), AppError> {
    let url = reqwest::Url::parse(&prepared.target_url)
        .map_err(|_| AppError::BadRequest("invalid upstream URL".into()))?;
    let allowed = reqwest::Url::parse(&config.allowed_origin)
        .map_err(|_| AppError::ServiceUnavailable("invalid allowed origin".into()))?;
    if Uuid::parse_str(&prepared.request_id).is_err()
        || prepared.deadline_unix_ms <= now_ms()
        || prepared.deadline_unix_ms > now_ms() + 120_000
        || prepared.account_id != config.binding.account_id
        || prepared.environment_id != config.binding.environment_id
        || prepared.generation != config.binding.generation
        || url.origin() != allowed.origin()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !["/v1/", "/api/"]
            .iter()
            .any(|prefix| url.path().starts_with(prefix))
    {
        return Err(AppError::BadRequest(
            "runtime request binding or target mismatch".into(),
        ));
    }
    Ok(())
}

async fn status(
    State(state): State<SenderState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let config = read_config(&state.config_path)?;
    authenticate(&headers, &config)?;
    Ok(Json(
        serde_json::json!({"active": state.active.load(Ordering::SeqCst),
        "environment_id": config.binding.environment_id, "generation": config.binding.generation,
        "evidence_ok": !state.evidence_error.load(Ordering::SeqCst)}),
    ))
}

async fn forward(State(state): State<SenderState>, headers: HeaderMap, body: Bytes) -> Response {
    let mut submitted = false;
    let result =
        async {
            let config = read_config(&state.config_path)?;
            authenticate(&headers, &config)?;
            if state.evidence_error.load(Ordering::SeqCst) {
                return Err(AppError::ServiceUnavailable(
                    "API evidence requires maintenance".into(),
                ));
            }
            let meta = headers
                .get(META_HEADER)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| AppError::BadRequest("missing runtime request".into()))?;
            let bytes = STANDARD
                .decode(meta)
                .map_err(|_| AppError::BadRequest("invalid runtime request".into()))?;
            let prepared: PreparedRequest = serde_json::from_slice(&bytes)
                .map_err(|_| AppError::BadRequest("invalid runtime request".into()))?;
            validate_request(&prepared, &config)?;
            state.active.fetch_add(1, Ordering::SeqCst);
            let guard = ActiveRequest(state.active.clone());
            submitted = true;
            let upstream = timeout(
                Duration::from_millis(prepared.deadline_unix_ms.saturating_sub(now_ms())),
                send_direct(&config.proxy_url, &prepared, body.to_vec()),
            )
            .await
            .map_err(|_| AppError::BadGateway("upstream submission status unknown".into()))??;
            let code = upstream.status();
            {
                let _lock = state.evidence_lock.lock().unwrap();
                if record_headers(&config, &prepared, &upstream).is_err() {
                    // 当前 API 响应仍保持原样，故障通过健康状态阻止下一作业或新提交。
                    state.evidence_error.store(true, Ordering::SeqCst);
                }
            }
            let response_headers = upstream.headers().clone();
            // guard 跟随流；下游取消会 drop reqwest 流并关闭该响应，不在这里重放。
            let stream = unfold(
                (upstream.bytes_stream(), guard),
                |(mut stream, guard)| async move {
                    stream.next().await.map(|item| (item, (stream, guard)))
                },
            );
            let mut response = Response::new(Body::from_stream(stream));
            *response.status_mut() = code;
            *response.headers_mut() = response_headers;
            response.headers_mut().remove("transfer-encoding");
            Ok::<Response, AppError>(response)
        }
        .await;
    result.unwrap_or_else(|error| {
        let mut response = error.into_response();
        response.headers_mut().insert(
            ERROR_HEADER,
            if submitted {
                "uncertain"
            } else {
                "before-send"
            }
            .parse()
            .unwrap(),
        );
        response
    })
}

/// 构建只服务单个账号环境的内部发送器路由。
///
/// @param config_path root 所有的账号配置文件路径。
/// @return 包含发送与活动状态接口的 Router。
pub fn sender_router(config_path: String) -> Router {
    Router::new()
        .route("/internal/send", post(forward))
        .route("/internal/status", get(status))
        .layer(DefaultBodyLimit::max(128 * 1024 * 1024))
        .with_state(SenderState {
            config_path,
            active: Arc::new(AtomicU64::new(0)),
            evidence_error: Arc::new(AtomicBool::new(false)),
            evidence_lock: Arc::new(Mutex::new(())),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::tlsfp::test_request_client;
    use rustls_pki_types::CertificateDer;
    use std::io::{BufRead, BufReader};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};

    #[test]
    fn gateway_accepts_short_account_dns_and_rejects_foreign_bindings() {
        let deployment = "synthetic-deployment";
        let key = format!("{:x}", Sha256::digest(deployment.as_bytes()));
        let environment_id = "a".repeat(32);
        let binding = TransportBinding {
            account_id: 7,
            environment_id: environment_id.clone(),
            generation: 2,
            endpoint: format!("http://v3-{}-account-{environment_id}:8091", &key[..12]),
            secret: "synthetic-secret-".repeat(4),
            release_id: "test".into(),
        };
        assert!(validate_binding(&binding, 7, deployment, "test").is_ok());
        assert!(validate_binding(&binding, 8, deployment, "test").is_err());
        assert!(validate_binding(&binding, 7, "other-deployment", "test").is_err());
        assert!(validate_binding(&binding, 7, deployment, "other-release").is_err());
        assert!(validate_binding(&binding, 7, "", "test").is_err());
        assert!(validate_binding(&binding, 7, deployment, "").is_err());
        for endpoint in [
            format!("http://vibebench-v3-{deployment}-account-{environment_id}:8091"),
            format!("http://v3-{}-account-{}:8091", &key[..12], "b".repeat(32)),
            binding.endpoint.replace(":8091", ":8092"),
            binding.endpoint.replace("http://", "https://"),
            format!("{}/internal/send", binding.endpoint),
            format!("{}?token=synthetic", binding.endpoint),
            format!("{}#fragment", binding.endpoint),
            binding.endpoint.replace("http://", "http://synthetic@"),
        ] {
            let invalid = TransportBinding {
                endpoint,
                ..binding.clone()
            };
            assert!(validate_binding(&invalid, 7, deployment, "test").is_err());
        }
        let invalid = TransportBinding {
            secret: "short".into(),
            ..binding.clone()
        };
        assert!(validate_binding(&invalid, 7, deployment, "test").is_err());
        let invalid = TransportBinding {
            environment_id: "z".repeat(32),
            ..binding
        };
        assert!(validate_binding(&invalid, 7, deployment, "test").is_err());
    }

    /// 私有临时证书和回环 TLS 服务，退出时收口进程并删除资料。
    struct TlsFixture {
        root: PathBuf,
        child: Child,
        address: SocketAddr,
    }

    impl TlsFixture {
        fn start() -> Self {
            let root = std::env::temp_dir().join(format!("v3-tls-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            }
            let child = Command::new("python3")
                .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls_http.py"))
                .arg(&root)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut fixture = Self {
                root,
                child,
                address: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            };
            let mut line = String::new();
            BufReader::new(fixture.child.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            let value: serde_json::Value = serde_json::from_str(&line).unwrap();
            fixture
                .address
                .set_port(value["port"].as_u64().unwrap().try_into().unwrap());
            fixture
        }

        fn client(&self, trusted: bool, hostname: &str) -> reqwest::Client {
            let certificate =
                trusted.then(|| CertificateDer::from(fs::read(self.root.join("ca.der")).unwrap()));
            test_request_client(certificate, hostname, self.address).unwrap()
        }

        fn request(&self, hostname: &str, path: &str) -> PreparedRequest {
            PreparedRequest {
                account_id: 7,
                environment_id: "synthetic".into(),
                generation: 1,
                request_id: Uuid::new_v4().to_string(),
                deadline_unix_ms: now_ms() + 5000,
                method: "POST".into(),
                target_url: format!("https://{hostname}:{}{path}", self.address.port()),
                headers: vec![
                    ("Authorization".into(), "Bearer synthetic".into()),
                    ("Content-Type".into(), "application/json".into()),
                    ("X-Order-A".into(), "a".into()),
                    ("X-Order-B".into(), "b".into()),
                ],
                preserve_header_casing: true,
            }
        }
    }

    impl Drop for TlsFixture {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[tokio::test]
    async fn local_https_preserves_bytes_and_rejects_untrusted_or_wrong_hostname() {
        let fixture = TlsFixture::start();
        let prepared = fixture.request("api.anthropic.com", "/v1/messages");
        let body = b"{ \"synthetic\" : true }\n".to_vec();
        let response = timeout(
            Duration::from_secs(5),
            send_with_client(
                &fixture.client(true, "api.anthropic.com"),
                &prepared,
                body.clone(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status().as_u16(), 429);
        assert_eq!(response.headers()["x-tls-fixture"], "verified");
        assert_eq!(response.bytes().await.unwrap(), body);
        let record = fs::read_to_string(fixture.root.join("requests.jsonl")).unwrap();
        let record: serde_json::Value =
            serde_json::from_str(record.lines().next().unwrap()).unwrap();
        let headers = record["headers"].as_array().unwrap();
        assert!(
            headers
                .iter()
                .position(|key| key == "Authorization")
                .unwrap()
                < headers
                    .iter()
                    .position(|key| key == "Content-Type")
                    .unwrap()
        );
        assert!(
            headers.iter().position(|key| key == "X-Order-A").unwrap()
                < headers.iter().position(|key| key == "X-Order-B").unwrap()
        );
        assert!(
            timeout(
                Duration::from_secs(5),
                send_with_client(
                    &fixture.client(false, "api.anthropic.com"),
                    &prepared,
                    vec![]
                )
            )
            .await
            .unwrap()
            .is_err()
        );
        let wrong = fixture.request("wrong.invalid", "/v1/messages");
        assert!(
            timeout(
                Duration::from_secs(5),
                send_with_client(&fixture.client(true, "wrong.invalid"), &wrong, vec![])
            )
            .await
            .unwrap()
            .is_err()
        );
        assert_eq!(
            fs::read_to_string(fixture.root.join("requests.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn local_https_stream_cancel_closes_connection_without_replay() {
        let fixture = TlsFixture::start();
        let prepared = fixture.request("api.anthropic.com", "/stream");
        let client = fixture.client(true, "api.anthropic.com");
        let mut response = timeout(
            Duration::from_secs(5),
            send_with_client(&client, &prepared, b"{}".to_vec()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), response.chunk())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            &b"data: a\n\n"[..]
        );
        drop(response);
        timeout(Duration::from_secs(5), async {
            while !fixture.root.join("stream_closed").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            fs::read_to_string(fixture.root.join("requests.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[test]
    fn rejects_wrong_environment_and_arbitrary_target() {
        let binding = TransportBinding {
            account_id: 7,
            environment_id: "env-a".into(),
            generation: 2,
            endpoint: "http://account-a:8091".into(),
            secret: "x".repeat(64),
            release_id: "test".into(),
        };
        let config = SenderConfig {
            binding,
            proxy_url: String::new(),
            allowed_origin: "https://api.anthropic.com".into(),
            evidence_file: None,
            quota_identity: String::new(),
        };
        let mut prepared = PreparedRequest {
            account_id: 7,
            environment_id: "env-a".into(),
            generation: 2,
            request_id: Uuid::new_v4().to_string(),
            deadline_unix_ms: now_ms() + 30_000,
            method: "POST".into(),
            target_url: "https://api.anthropic.com/v1/messages?beta=true".into(),
            headers: vec![],
            preserve_header_casing: true,
        };
        assert!(validate_request(&prepared, &config).is_ok());
        prepared.generation = 1;
        assert!(validate_request(&prepared, &config).is_err());
        prepared.generation = 2;
        prepared.target_url = "http://169.254.169.254/api/metadata".into();
        assert!(validate_request(&prepared, &config).is_err());
    }
}
