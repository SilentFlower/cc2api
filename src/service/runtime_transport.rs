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
use tokio::sync::RwLock;
use tokio::time::{Instant, timeout, timeout_at};
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
use reqwest::Client;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::AppError;
use crate::model::account::Account;
use crate::model::request_diagnostics::RequestLogMode;
use crate::service::native_contract::{
    NativeKernelPreparation, NativeKernelTarget, NativePreparation, NativePreparationControl,
    NativePreparationStatus, NativeReadinessStatus,
};
use crate::service::native_runtime::{NativePool, NativeSenderConfig, NativeSubmission};
use crate::tlsfp::{TlsProfileId, get_request_client_for_profile};

const META_HEADER: &str = "x-v3-request";
const ERROR_HEADER: &str = "x-v3-transport-error";
/// 私有请求最多持有的单调执行预算。
pub(crate) const REQUEST_BUDGET_MS: u64 = 120_000;
/// 绑定校验允许的跨进程墙钟偏差。
pub(crate) const CLOCK_SKEW_MS: u64 = 5_000;

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
    /// 网关冻结的已注册 TLS 标识；历史报文缺字段时保留原内置默认。
    #[serde(default)]
    pub tls_profile: TlsProfileId,
}

/// 账号进程的受限配置；由管理进程原子写入，CLI 用户不可读取。
#[derive(Clone, Deserialize, Serialize)]
pub struct SenderConfig {
    #[serde(default)]
    pub native: Option<NativeSenderConfig>,
    #[serde(default)]
    pub native_capture_dir: Option<String>,
    #[serde(default)]
    pub native_adaptive_prewarm: bool,
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
    record_response_headers(
        config,
        prepared,
        upstream.status().as_u16(),
        upstream.headers(),
    )
}

fn record_response_headers(
    config: &SenderConfig,
    prepared: &PreparedRequest,
    status: u16,
    response_headers: &HeaderMap,
) -> Result<(), AppError> {
    let Some(path) = &config.evidence_file else {
        return Ok(());
    };
    let headers: serde_json::Map<String, serde_json::Value> = response_headers
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
        "status": status, "headers": headers});
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
    let client = get_request_client_for_profile(proxy_url, prepared.tls_profile)
        .map_err(|_| AppError::BadGateway("upstream request client unavailable".into()))?;
    send_with_client(&client, prepared, body).await
}

async fn send_with_client(
    client: &Client,
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

/// 已完成环境准备的发送通道；不持有正文或发送许可。
pub struct PreparedTransport {
    account_id: i64,
    route: PreparedRoute,
}

/// 已确认归属的内部通道或原有直连兼容通道。
enum PreparedRoute {
    Direct(String),
    V3 {
        client: Client,
        binding: TransportBinding,
    },
}

/// 准备传输环境，供 v3 在环境就绪后取得平滑发送许可。
/// @param account 已选定账号。
/// @param preparation_timeout 本请求剩余准入预算。
/// @return 已验证账号、环境、代次和发布归属的通道。
pub async fn prepare(
    account: &Account,
    preparation_timeout: Duration,
) -> Result<PreparedTransport, AppError> {
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
        return Ok(PreparedTransport {
            account_id: account.id,
            route: PreparedRoute::Direct(account.proxy_url.clone()),
        });
    }
    let secret = std::env::var("V3_INTERNAL_TOKEN").map_err(|_| {
        AppError::ServiceUnavailable("v3 runtime authentication unavailable".into())
    })?;
    // 内部传输不继承代理或上游 TLS，也不设置会误杀长流的总超时。
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| AppError::ServiceUnavailable("v3 bridge unavailable".into()))?;
    let deployment = std::env::var("V3_DEPLOYMENT_ID").unwrap_or_default();
    let release = configured_account_release();
    prepare_from_runtime(
        account,
        client,
        &base,
        &secret,
        &deployment,
        &release,
        preparation_timeout,
    )
    .await
}

/// 只解析已经运行的账号环境，后台准备不会唤醒休眠环境。
/// @param account 当前账号快照。
/// @return 已验证绑定；环境停止或未配置 V3 时为空。
pub(crate) async fn prepare_existing(
    account: &Account,
) -> Result<Option<PreparedTransport>, AppError> {
    let base = std::env::var("V3_RUNTIME_URL").unwrap_or_default();
    if base.is_empty() {
        return Ok(None);
    }
    let secret = std::env::var("V3_INTERNAL_TOKEN").map_err(|_| {
        AppError::ServiceUnavailable("v3 runtime authentication unavailable".into())
    })?;
    let client = Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .connect_timeout(Duration::from_secs(5))
        .build()
        .map_err(|_| AppError::ServiceUnavailable("v3 bridge unavailable".into()))?;
    let binding: Option<TransportBinding> = client
        .get(format!(
            "{}/internal/v3/transport/{}?wake=false",
            base.trim_end_matches('/'),
            account.id
        ))
        .bearer_auth(secret)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("运行绑定读取失败".into()))?
        .error_for_status()
        .map_err(|_| AppError::ServiceUnavailable("运行绑定未就绪".into()))?
        .json()
        .await
        .map_err(|_| AppError::ServiceUnavailable("运行绑定无效".into()))?;
    let Some(binding) = binding else {
        return Ok(None);
    };
    validate_binding(
        &binding,
        account.id,
        &std::env::var("V3_DEPLOYMENT_ID").unwrap_or_default(),
        &configured_account_release(),
    )?;
    Ok(Some(PreparedTransport {
        account_id: account.id,
        route: PreparedRoute::V3 { client, binding },
    }))
}

/// 分离配置来源与传输，测试可注入仅解析到回环地址的客户端。
/// @param account 当前账号。
/// @param client 不继承代理的内部客户端。
/// @param base 调度器入口。
/// @param secret 内部鉴权值。
/// @param deployment 部署归属。
/// @param release 发布归属。
/// @param preparation_timeout 剩余准备期限。
/// @return 身份验证后的发送通道。
async fn prepare_from_runtime(
    account: &Account,
    client: Client,
    base: &str,
    secret: &str,
    deployment: &str,
    release: &str,
    preparation_timeout: Duration,
) -> Result<PreparedTransport, AppError> {
    let binding: TransportBinding = client
        .get(format!(
            "{}/internal/v3/transport/{}",
            base.trim_end_matches('/'),
            account.id
        ))
        .bearer_auth(secret)
        .timeout(preparation_timeout.min(Duration::from_secs(120)))
        .send()
        .await
        .map_err(|_| AppError::ServiceUnavailable("account environment unavailable".into()))?
        .error_for_status()
        .map_err(|_| AppError::ServiceUnavailable("account environment not ready".into()))?
        .json()
        .await
        .map_err(|_| AppError::ServiceUnavailable("invalid runtime binding".into()))?;
    validate_binding(&binding, account.id, deployment, release)?;
    Ok(PreparedTransport {
        account_id: account.id,
        route: PreparedRoute::V3 { client, binding },
    })
}

impl PreparedTransport {
    /// 核对两次只读解析是否仍指向同一受信任 sender。
    /// @param other 当前重新读取的传输绑定。
    /// @return 账号、代次、服务与鉴权配置完全一致时为 true。
    pub(crate) fn same_binding(&self, other: &Self) -> bool {
        match (&self.route, &other.route) {
            (PreparedRoute::V3 { binding: left, .. }, PreparedRoute::V3 { binding: right, .. }) => {
                self.account_id == other.account_id
                    && left.account_id == right.account_id
                    && left.environment_id == right.environment_id
                    && left.generation == right.generation
                    && left.endpoint == right.endpoint
                    && left.secret == right.secret
                    && left.release_id == right.release_id
            }
            _ => false,
        }
    }

    /// 使用已绑定环境准备一个原生进程，不提交模型作业。
    /// @param preparation 网关冻结的账号身份与准备标识。
    /// @return 真实握手后的准备状态。
    pub(crate) async fn prepare_native(
        &self,
        preparation: NativePreparation,
    ) -> Result<NativePreparationStatus, AppError> {
        self.prepare_native_target(preparation, None).await
    }

    /// 只读取已绑定 sender 的当前身份就绪状态。
    /// @param identity 当前权威账号身份，绑定由此 transport 冻结。
    /// @return 新鲜安全状态；不支持或检查失败时明确报错。
    pub(crate) async fn native_readiness(
        &self,
        mut identity: NativePreparation,
    ) -> Result<NativeReadinessStatus, AppError> {
        let PreparedRoute::V3 { client, binding } = &self.route else {
            return Err(AppError::ServiceUnavailable(
                "原生检查需要 V3 sender".into(),
            ));
        };
        identity.account_id = self.account_id;
        identity.environment_id = binding.environment_id.clone();
        identity.generation = binding.generation;
        let status: NativeReadinessStatus = client
            .post(format!(
                "{}/internal/native/readiness",
                binding.endpoint.trim_end_matches('/')
            ))
            .bearer_auth(&binding.secret)
            .json(&identity)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|_| AppError::ServiceUnavailable("原生检查通道不可用".into()))?
            .error_for_status()
            .map_err(|_| AppError::ServiceUnavailable("原生检查失败或能力不支持".into()))?
            .json()
            .await
            .map_err(|_| AppError::ServiceUnavailable("原生检查回执无效".into()))?;
        if status.account_id != self.account_id
            || status.environment_id != binding.environment_id
            || status.generation != binding.generation
            || !matches!(
                status.state.as_str(),
                "ready" | "busy" | "preparing" | "unprepared" | "failed"
            )
            || (status.state == "ready" && (status.pid.is_none() || status.idle_workers == 0))
        {
            return Err(AppError::ServiceUnavailable("原生检查回执归属不符".into()));
        }
        Ok(status)
    }

    /// 以已核验的同部署绑定预热精确候选，不向上游提交业务。
    /// @param binding 调度器持锁读取的 sender 绑定。
    /// @param preparation 当前账号身份。
    /// @param target 候选发布及操作代次。
    /// @return 真实握手回执。
    pub(crate) async fn probe_native_kernel(
        binding: TransportBinding,
        preparation: NativePreparation,
        target: NativeKernelTarget,
    ) -> Result<NativePreparationStatus, AppError> {
        validate_binding(
            &binding,
            preparation.account_id,
            &std::env::var("V3_DEPLOYMENT_ID").unwrap_or_default(),
            &std::env::var("V3_RELEASE").unwrap_or_default(),
        )?;
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| AppError::ServiceUnavailable("切换准备通道不可用".into()))?;
        Self {
            account_id: binding.account_id,
            route: PreparedRoute::V3 { client, binding },
        }
        .prepare_native_target(preparation, Some(target))
        .await
    }

    async fn prepare_native_target(
        &self,
        mut preparation: NativePreparation,
        target: Option<NativeKernelTarget>,
    ) -> Result<NativePreparationStatus, AppError> {
        let PreparedRoute::V3 { client, binding } = &self.route else {
            return Err(AppError::ServiceUnavailable(
                "原生准备需要 V3 sender".into(),
            ));
        };
        preparation.account_id = self.account_id;
        preparation.environment_id = binding.environment_id.clone();
        preparation.generation = binding.generation;
        let path = if target.is_some() {
            "prepare-switch"
        } else {
            "prepare"
        };
        let request = client
            .post(format!(
                "{}/internal/native/{path}",
                binding.endpoint.trim_end_matches('/')
            ))
            .bearer_auth(&binding.secret);
        let request = if let Some(target) = &target {
            request.json(&NativeKernelPreparation {
                target: target.clone(),
                identity: preparation.clone(),
            })
        } else {
            request.json(&preparation)
        };
        let response = request
            .timeout(Duration::from_secs(110))
            .send()
            .await
            .map_err(|_| AppError::ServiceUnavailable("原生准备通道不可用".into()))?
            .error_for_status()
            .map_err(|_| AppError::ServiceUnavailable("原生准备失败".into()))?;
        let status: NativePreparationStatus = response
            .json()
            .await
            .map_err(|_| AppError::ServiceUnavailable("原生准备回执无效".into()))?;
        if status.account_id != self.account_id
            || status.environment_id != binding.environment_id
            || status.generation != binding.generation
            || status.preparation_id != preparation.preparation_id
            || status.version != "2.1.293"
            || status.raw_input_protocol != 1
            || !matches!(status.state.as_str(), "ready" | "busy")
            || target.as_ref().is_some_and(|target| {
                status.release != target.release
                    || status.kernel_generation.as_deref()
                        != Some(target.kernel_generation.as_str())
                    || status.state != "ready"
            })
            || (status.state == "ready" && (status.pid.is_none() || status.idle_workers == 0))
        {
            return Err(AppError::ServiceUnavailable("原生准备回执归属不符".into()));
        }
        Ok(status)
    }

    /// 取消当前绑定中指定的准备，保留在途业务。
    /// @param preparation_id 本次准备 UUID。
    /// @return 已提交取消控制或明确通道错误。
    pub(crate) async fn cancel_native_preparation(
        &self,
        preparation_id: &str,
    ) -> Result<(), AppError> {
        let PreparedRoute::V3 { client, binding } = &self.route else {
            return Ok(());
        };
        client
            .post(format!(
                "{}/internal/native/prepare/cancel",
                binding.endpoint.trim_end_matches('/')
            ))
            .bearer_auth(&binding.secret)
            .json(&NativePreparationControl {
                account_id: self.account_id,
                environment_id: binding.environment_id.clone(),
                generation: binding.generation,
                preparation_id: preparation_id.into(),
            })
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|_| AppError::ServiceUnavailable("原生准备取消失败".into()))?
            .error_for_status()
            .map_err(|_| AppError::ServiceUnavailable("原生准备取消被拒绝".into()))?;
        Ok(())
    }

    /// 在发送准入之前校验账号原生能力，准备失败不记录模型提交。
    /// @return 已验证原始输入协议和环境代次，或明确准备错误。
    pub async fn native_ready(&self) -> Result<(), AppError> {
        let PreparedRoute::V3 { client, binding } = &self.route else {
            return Err(AppError::ServiceUnavailable(
                "原生方式需要 V3 账号 sender".into(),
            ));
        };
        let capability = client
            .get(format!(
                "{}/internal/native/status",
                binding.endpoint.trim_end_matches('/')
            ))
            .bearer_auth(&binding.secret)
            .send()
            .await
            .map_err(|_| AppError::ServiceUnavailable("原生 sender 能力不可用".into()))?;
        if !capability.status().is_success() {
            return Err(AppError::ServiceUnavailable("原生 sender 准备失败".into()));
        }
        let capability: serde_json::Value = capability
            .json()
            .await
            .map_err(|_| AppError::ServiceUnavailable("原生 sender 能力无效".into()))?;
        if capability["raw_input_protocol"] != 1
            || capability["environment_id"] != binding.environment_id
            || capability["generation"] != binding.generation
        {
            return Err(AppError::ServiceUnavailable(
                "原生 sender 能力或代次不匹配".into(),
            ));
        }
        Ok(())
    }

    /// 向具备原始输入能力的 V3 sender 提交一次请求。
    /// @param submission 已冻结的调用方信封与当前账号授权。
    /// @param ttfb_timeout 包括能力检查及首次响应的预算。
    /// @return 上游响应；缺能力或提交失败时返回明确错误。
    pub async fn submit_native(
        self,
        mut submission: NativeSubmission,
        ttfb_timeout: Duration,
    ) -> Result<reqwest::Response, AppError> {
        let PreparedRoute::V3 { client, binding } = self.route else {
            return Err(AppError::ServiceUnavailable(
                "原生方式需要 V3 账号 sender".into(),
            ));
        };
        let operation = async {
            submission.request.account_id = self.account_id;
            submission.request.environment_id = binding.environment_id;
            submission.request.generation = binding.generation;
            submission.request.deadline_unix_ms =
                now_ms() + ttfb_timeout.as_millis().min(REQUEST_BUDGET_MS as u128) as u64;
            let response = client
                .post(format!(
                    "{}/internal/native/messages",
                    binding.endpoint.trim_end_matches('/')
                ))
                .bearer_auth(binding.secret)
                .json(&submission)
                .send()
                .await
                .map_err(|_| AppError::BadGateway("原生请求提交状态未知".into()))?;
            if let Some(error) = response.headers().get(ERROR_HEADER) {
                return Err(if error == "before-send" {
                    AppError::ServiceUnavailable("原生请求准备失败".into())
                } else {
                    AppError::BadGateway("原生请求提交状态未知".into())
                });
            }
            Ok(response)
        };
        timeout(ttfb_timeout, operation)
            .await
            .map_err(|_| AppError::BadGateway("原生请求响应超时，提交状态未知".into()))?
    }

    /// 提交冻结请求；调用者必须已经完成本次准入。
    /// @param prepared 不再改写的协议元信息。
    /// @param body 不再修改的原始传输字节。
    /// @param ttfb_timeout 从本次提交开始的响应头期限。
    /// @return 上游响应或提交未知错误；不会自动重放。
    pub async fn submit(
        self,
        mut prepared: PreparedRequest,
        body: &[u8],
        ttfb_timeout: Duration,
    ) -> Result<reqwest::Response, AppError> {
        let PreparedRoute::V3 { client, binding } = self.route else {
            let PreparedRoute::Direct(proxy_url) = self.route else {
                unreachable!()
            };
            return timeout(
                ttfb_timeout,
                send_direct(&proxy_url, &prepared, body.to_vec()),
            )
            .await
            .map_err(|_| AppError::BadGateway("upstream TTFB timeout".into()))?;
        };
        if prepared.request_id.is_empty() {
            prepared.request_id = Uuid::new_v4().to_string();
        } else if Uuid::parse_str(&prepared.request_id).is_err() {
            return Err(AppError::BadRequest("内部请求标识无效".into()));
        }
        prepared.deadline_unix_ms = now_ms() + ttfb_timeout.as_millis() as u64;
        prepared.account_id = self.account_id;
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
}

/// 兼容发送入口，环境准备与原有 TTFB 分开计时。
/// @param account 已选定账号。
/// @param prepared 冻结的传输元信息。
/// @param body 原始 body 字节。
/// @param ttfb_timeout 提交后的响应头期限。
/// @return 原有传输响应或错误。
pub async fn send(
    account: &Account,
    prepared: PreparedRequest,
    body: &[u8],
    ttfb_timeout: Duration,
) -> Result<reqwest::Response, AppError> {
    prepare(account, Duration::from_secs(120))
        .await?
        .submit(prepared, body, ttfb_timeout)
        .await
}

/// 获取底座已明确配置的账号来源；legacy 继续使用联合发布身份。
/// @return 当前账号镜像的精确来源批次。
fn configured_account_release() -> String {
    if !std::env::var("V3_UPDATE_GATE_DIR")
        .unwrap_or_default()
        .is_empty()
    {
        return std::env::var("V3_ACCOUNT_RELEASE").unwrap_or_default();
    }
    std::env::var("V3_RELEASE").unwrap_or_default()
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
    native_pool: Arc<NativePool>,
    native_transition: Arc<RwLock<()>>,
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

/// 返回当前 Unix 毫秒，用于私有请求的同一时钟截止。@return UTC 毫秒。
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// 核对私有请求归属、时限和目标，并冻结本次单调截止。
/// @param prepared 冻结元信息。
/// @param config 当前账号绑定。
/// @return 剩余预算不超过120秒的单调截止；过期或超出时钟偏差时拒绝。
pub(crate) fn validate_request(
    prepared: &PreparedRequest,
    config: &SenderConfig,
) -> Result<Instant, AppError> {
    let monotonic_now = Instant::now();
    validate_request_at(prepared, config, now_ms()).map(|budget| monotonic_now + budget)
}

fn validate_request_at(
    prepared: &PreparedRequest,
    config: &SenderConfig,
    wall_now_ms: u64,
) -> Result<Duration, AppError> {
    let url = reqwest::Url::parse(&prepared.target_url)
        .map_err(|_| AppError::BadRequest("invalid upstream URL".into()))?;
    let allowed = reqwest::Url::parse(&config.allowed_origin)
        .map_err(|_| AppError::ServiceUnavailable("invalid allowed origin".into()))?;
    if Uuid::parse_str(&prepared.request_id).is_err()
        || prepared.deadline_unix_ms <= wall_now_ms
        || prepared.deadline_unix_ms > wall_now_ms.saturating_add(REQUEST_BUDGET_MS + CLOCK_SKEW_MS)
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
    // 跨进程的墙钟可能回拨；偏差只用于接纳，实际等待仍钳制为原预算。
    Ok(Duration::from_millis(
        prepared
            .deadline_unix_ms
            .saturating_sub(wall_now_ms)
            .min(REQUEST_BUDGET_MS),
    ))
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
            let deadline = validate_request(&prepared, &config)?;
            state.active.fetch_add(1, Ordering::SeqCst);
            let guard = ActiveRequest(state.active.clone());
            submitted = true;
            let upstream = timeout_at(
                deadline,
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

async fn native_status(
    State(state): State<SenderState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AppError> {
    let config = read_config(&state.config_path)?;
    authenticate(&headers, &config)?;
    state.native_pool.verify(
        config
            .native
            .as_ref()
            .ok_or_else(|| AppError::ServiceUnavailable("未配置原生运行时".into()))?,
    )?;
    if state.evidence_error.load(Ordering::SeqCst) {
        return Err(AppError::ServiceUnavailable("账号证据写入异常".into()));
    }
    Ok(Json(
        serde_json::json!({"raw_input_protocol":1,"version":"2.1.293","environment_id":config.binding.environment_id,"generation":config.binding.generation,
            "release":config.native.as_ref().and_then(|native| native.release.as_ref()),
            "tls_observer":state.native_pool.tls_status(&config)}),
    ))
}

/// 在现有服务鉴权与绑定边界内执行零模型准备。
/// @param state sender 状态。
/// @param headers 服务鉴权头。
/// @param preparation 账号身份和准备截止。
/// @return 实际进程握手结果。
async fn native_prepare(
    State(state): State<SenderState>,
    headers: HeaderMap,
    Json(preparation): Json<NativePreparation>,
) -> Result<Json<NativePreparationStatus>, AppError> {
    let _guard = state.native_transition.read().await;
    prepare_sender(&state, &headers, preparation, None).await
}

/// 在私有鉴权和精确绑定内只检查原生池，页面检查不触发进程启动。
/// @param state sender 状态。
/// @param headers 服务鉴权头。
/// @param preparation 当前身份和检查截止。
/// @return 实时进程状态。
async fn native_readiness(
    State(state): State<SenderState>,
    headers: HeaderMap,
    Json(preparation): Json<NativePreparation>,
) -> Result<Json<NativeReadinessStatus>, AppError> {
    let config = read_config(&state.config_path)?;
    authenticate(&headers, &config)?;
    if state.evidence_error.load(Ordering::SeqCst) {
        return Err(AppError::ServiceUnavailable("账号证据写入异常".into()));
    }
    let identity = preparation_identity(preparation, &config);
    let deadline = validate_request(&identity.request, &config)?;
    let status = timeout_at(deadline, state.native_pool.readiness(&config, &identity))
        .await
        .map_err(|_| AppError::ServiceUnavailable("原生检查超时".into()))??;
    if serde_json::to_vec(&read_config(&state.config_path)?).ok()
        != serde_json::to_vec(&config).ok()
    {
        return Err(AppError::ServiceUnavailable(
            "原生检查期间环境配置已变化".into(),
        ));
    }
    Ok(Json(status))
}

/// 专用切换预热在 sender 内排除已入场流和迟到的普通准备。
/// @param state sender 状态。
/// @param headers 私有服务鉴权。
/// @param input 精确操作代次、内核引用和账号身份。
/// @return 候选真实预热结果。
async fn native_prepare_switch(
    State(state): State<SenderState>,
    headers: HeaderMap,
    Json(input): Json<NativeKernelPreparation>,
) -> Result<Json<NativePreparationStatus>, AppError> {
    let config = read_config(&state.config_path)?;
    authenticate(&headers, &config)?;
    let _transition = timeout(Duration::from_secs(10), state.native_transition.write())
        .await
        .map_err(|_| AppError::ServiceUnavailable("发送器仍有原生请求或准备".into()))?;
    prepare_sender(&state, &headers, input.identity, Some(input.target)).await
}

async fn prepare_sender(
    state: &SenderState,
    headers: &HeaderMap,
    preparation: NativePreparation,
    target: Option<NativeKernelTarget>,
) -> Result<Json<NativePreparationStatus>, AppError> {
    let config = read_config(&state.config_path)?;
    authenticate(headers, &config)?;
    if state.evidence_error.load(Ordering::SeqCst) {
        return Err(AppError::ServiceUnavailable("账号证据写入异常".into()));
    }
    if let Some(target) = &target {
        let native = config
            .native
            .as_ref()
            .ok_or_else(|| AppError::ServiceUnavailable("未配置原生运行时".into()))?;
        if state.active.load(Ordering::SeqCst) != 0
            || native.release != target.release
            || native.kernel_generation.as_deref() != Some(target.kernel_generation.as_str())
        {
            return Err(AppError::ServiceUnavailable(
                "候选内核或发送器活动状态已变化".into(),
            ));
        }
    }
    state.native_pool.verify(
        config
            .native
            .as_ref()
            .ok_or_else(|| AppError::ServiceUnavailable("未配置原生运行时".into()))?,
    )?;
    // 这里只借用既有绑定与截止校验结构；input 没有业务正文，也不会进入 execute。
    let identity = preparation_identity(preparation, &config);
    let deadline = validate_request(&identity.request, &config)?;
    let status = if target.is_some() {
        state
            .native_pool
            .prepare_switch(config.clone(), identity, deadline)
            .await?
    } else {
        state
            .native_pool
            .prepare(config.clone(), identity, deadline)
            .await?
    };
    let current = read_config(&state.config_path)?;
    if serde_json::to_vec(&current).ok() != serde_json::to_vec(&config).ok() {
        return Err(AppError::ServiceUnavailable(
            "原生准备期间环境配置已变化".into(),
        ));
    }
    Ok(Json(status))
}

/// 准备和只读检查使用同一身份信封，避免字段映射分叉。
fn preparation_identity(preparation: NativePreparation, config: &SenderConfig) -> NativeSubmission {
    NativeSubmission {
        cache_ttl_rewrite: None,
        ingress_capture: None,
        log_mode: RequestLogMode::Info,
        request: PreparedRequest {
            account_id: preparation.account_id,
            environment_id: preparation.environment_id,
            generation: preparation.generation,
            request_id: preparation.preparation_id.clone(),
            deadline_unix_ms: preparation.deadline_unix_ms,
            method: "POST".into(),
            target_url: format!("{}/v1/messages", config.allowed_origin),
            headers: Vec::new(),
            preserve_header_casing: false,
            tls_profile: TlsProfileId::default(),
        },
        input: serde_json::json!({"schema_version":1,"request_id":preparation.preparation_id}),
        bearer: preparation.bearer,
        device_id: preparation.device_id,
        account_uuid: preparation.account_uuid,
        organization_uuid: preparation.organization_uuid,
        email: preparation.email,
        concurrency: preparation.concurrency,
    }
}

/// 只取消本账号及当前环境代次的准备。
/// @param state sender 状态。
/// @param headers 服务鉴权头。
/// @param control 准备归属和 UUID。
/// @return 已登记撤销，覆盖准备尚未到达的情况。
async fn native_prepare_cancel(
    State(state): State<SenderState>,
    headers: HeaderMap,
    Json(control): Json<NativePreparationControl>,
) -> Result<Json<serde_json::Value>, AppError> {
    let config = read_config(&state.config_path)?;
    authenticate(&headers, &config)?;
    if control.account_id != config.binding.account_id
        || control.environment_id != config.binding.environment_id
        || control.generation != config.binding.generation
        || Uuid::parse_str(&control.preparation_id).is_err()
    {
        return Err(AppError::BadRequest("准备取消归属不匹配".into()));
    }
    if !state.native_pool.cancel_preparation(&control) {
        return Err(AppError::ServiceUnavailable("原生准备撤销登记已满".into()));
    }
    Ok(Json(serde_json::json!({"cancelled":true})))
}

async fn native_forward(
    State(state): State<SenderState>,
    headers: HeaderMap,
    Json(submission): Json<NativeSubmission>,
) -> Response {
    let mut submitted = false;
    let operation = async {
        let transition = state.native_transition.clone().read_owned().await;
        let config = read_config(&state.config_path)?;
        authenticate(&headers, &config)?;
        let deadline = validate_request(&submission.request, &config)?;
        let target = reqwest::Url::parse(&submission.request.target_url)
            .map_err(|_| AppError::BadRequest("原生目标无效".into()))?;
        if submission.request.method != "POST"
            || target.path() != "/v1/messages"
            || target.query().is_some()
        {
            return Err(AppError::BadRequest("原生方式只支持 Messages".into()));
        }
        if state.evidence_error.load(Ordering::SeqCst) {
            return Err(AppError::ServiceUnavailable("账号证据写入异常".into()));
        }
        state.native_pool.verify(
            config
                .native
                .as_ref()
                .ok_or_else(|| AppError::ServiceUnavailable("未配置原生运行时".into()))?,
        )?;
        state.active.fetch_add(1, Ordering::SeqCst);
        let guard = ActiveRequest(state.active.clone());
        submitted = true;
        let response = state
            .native_pool
            .submit_validated(config.clone(), submission.clone(), deadline)
            .await?;
        // 共用额度证据格式，进程池和响应流由同一活动许可覆盖。
        {
            let _lock = state.evidence_lock.lock().unwrap();
            if record_response_headers(
                &config,
                &submission.request,
                response.status().as_u16(),
                response.headers(),
            )
            .is_err()
            {
                state.evidence_error.store(true, Ordering::SeqCst);
            }
        }
        let (parts, body) = response.into_parts();
        let stream = unfold(
            (body.into_data_stream(), guard, transition),
            |(mut stream, guard, transition)| async move {
                stream
                    .next()
                    .await
                    .map(|item| (item, (stream, guard, transition)))
            },
        );
        Ok::<Response, AppError>(Response::from_parts(parts, Body::from_stream(stream)))
    };
    operation.await.unwrap_or_else(|error| {
        // 准备类错误在写入业务指令前产生；活动计数已经覆盖启动阶段。
        let before_send = !submitted
            || matches!(
                &error,
                AppError::ServiceUnavailable(_) | AppError::BadRequest(_) | AppError::Unauthorized
            );
        let mut response = error.into_response();
        response.headers_mut().insert(
            ERROR_HEADER,
            if before_send {
                "before-send"
            } else {
                "uncertain"
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
        .route("/internal/native/status", get(native_status))
        .route("/internal/native/prepare", post(native_prepare))
        .route("/internal/native/readiness", post(native_readiness))
        .route(
            "/internal/native/prepare-switch",
            post(native_prepare_switch),
        )
        .route(
            "/internal/native/prepare/cancel",
            post(native_prepare_cancel),
        )
        .route("/internal/native/messages", post(native_forward))
        .route("/internal/status", get(status))
        .layer(DefaultBodyLimit::max(128 * 1024 * 1024))
        .with_state(SenderState {
            config_path,
            native_pool: Arc::new(NativePool::default()),
            native_transition: Arc::new(RwLock::new(())),
            active: Arc::new(AtomicU64::new(0)),
            evidence_error: Arc::new(AtomicBool::new(false)),
            evidence_lock: Arc::new(Mutex::new(())),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::tlsfp::{parse_test_clienthello, test_request_client};
    use rustls_pki_types::CertificateDer;
    use serde_json::{Value, json};
    use std::env::temp_dir;
    use std::io::{BufRead, BufReader};
    use std::net::{Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn sender_preserves_tls_profile_legacy_default_and_rejects_unknown_before_submission() {
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
        let capture = tokio::spawn(async move {
            let mut records = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = proxy.accept().await.unwrap();
                let mut connect = Vec::new();
                while !connect.ends_with(b"\r\n\r\n") {
                    assert!(connect.len() < 8192);
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await.unwrap();
                    connect.extend_from_slice(&byte);
                }
                assert!(connect.starts_with(b"CONNECT api.anthropic.com:443 "));
                stream
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await
                    .unwrap();
                let mut header = [0; 5];
                stream.read_exact(&mut header).await.unwrap();
                let size = usize::from(u16::from_be_bytes([header[3], header[4]]));
                assert!(size <= 16384);
                let mut wire = header.to_vec();
                wire.resize(5 + size, 0);
                stream.read_exact(&mut wire[5..]).await.unwrap();
                records.push(wire);
            }
            // 不再接受上游；若未知 ID 错误放行，将表现为提交失败而不是预期的本地400。
            records
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let config = SenderConfig {
            native: None,
            binding: TransportBinding {
                account_id: 7,
                environment_id: "synthetic-tls".into(),
                generation: 1,
                endpoint: endpoint.clone(),
                secret: "synthetic-private-sender-key".repeat(3),
                release_id: "test".into(),
            },
            proxy_url,
            allowed_origin: "https://api.anthropic.com".into(),
            evidence_file: None,
            native_capture_dir: None,
            native_adaptive_prewarm: false,
            quota_identity: String::new(),
        };
        let path = temp_dir().join(format!("sender-tls-{}.json", Uuid::new_v4()));
        fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
        let app = sender_router(path.to_string_lossy().into_owned());
        let sender = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let request = PreparedRequest {
            account_id: 7,
            environment_id: config.binding.environment_id.clone(),
            generation: 1,
            request_id: Uuid::new_v4().to_string(),
            deadline_unix_ms: now_ms() + 30_000,
            method: "POST".into(),
            target_url: "https://api.anthropic.com/v1/messages".into(),
            headers: vec![("Content-Type".into(), "application/json".into())],
            preserve_header_casing: true,
            tls_profile: TlsProfileId::default(),
        };
        let client = Client::builder().no_proxy().build().unwrap();
        for profile in [
            Some(TlsProfileId::ClaudeCode293),
            Some(TlsProfileId::TestReordered),
            None,
        ] {
            let mut meta = serde_json::to_value(&request).unwrap();
            match profile {
                Some(profile) => meta["tls_profile"] = serde_json::to_value(profile).unwrap(),
                None => {
                    meta.as_object_mut().unwrap().remove("tls_profile");
                }
            }
            let response = client
                .post(format!("{endpoint}/internal/send"))
                .bearer_auth(&config.binding.secret)
                .header(
                    META_HEADER,
                    STANDARD.encode(serde_json::to_vec(&meta).unwrap()),
                )
                .body("{}")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), 502);
        }
        let records = timeout(Duration::from_secs(5), capture)
            .await
            .unwrap()
            .unwrap();
        let parsed: Vec<Value> = records
            .iter()
            .map(|wire| parse_test_clienthello(wire))
            .collect();
        assert_eq!(parsed[0]["ja3"]["md5"], "1523504b38f0fae0d881d4b6554aac1b");
        assert_ne!(parsed[0]["ja3"], parsed[1]["ja3"]);
        assert_eq!(parsed[0]["ja3"], parsed[2]["ja3"]);
        let mut meta = serde_json::to_value(&request).unwrap();
        meta["tls_profile"] = json!("unknown-unregistered");
        let response = client
            .post(format!("{endpoint}/internal/send"))
            .bearer_auth(&config.binding.secret)
            .header(
                META_HEADER,
                STANDARD.encode(serde_json::to_vec(&meta).unwrap()),
            )
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 400);
        assert_eq!(response.headers()[ERROR_HEADER], "before-send");
        let status: Value = client
            .get(format!("{endpoint}/internal/status"))
            .bearer_auth(&config.binding.secret)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status["active"], 0);
        sender.abort();
        fs::remove_file(path).unwrap();
    }

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

        fn client(&self, trusted: bool, hostname: &str) -> Client {
            let certificate =
                trusted.then(|| CertificateDer::from(fs::read(self.root.join("ca.der")).unwrap()));
            test_request_client(certificate, hostname, self.address).unwrap()
        }

        fn request(&self, hostname: &str, path: &str) -> PreparedRequest {
            PreparedRequest {
                tls_profile: TlsProfileId::default(),
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
            native: None,
            binding,
            proxy_url: String::new(),
            allowed_origin: "https://api.anthropic.com".into(),
            evidence_file: None,
            native_capture_dir: None,
            native_adaptive_prewarm: false,
            quota_identity: String::new(),
        };
        let mut prepared = PreparedRequest {
            tls_profile: TlsProfileId::default(),
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
        // 真实宿主采集到约1.3秒回拨；相同合法信封在接收时仍应保持有界预算。
        let issued = 1_800_000_000_000;
        prepared.deadline_unix_ms = issued + 120_000;
        assert_eq!(
            validate_request_at(&prepared, &config, issued - 1275).unwrap(),
            Duration::from_secs(120)
        );
        assert_eq!(
            validate_request_at(&prepared, &config, issued - CLOCK_SKEW_MS).unwrap(),
            Duration::from_secs(120)
        );
        assert!(validate_request_at(&prepared, &config, issued - CLOCK_SKEW_MS - 1).is_err());
        assert!(validate_request_at(&prepared, &config, issued + 120_000).is_err());
        assert!(validate_request_at(&prepared, &config, issued + 120_001).is_err());
        assert_eq!(
            validate_request_at(&prepared, &config, issued + 119_000).unwrap(),
            Duration::from_secs(1)
        );
        prepared.deadline_unix_ms = now_ms() + 30_000;
        prepared.generation = 1;
        assert!(validate_request(&prepared, &config).is_err());
        prepared.generation = 2;
        prepared.target_url = "http://169.254.169.254/api/metadata".into();
        assert!(validate_request(&prepared, &config).is_err());
    }
    /// 冷环境 HTTP 请求同时就绪后，真实提交仍经过同一账号闸门，压缩流字节不改变。
    #[tokio::test]
    async fn cold_environment_ready_cohort_still_submits_smoothly() {
        use crate::service::v3_scheduling::{RequestPhase, V3Scheduler};
        use crate::store::scheduling_store::SchedulingStore;
        use flate2::{Compression, write::GzEncoder};
        use futures_util::future::join_all;
        use sqlx::any::AnyPoolOptions;
        use std::sync::atomic::AtomicUsize;
        use std::time::Instant as StdInstant;
        use tokio::sync::Barrier;

        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE accounts(id BIGINT PRIMARY KEY,rpm_limit INTEGER NOT NULL DEFAULT 300)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO accounts(id) VALUES(7)")
            .execute(&pool)
            .await
            .unwrap();
        SchedulingStore::migrate(&pool).await.unwrap();
        let scheduler = Arc::new(V3Scheduler::new(Arc::new(SchedulingStore::new(
            pool.clone(),
        ))));
        let account:Account=serde_json::from_value(json!({"id":7,"name":"合成账号","email":"runtime@example.invalid","status":"active","device_id":"synthetic","canonical_env":{},"canonical_prompt_env":{},"canonical_process":{},"billing_mode":"strip","concurrency":4,"rpm_limit":300,"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()})).unwrap();
        let ip = Ipv4Addr::new(
            127,
            77,
            rand::random::<u8>().max(1),
            rand::random::<u8>().max(1),
        );
        let listener = tokio::net::TcpListener::bind(SocketAddr::from((ip, 8091)))
            .await
            .unwrap();
        let deployment = "synthetic-deployment";
        let release = "synthetic-release";
        let env = "abcdef0123456789abcdef0123456789";
        let hash = format!("{:x}", Sha256::digest(deployment.as_bytes()));
        let host = format!("v3-{}-account-{env}", &hash[..12]);
        let binding = TransportBinding {
            account_id: 7,
            environment_id: env.into(),
            generation: 3,
            endpoint: format!("http://{host}:8091"),
            secret: "s".repeat(32),
            release_id: release.into(),
        };
        let ready = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(5));
        let received = Arc::new(Mutex::new(Vec::<StdInstant>::new()));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(b"data: {\"type\":\"message_stop\"}\n\n")
            .unwrap();
        let compressed = encoder.finish().unwrap();
        let app = Router::new()
            .route(
                "/internal/v3/transport/:id",
                get({
                    let binding = binding.clone();
                    let ready = ready.clone();
                    let barrier = barrier.clone();
                    move || {
                        let binding = binding.clone();
                        let ready = ready.clone();
                        let barrier = barrier.clone();
                        async move {
                            ready.fetch_add(1, Ordering::SeqCst);
                            barrier.wait().await;
                            Json(binding)
                        }
                    }
                }),
            )
            .route(
                "/internal/send",
                post({
                    let received = received.clone();
                    let compressed = compressed.clone();
                    move |headers: HeaderMap, body: Bytes| {
                        let received = received.clone();
                        let compressed = compressed.clone();
                        async move {
                            let meta: PreparedRequest = serde_json::from_slice(
                                &STANDARD.decode(headers[META_HEADER].as_bytes()).unwrap(),
                            )
                            .unwrap();
                            assert_eq!(meta.account_id, 7);
                            assert_eq!(meta.generation, 3);
                            assert_eq!(meta.target_url, "https://synthetic.invalid/v1/messages");
                            assert_eq!(body.as_ref(), b"{\"synthetic\":true}");
                            received.lock().unwrap().push(StdInstant::now());
                            (
                                [
                                    ("content-type", "text/event-stream"),
                                    ("content-encoding", "gzip"),
                                ],
                                compressed,
                            )
                                .into_response()
                        }
                    }
                }),
            );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = Client::builder()
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .resolve(&host, SocketAddr::from((ip, 8091)))
            .build()
            .unwrap();
        let base = format!("http://{ip}:8091");
        let mut jobs = Vec::new();
        for _ in 0..4 {
            let scheduler = scheduler.clone();
            let account = account.clone();
            let client = client.clone();
            let base = base.clone();
            jobs.push(tokio::spawn(async move {
                let request = scheduler.begin(1, None);
                request.selected(&account, None);
                request.phase(RequestPhase::Preparing);
                let transport = prepare_from_runtime(
                    &account,
                    client,
                    &base,
                    "synthetic-control",
                    deployment,
                    release,
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
                let permit = request.admit(&account).await.unwrap();
                let prepared = PreparedRequest {
                    account_id: 7,
                    environment_id: String::new(),
                    generation: 0,
                    request_id: Uuid::new_v4().to_string(),
                    deadline_unix_ms: 0,
                    method: "POST".into(),
                    target_url: "https://synthetic.invalid/v1/messages".into(),
                    headers: vec![("Content-Type".into(), "application/json".into())],
                    preserve_header_casing: true,
                    tls_profile: TlsProfileId::default(),
                };
                permit.submitted();
                transport
                    .submit(prepared, b"{\"synthetic\":true}", Duration::from_secs(5))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
            }));
        }
        timeout(Duration::from_secs(5), async {
            while ready.load(Ordering::SeqCst) < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(scheduler.snapshot(7).events.is_empty());
        barrier.wait().await;
        for result in join_all(jobs).await {
            assert_eq!(result.unwrap().as_ref(), compressed.as_slice());
        }
        let observed = received.lock().unwrap();
        assert_eq!(observed.len(), 4);
        assert!(observed[3].duration_since(observed[0]) >= Duration::from_millis(400));
        let events = scheduler.snapshot(7).events;
        assert_eq!(events.len(), 4);
        assert!(
            events
                .windows(2)
                .all(|pair| pair[1].submitted_at_ms - pair[0].submitted_at_ms >= 195)
        );
        server.abort();
    }
}
