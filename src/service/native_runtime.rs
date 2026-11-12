//! 固定 293 原生请求池；私有 IPC 只负责单次执行，业务准入由网关持有。

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use futures_util::FutureExt;
use http_body::Body as HttpBody;
use libc::{F_GET_SEALS, F_SEAL_GROW, F_SEAL_SEAL, F_SEAL_SHRINK, F_SEAL_WRITE, fcntl};
use percent_encoding::percent_decode_str;
use reqwest::Url;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, copy_bidirectional};
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{RwLock, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, sleep_until, timeout, timeout_at};
use tokio_socks::{TargetAddr, tcp::Socks5Stream};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::error::AppError;
use crate::model::request_diagnostics::RequestLogMode;
use crate::model::request_diagnostics::{
    DiagnosticParameters, NATIVE_OBSERVATION_HEADER, NativeRequestObservation,
    NativeRuntimeObservation,
};
use crate::service::native_contract::{
    NativePreparationControl, NativePreparationStatus, NativeReadinessStatus,
};
pub use crate::service::native_contract::{
    NativeSenderConfig, NativeSubmission, POOL_MEMORY_MIB, WORKER_MEMORY_MIB,
};
use crate::service::native_release::ReleaseArtifact;
use crate::service::rewriter::{CacheControlTtlRewrite, extract_session_id_from_body};
use crate::service::runtime_transport::{
    CLOCK_SKEW_MS, REQUEST_BUDGET_MS, SenderConfig, now_ms, validate_request,
};

const LAUNCHER_SHA256: &str = "64c61cae565e9097de18cf61a35f46e7595f633cb14f7d6b8c6609fcd294ce89";
const IMAGE_SHA256: &str = "af4cea9e21aa959c5e39c389173ba30ebb19b354b67acfe99164946450e9776d";
const IMAGE_BYTES: u64 = 112_839_864;
const FRAME_LIMIT: usize = 32 * 1024 * 1024;
fn unavailable() -> AppError {
    AppError::ServiceUnavailable("原生运行时准备失败，请核对能力、身份和资源配置".into())
}

fn protocol_error() -> AppError {
    AppError::BadGateway("原生运行时通道异常，请求提交状态未知".into())
}

fn digest_file(file: &mut File) -> io::Result<String> {
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

// 能力查询与新进程启动共用资源校验，暖路径仍逐次检查预算。
fn validate_distribution_config(config: &NativeSenderConfig) -> Result<(), AppError> {
    if config.max_workers == 0
        || (config.memory_mib != 0
            && config.max_workers as u64
                > config.memory_mib.saturating_sub(POOL_MEMORY_MIB) / WORKER_MEMORY_MIB)
        || !Path::new(&config.binary_path).is_absolute()
        || !Path::new(&config.work_root).is_absolute()
    {
        return Err(unavailable());
    }
    Ok(())
}

/// 完整核验新进程使用的登记分发体，返回精确只读句柄。
/// @param config 冻结的部署配置。
/// @return 经完整摘要验证的文件。
pub(crate) fn verify_distribution(config: &NativeSenderConfig) -> Result<File, AppError> {
    validate_distribution_config(config)?;
    verify_artifact(&artifact(config)?)
}

fn artifact(config: &NativeSenderConfig) -> Result<ReleaseArtifact, AppError> {
    match &config.release {
        Some(reference) => ReleaseArtifact::signed(reference),
        None => Ok(ReleaseArtifact {
            path: PathBuf::from(&config.binary_path),
            launcher_sha256: LAUNCHER_SHA256.into(),
            launcher_bytes: 27_388_048,
            image_sha256: IMAGE_SHA256.into(),
            image_bytes: IMAGE_BYTES,
        }),
    }
}

fn verify_artifact(artifact: &ReleaseArtifact) -> Result<File, AppError> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(&artifact.path)
        .map_err(|_| unavailable())?;
    let metadata = file.metadata().map_err(|_| unavailable())?;
    if !metadata.is_file()
        || metadata.len() != artifact.launcher_bytes
        || metadata.mode() & 0o022 != 0
        || digest_file(&mut file).map_err(|_| unavailable())? != artifact.launcher_sha256
    {
        return Err(unavailable());
    }
    Ok(file)
}

/// 完整核验时冻结的文件身份，包含无法通过恢复 mtime 隐去的 ctime。
#[derive(PartialEq, Eq)]
struct DistributionIdentity {
    dev: u64,
    ino: u64,
    len: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

impl From<fs::Metadata> for DistributionIdentity {
    fn from(value: fs::Metadata) -> Self {
        Self {
            dev: value.dev(),
            ino: value.ino(),
            len: value.len(),
            mode: value.mode(),
            uid: value.uid(),
            gid: value.gid(),
            mtime: (value.mtime(), value.mtime_nsec()),
            ctime: (value.ctime(), value.ctime_nsec()),
        }
    }
}

/// 仅缓存最后一份已核验且保持打开的分发体，大小恒定有界。
struct VerifiedDistribution {
    path: PathBuf,
    identity: DistributionIdentity,
    sha256: String,
    file: File,
}

fn validate_worker_identity(
    config: &SenderConfig,
    submission: &NativeSubmission,
) -> Result<usize, AppError> {
    CacheControlTtlRewrite::parse(submission.cache_ttl_rewrite.as_deref().unwrap_or("off"))?;
    if config.allowed_origin != "https://api.anthropic.com" {
        return Err(unavailable());
    }
    let native = config.native.as_ref().ok_or_else(unavailable)?;
    validate_distribution_config(native)?;
    let slots = usize::try_from(submission.concurrency)
        .ok()
        .and_then(|v| v.checked_mul(2))
        .filter(|v| *v > 0 && *v <= native.max_workers)
        .ok_or_else(unavailable)?;
    if submission.bearer.is_empty()
        || submission.device_id.len() != 64
        || !submission
            .device_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || submission.input["request_id"] != submission.request.request_id
        || submission.input["schema_version"] != 1
    {
        return Err(unavailable());
    }
    Ok(slots)
}

/// 为准备、巡检和业务计算同一私有身份，秘密摘要仅用于池内隔离。
fn pool_identity(
    config: &SenderConfig,
    submission: &NativeSubmission,
) -> Result<(String, usize), AppError> {
    let slots = validate_worker_identity(config, submission)?;
    // 额度观察身份在首条业务才补写，它不参与进程启动，不能使已预热的进程换代。
    let identity_config = SenderConfig {
        native_adaptive_prewarm: false,
        quota_identity: String::new(),
        ..config.clone()
    };
    let key = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                &identity_config,
                &submission.bearer,
                &submission.device_id,
                &submission.account_uuid,
                &submission.organization_uuid,
                &submission.email,
                slots
            ))
            .map_err(|_| unavailable())?
        )
    );
    Ok((key, slots))
}

/// 池的当前代次与物理进程计数，仅在短互斥区内变更。
#[derive(Default)]
struct PoolState {
    key: String,
    live: usize,
    live_by_key: HashMap<String, usize>,
    starting: usize,
    idle: Vec<Worker>,
    reaper_running: bool,
    spare: Option<SparePreparation>,
}

/// 当前代次唯一的后台补位；丢弃撤销端会收口尚未就绪的进程。
struct SparePreparation {
    ready: watch::Receiver<()>,
    _cancel: oneshot::Sender<()>,
}

/// 启动 worker 所需的账号字段；后台补位不持有业务信封。
struct WorkerIdentity {
    bearer: String,
    device_id: String,
    account_uuid: Option<String>,
    organization_uuid: Option<String>,
    email: String,
}

impl From<&NativeSubmission> for WorkerIdentity {
    fn from(submission: &NativeSubmission) -> Self {
        Self {
            bearer: submission.bearer.clone(),
            device_id: submission.device_id.clone(),
            account_uuid: submission.account_uuid.clone(),
            organization_uuid: submission.organization_uuid.clone(),
            email: submission.email.clone(),
        }
    }
}

/// 撤销与准备共用完整归属，旧代控制不会影响新环境中的同名标识。
#[derive(Clone, Hash, PartialEq, Eq)]
struct PreparationKey {
    account_id: i64,
    environment_id: String,
    generation: u64,
    id: String,
}

/// 活动控制信号与有界的短期撤销记录。
enum PreparationState {
    Active(oneshot::Sender<()>),
    Cancelled(Instant),
}

/// 一个账号 sender 的按需进程池；互斥锁只保护本地所有权变更。
pub struct NativePool {
    state: Mutex<PoolState>,
    generation: tokio::sync::Mutex<()>,
    image: tokio::sync::Mutex<Option<(String, Arc<File>)>>,
    idle_timeout: Duration,
    verified: Mutex<Option<VerifiedDistribution>>,
    warmup: RwLock<()>,
    preparations: Mutex<HashMap<PreparationKey, PreparationState>>,
}

impl Default for NativePool {
    /// 创建按需池，额外暖进程空闲60秒后回收。@return 空池及独立映像缓存。
    fn default() -> Self {
        Self {
            state: Mutex::new(PoolState::default()),
            generation: tokio::sync::Mutex::new(()),
            image: tokio::sync::Mutex::new(None),
            idle_timeout: Duration::from_secs(60),
            verified: Mutex::new(None),
            warmup: RwLock::new(()),
            preparations: Mutex::new(HashMap::new()),
        }
    }
}

/// 准备完成、异常或 future 取消时移除相应控制登记。
struct PreparationLease {
    pool: Weak<NativePool>,
    key: PreparationKey,
}

impl Drop for PreparationLease {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let mut pending = pool.preparations.lock().unwrap();
            if matches!(pending.get(&self.key), Some(PreparationState::Active(_))) {
                pending.remove(&self.key);
            }
        }
    }
}

/// 物理进程计数覆盖启动、活动及空闲，故障和取消均自动归还。
struct WorkerLease {
    pool: Weak<NativePool>,
    key: String,
}

/// 启动统计跟随 future 生命周期归还，取消时不会遗留 preparing。
struct StartupLease(Weak<NativePool>);

impl Drop for StartupLease {
    fn drop(&mut self) {
        if let Some(pool) = self.0.upgrade() {
            pool.state.lock().unwrap().starting -= 1;
        }
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let mut state = pool.state.lock().unwrap();
            state.live -= 1;
            if let Some(count) = state.live_by_key.get_mut(&self.key) {
                *count -= 1;
                if *count == 0 {
                    state.live_by_key.remove(&self.key);
                }
            }
        }
    }
}

/// 一个物理进程的私有管道、目录、映像和生命周期归属。
struct Worker {
    tls_observer: Option<super::native_tls_observer::NativeTlsSlot>,
    id: String,
    cache_ttl_rewrite_supported: bool,
    child: Option<Child>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    root: PathBuf,
    _image: File,
    execution_image: Option<Arc<File>>,
    idle_since: Instant,
    _lease: Option<WorkerLease>,
    _proxy: NativeProxy,
    image_sha256: String,
    ready_at: Instant,
    started_at: Instant,
    startup_ms: u64,
}

/// 生命周期结束时保留本层原因，取消 future 时也留下可关联的观察。
struct NativeExecutionLog {
    job_id: String,
    worker_id: String,
    worker_pid: Option<u32>,
    account_id: i64,
    generation: u64,
    started: Instant,
    last_event: Option<Instant>,
    last_chunk: Option<Instant>,
    chunks: u64,
    bytes: u64,
    headers_seen: bool,
    reason: &'static str,
}

impl Drop for NativeExecutionLog {
    fn drop(&mut self) {
        tracing::info!(native_job_id = %self.job_id, worker_id = %self.worker_id,
            worker_pid = self.worker_pid, account_id = self.account_id, generation = self.generation,
            layer = "native_sender", reason = self.reason, headers_seen = self.headers_seen,
            chunks = self.chunks, bytes = self.bytes, elapsed_ms = self.started.elapsed().as_millis(),
            last_event_ago_ms = self.last_event.map(|at| at.elapsed().as_millis()),
            last_chunk_ago_ms = self.last_chunk.map(|at| at.elapsed().as_millis()),
            "原生请求执行已收口");
    }
}

/// 只投影固定运行时已定义的受控错误码，未知值保持 unknown。
fn native_error_code(payload: &Value) -> &'static str {
    match payload["code"].as_str() {
        Some("request_timeout") => "request_timeout",
        Some("runtime_protocol") => "runtime_protocol",
        Some("native_configuration") => "native_configuration",
        Some("native_construction") => "native_construction",
        Some("native_identity") => "native_identity",
        Some("unsupported_parameter") => "unsupported_parameter",
        _ => "unknown",
    }
}

/// 原生 TLS 字节通过有鉴权的本地 CONNECT 转到现有 SOCKS 代理。
struct NativeProxy {
    url: String,
    task: Option<JoinHandle<()>>,
}

impl Drop for NativeProxy {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl NativeProxy {
    async fn create(proxy: &str, worker_id: &str) -> Result<Self, AppError> {
        if proxy.is_empty() {
            return Ok(Self {
                url: String::new(),
                task: None,
            });
        }
        let parsed = Url::parse(proxy).map_err(|_| unavailable())?;
        if matches!(parsed.scheme(), "http" | "https") {
            return Ok(Self {
                url: proxy.into(),
                task: None,
            });
        }
        if !matches!(parsed.scheme(), "socks5" | "socks5h") {
            return Err(unavailable());
        }
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| unavailable())?;
        let secret = Uuid::new_v4().simple().to_string();
        let url = format!(
            "http://ccrb:{secret}@{}",
            listener.local_addr().map_err(|_| unavailable())?
        );
        let authorization = format!("Basic {}", STANDARD.encode(format!("ccrb:{secret}")));
        let worker_id = worker_id.to_string();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    next = listener.accept() => {
                        let Ok((socket, _)) = next else { break; };
                        let parsed = parsed.clone(); let authorization = authorization.clone();
                        let worker_id = worker_id.clone();
                        connections.spawn(async move { let _ = native_socks_tunnel(socket, parsed, authorization, &worker_id).await; });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(Self {
            url,
            task: Some(task),
        })
    }
}

async fn native_socks_tunnel(
    mut socket: TcpStream,
    proxy: Url,
    authorization: String,
    worker_id: &str,
) -> Result<(), AppError> {
    let tunnel_id = Uuid::new_v4();
    let started = Instant::now();
    let mut phase = "connect_request";
    let operation = async {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            if head.len() >= 8192 {
                return Err(unavailable());
            }
            head.push(socket.read_u8().await.map_err(|_| unavailable())?);
        }
        let head = String::from_utf8(head).map_err(|_| unavailable())?;
        let mut lines = head.split("\r\n");
        if lines.next() != Some("CONNECT api.anthropic.com:443 HTTP/1.1") {
            return Err(unavailable());
        }
        let credentials: Vec<_> = lines
            .filter_map(|line| line.split_once(':'))
            .filter(|(name, _)| name.eq_ignore_ascii_case("proxy-authorization"))
            .collect();
        if credentials.len() != 1
            || !bool::from(
                credentials[0]
                    .1
                    .trim()
                    .as_bytes()
                    .ct_eq(authorization.as_bytes()),
            )
        {
            return Err(unavailable());
        }
        phase = "proxy_resolution";
        let endpoint = (
            proxy.host_str().ok_or_else(unavailable)?,
            proxy.port().unwrap_or(1080),
        );
        // 与旧后端保持 DNS 语义；5h 交给 SOCKS，5 在账号网络中解析。
        let target = if proxy.scheme() == "socks5h" {
            TargetAddr::Domain(Cow::Borrowed("api.anthropic.com"), 443)
        } else {
            TargetAddr::Ip(
                lookup_host(("api.anthropic.com", 443))
                    .await
                    .map_err(|_| unavailable())?
                    .next()
                    .ok_or_else(unavailable)?,
            )
        };
        let user = percent_decode_str(proxy.username())
            .decode_utf8()
            .map_err(|_| unavailable())?;
        let password = percent_decode_str(proxy.password().unwrap_or(""))
            .decode_utf8()
            .map_err(|_| unavailable())?;
        phase = "socks_connect";
        let stream = if user.is_empty() {
            Socks5Stream::connect(endpoint, target).await
        } else {
            Socks5Stream::connect_with_password(endpoint, target, &user, &password).await
        }
        .map_err(|error| {
            tracing::warn!(worker_id, %tunnel_id, layer = "native_proxy", reason = "socks_connect_error",
                error_summary = ?super::stream_error::summarize(&error), "SOCKS连接失败");
            unavailable()
        })?;
        phase = "connect_response";
        socket
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await
            .map_err(|_| unavailable())?;
        Ok::<_, AppError>(stream.into_inner())
    };
    let mut target = match timeout(Duration::from_secs(15), operation).await {
        Ok(Ok(target)) => target,
        failure => {
            tracing::warn!(worker_id, %tunnel_id, layer = "native_proxy", phase,
                reason = if failure.is_err() { "local_connect_timeout" } else { "connect_failed" },
                elapsed_ms = started.elapsed().as_millis(), "原生代理握手结束");
            return Err(unavailable());
        }
    };
    tracing::info!(worker_id, %tunnel_id, layer = "native_proxy", reason = "tunnel_established",
        elapsed_ms = started.elapsed().as_millis(), "原生代理隧道已建立");
    // 仅复制原生 TLS 密文；SDK 保持证书校验、CCH 及唯一模型发送所有权。
    match copy_bidirectional(&mut socket, &mut target).await {
        Ok((sent, received)) => {
            tracing::info!(worker_id, %tunnel_id, layer = "native_proxy", reason = "tunnel_eof",
                sent_bytes = sent, received_bytes = received, elapsed_ms = started.elapsed().as_millis(),
                "代理隧道双向复制已结束");
        }
        Err(error) => {
            tracing::warn!(worker_id, %tunnel_id, layer = "native_proxy", reason = "tunnel_io_error",
                elapsed_ms = started.elapsed().as_millis(),
                error_summary = ?super::stream_error::summarize(&error), "代理隧道复制失败");
            return Err(protocol_error());
        }
    }
    Ok(())
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(slot) = &self.tls_observer {
            slot.close();
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let root = self.root.clone();
            let lease = self._lease.take();
            // 等待已创建的子进程退出，再回收该进程专属目录。
            tokio::spawn(async move {
                let _ = child.wait().await;
                let _ = fs::remove_dir_all(root);
                drop(lease);
            });
        }
    }
}

impl Worker {
    async fn retire(mut self) {
        if let Some(slot) = &self.tls_observer {
            slot.close();
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = fs::remove_dir_all(&self.root);
        }
        self._lease.take();
    }

    async fn send(
        &mut self,
        kind: &str,
        request_id: Option<&str>,
        input: Option<&Value>,
        cache_ttl_rewrite: Option<&str>,
    ) -> Result<(), AppError> {
        let mut frame = json!({"schema_version":2,"worker_id":self.id,"type":kind});
        if let Some(id) = request_id {
            frame["request_id"] = json!(id);
        }
        if let Some(input) = input {
            frame["input"] = input.clone();
        }
        if let Some(mode) = cache_ttl_rewrite {
            frame["cache_ttl_rewrite"] = json!(mode);
        }
        let mut bytes = serde_json::to_vec(&frame).map_err(|_| protocol_error())?;
        bytes.push(b'\n');
        self.stdin
            .write_all(&bytes)
            .await
            .map_err(|_| protocol_error())
    }

    async fn next(&mut self) -> Result<Value, AppError> {
        let mut bytes = Vec::new();
        loop {
            let available = self.stdout.fill_buf().await.map_err(|error| {
                tracing::warn!(worker_id = %self.id, reason = "ipc_read_error",
                    error_summary = ?super::stream_error::summarize(&error), "原生IPC读取失败");
                protocol_error()
            })?;
            if available.is_empty() {
                let exit = self
                    .child
                    .as_mut()
                    .and_then(|child| child.try_wait().ok())
                    .flatten();
                tracing::warn!(worker_id = %self.id, reason = "ipc_eof",
                    exit_code = exit.as_ref().and_then(|status| status.code()),
                    process_exited = exit.is_some(), "原生IPC已关闭");
                return Err(protocol_error());
            }
            let count = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|i| i + 1)
                .unwrap_or(available.len());
            if bytes.len() + count > FRAME_LIMIT {
                tracing::warn!(worker_id = %self.id, reason = "ipc_frame_limit", "原生IPC帧超限");
                return Err(protocol_error());
            }
            bytes.extend_from_slice(&available[..count]);
            self.stdout.consume(count);
            if bytes.last() == Some(&b'\n') {
                break;
            }
        }
        let frame: Value = serde_json::from_slice(&bytes).map_err(|_| {
            tracing::warn!(worker_id = %self.id, reason = "ipc_invalid_json", "原生IPC帧解析失败");
            protocol_error()
        })?;
        if frame["schema_version"] != 2 || frame["worker_id"] != self.id {
            tracing::warn!(worker_id = %self.id, reason = "ipc_binding_mismatch", "原生IPC归属校验失败");
            return Err(protocol_error());
        }
        Ok(frame)
    }

    async fn start(
        config: &SenderConfig,
        identity: &WorkerIdentity,
        lease: WorkerLease,
        execution_image: Option<Arc<File>>,
        deadline: Instant,
    ) -> Result<Self, AppError> {
        let started = Instant::now();
        let native = config.native.as_ref().ok_or_else(unavailable)?;
        let artifact = artifact(native)?;
        let image = verify_artifact(&artifact)?;
        let id = Uuid::new_v4().to_string();
        let proxy = NativeProxy::create(&config.proxy_url, &id).await?;
        fs::create_dir_all(&native.work_root).map_err(|_| unavailable())?;
        fs::set_permissions(&native.work_root, fs::Permissions::from_mode(0o700))
            .map_err(|_| unavailable())?;
        let root = Path::new(&native.work_root).join(&id);
        fs::create_dir(&root).map_err(|_| unavailable())?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).map_err(|_| unavailable())?;
        let setup = (|| {
            for name in ["home", "config", "cwd"] {
                fs::create_dir(root.join(name)).map_err(|_| unavailable())?;
            }
            // 固定 293 的 ce().oauthAccount 与 tI() 分别读取账号 UUID 和 64 位 userID。
            let profile = json!({"userID":identity.device_id,"oauthAccount":{
                "accountUuid":identity.account_uuid,"organizationUuid":identity.organization_uuid,
                "emailAddress":identity.email}});
            fs::write(
                root.join("config/.claude.json"),
                serde_json::to_vec(&profile).map_err(|_| unavailable())?,
            )
            .map_err(|_| unavailable())?;
            // 共享的是封存的代码映像；每轮exec仍创建独立堆、环境和SDK。
            let entry = execution_image.as_deref().unwrap_or(&image);
            let mut command = Command::new(format!(
                "/proc/{}/fd/{}",
                std::process::id(),
                entry.as_raw_fd()
            ));
            command
                .arg("--version")
                .env_clear()
                .current_dir(root.join("cwd"))
                .env("HOME", root.join("home"))
                .env("CLAUDE_CONFIG_DIR", root.join("config"))
                .env("PATH", "/usr/bin:/bin")
                .env("LANG", "C.UTF-8")
                .env("CCRB_WORKER_ID", &id)
                .env("CCRB_CACHE_POLICY", "preserve")
                .env("CCRB_TELEMETRY", "off")
                .env("CCRB_NATIVE_INGRESS_POLICY", "local_base_url")
                .env("CLAUDE_CODE_ENTRYPOINT", "cli")
                .env("DISABLE_AUTO_COMPACT", "1")
                .env("CLAUDE_CODE_GATEWAY_HINT_HEADERS", "1")
                .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
                .env("DISABLE_AUTOUPDATER", "1")
                .env("DISABLE_TELEMETRY", "1")
                .env("DISABLE_ERROR_REPORTING", "1")
                .env("CLAUDE_CODE_OAUTH_TOKEN", &identity.bearer)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            if !proxy.url.is_empty() {
                command
                    .env("HTTP_PROXY", &proxy.url)
                    .env("HTTPS_PROXY", &proxy.url);
            }
            #[cfg(test)]
            if let Ok(ca) = std::env::var("CCRB_TEST_CA_FILE") {
                command.env("NODE_EXTRA_CA_CERTS", ca);
            }
            #[cfg(feature = "v3-verification")]
            {
                let der = fs::read("/run/v3/verification-ca.der").map_err(|_| unavailable())?;
                let pem = format!(
                    "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
                    STANDARD.encode(der)
                );
                let ca = root.join("ca.pem");
                fs::write(&ca, pem).map_err(|_| unavailable())?;
                command.env("NODE_EXTRA_CA_CERTS", ca);
            }
            command.spawn().map_err(|_| unavailable())
        })();
        let mut child = match setup {
            Ok(child) => child,
            Err(error) => {
                let _ = fs::remove_dir_all(&root);
                return Err(error);
            }
        };
        let stdin = child.stdin.take().ok_or_else(unavailable)?;
        let stdout = BufReader::new(child.stdout.take().ok_or_else(unavailable)?);
        let mut worker = Self {
            tls_observer: None,
            id,
            cache_ttl_rewrite_supported: false,
            child: Some(child),
            stdin,
            stdout,
            root,
            _image: image,
            execution_image: execution_image.clone(),
            image_sha256: String::new(),
            ready_at: started,
            started_at: started,
            startup_ms: 0,
            idle_since: Instant::now(),
            _lease: Some(lease),
            _proxy: proxy,
        };
        let ready = timeout_at(deadline, worker.next())
            .await
            .map_err(|_| unavailable())??;
        let pid = worker
            .child
            .as_ref()
            .and_then(Child::id)
            .ok_or_else(unavailable)?;
        if ready["type"] != "worker_ready"
            || ready["version"] != "2.1.293"
            || ready["raw_input_protocol"] != 1
            || ready["pid"].as_u64() != Some(pid as u64)
            || (ready.get("cache_ttl_rewrite_protocol").is_some()
                && ready["cache_ttl_rewrite_protocol"] != 1)
        {
            return Err(unavailable());
        }
        worker.cache_ttl_rewrite_supported = ready["cache_ttl_rewrite_protocol"] == 1;
        let mut executable = File::open(format!("/proc/{pid}/exe")).map_err(|_| unavailable())?;
        if executable.metadata().map_err(|_| unavailable())?.len() != artifact.image_bytes {
            return Err(unavailable());
        }
        let actual_digest = digest_file(&mut executable).map_err(|_| unavailable())?;
        if actual_digest != artifact.image_sha256 {
            return Err(unavailable());
        }
        worker.image_sha256 = actual_digest;
        // FD在fcntl期间保持打开；四项封存保证缓存映像不能被修改或解除保护。
        let seals = unsafe { fcntl(executable.as_raw_fd(), F_GET_SEALS) };
        let required_seals = F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL;
        if seals < 0 || seals & required_seals != required_seals {
            return Err(unavailable());
        }
        if let Some(shared) = execution_image {
            let expected = shared.metadata().map_err(|_| unavailable())?;
            let actual = executable.metadata().map_err(|_| unavailable())?;
            if expected.dev() != actual.dev() || expected.ino() != actual.ino() {
                return Err(unavailable());
            }
            worker.execution_image = Some(shared);
        } else {
            worker.execution_image = Some(Arc::new(executable));
        }
        worker.ready_at = Instant::now();
        if config.native_capture_dir.is_some() {
            let slot = super::native_tls_observer::NativeTlsSlot::new(
                pid,
                &worker.id,
                &worker.image_sha256,
            );
            slot.warm();
            worker.tls_observer = Some(slot);
        }
        worker.startup_ms = started.elapsed().as_millis() as u64;
        Ok(worker)
    }

    /// 空闲管道出现 EOF、部分帧或迟到事件时隔离进程，避免跨请求污染。
    fn reusable(&mut self) -> bool {
        self.child
            .as_mut()
            .is_some_and(|child| matches!(child.try_wait(), Ok(None)))
            && self.stdout.fill_buf().now_or_never().is_none()
    }

    async fn execute(
        &mut self,
        submission: &NativeSubmission,
        capture_dir: Option<&str>,
        response: &mut Option<oneshot::Sender<Result<Response, AppError>>>,
        chunks: &mpsc::Sender<Result<Bytes, io::Error>>,
        deadline: Instant,
        received: Instant,
        mut runtime: NativeRuntimeObservation,
    ) -> Result<bool, AppError> {
        // 启动已消耗同一份预算；到期后必须在写入业务指令前结束。
        if Instant::now() >= deadline {
            return Err(unavailable());
        }
        let id = &submission.request.request_id;
        let mut stream_log = NativeExecutionLog {
            job_id: id.clone(),
            worker_id: self.id.clone(),
            worker_pid: self.child.as_ref().and_then(Child::id),
            account_id: submission.request.account_id,
            generation: submission.request.generation,
            started: received,
            last_event: None,
            last_chunk: None,
            chunks: 0,
            bytes: 0,
            headers_seen: false,
            reason: "execution_future_dropped",
        };
        let mut tls_capture = None;
        let result = async {
            let cache_ttl = CacheControlTtlRewrite::parse(submission.cache_ttl_rewrite.as_deref().unwrap_or("off"))?;
            if cache_ttl != CacheControlTtlRewrite::Off && !self.cache_ttl_rewrite_supported {
                stream_log.reason = "cache_ttl_capability_missing";
                return Err(AppError::ServiceUnavailable("当前内核缺少缓存TTL策略能力，请先激活配套候选内核".into()));
            }
            if submission.log_mode == RequestLogMode::Debug {
                if let Some(directory) = capture_dir {
                    tls_capture = Some(super::native_tls_observer::NativeTlsCapture::take(directory, submission, &self.id, self.tls_observer.as_ref()));
                }
            } else if let Some(slot) = &self.tls_observer {
                slot.clear();
            }
            if Instant::now() >= deadline { return Err(unavailable()); }
            self.send("request_input", Some(id), Some(&submission.input),
                self.cache_ttl_rewrite_supported.then_some(cache_ttl.as_str()))
                .await?;
            let mut policy = false;
            let mut header_policy = None;
            let mut prepared = false;
            let mut headers_seen = false;
            let mut terminal = false;
            let mut reusable = false;
            let mut observation: Option<NativeRequestObservation> = None;
            loop {
                let frame = if headers_seen {
                    timeout(
                        Duration::from_secs(if terminal { 3 } else { 120 }),
                        self.next(),
                    )
                    .await
                    .map_err(|_| {
                        stream_log.reason = if terminal { "worker_idle_timeout" } else { "native_event_timeout" };
                        protocol_error()
                    })??
                } else {
                    timeout_at(deadline, self.next())
                        .await
                        .map_err(|_| { stream_log.reason = "native_response_deadline"; protocol_error() })??
                };
                let previous_event_gap_ms = stream_log.last_event.map(|at| at.elapsed().as_millis());
                stream_log.last_event = Some(Instant::now());
                if frame["request_id"] != *id {
                    return Err(protocol_error());
                }
                if frame["type"] == "worker_idle" {
                    if !terminal {
                        return Err(protocol_error());
                    }
                    if let Some(slot) = &self.tls_observer { slot.warm(); }
                    return if !reusable && headers_seen {
                        Err(protocol_error())
                    } else {
                        Ok(reusable)
                    };
                }
                if frame["type"] != "request_event" || terminal {
                    return Err(protocol_error());
                }
                let payload = &frame["payload"];
                if payload["type"] != "error" && payload["attempt"] != 1 {
                    return Err(protocol_error());
                }
                match payload["type"].as_str() {
                    Some("header_policy") if !policy && !prepared && !headers_seen => {
                        let decisions = payload["decisions"].as_array().ok_or_else(protocol_error)?;
                        for decision in decisions {
                            let obj = decision.as_object().ok_or_else(protocol_error)?;
                            if obj.len() != 3
                                || !decision["name"].is_string()
                                || !decision["reason"].is_string()
                                || !matches!(
                                    decision["action"].as_str(),
                                    Some("keep" | "drop" | "runtime" | "context")
                                )
                            {
                                return Err(protocol_error());
                            }
                        }
                        // 只记录动作计数；策略来自二进制，头值及任意调用方文本不进入普通日志。
                        tracing::debug!(request_id = %id, source = "native_header_policy", decisions = decisions.len(), "原生头部策略已完成");
                        if submission.log_mode == RequestLogMode::Debug && capture_dir.is_some() {
                            header_policy = Some(super::native_capture::header_policy(decisions));
                        }
                        policy = true;
                    }
                    Some("cc_prepared") if policy && !prepared && !headers_seen => {
                        runtime.prepared_ms = received.elapsed().as_millis() as u64;
                        let body = payload["body"].as_str().ok_or_else(protocol_error)?;
                        if payload["body_bytes"].as_u64() != Some(body.len() as u64)
                            || payload["body_sha256"]
                                != format!("{:x}", Sha256::digest(body.as_bytes()))
                            || !payload["headers"].is_array()
                        {
                            return Err(protocol_error());
                        }
                        prepared = true;
                        let value: Value = serde_json::from_str(body).map_err(|_| protocol_error())?;
                        let mut initial = None;
                        if let Some(capture) = tls_capture.as_mut() {
                            capture.prepared(payload, header_policy.as_ref());
                            let mut pending = payload.clone();
                            pending["_wire_final"] = capture.pending();
                            initial = Some(pending);
                        }
                        let capture_status = super::native_capture::record_queued(
                            capture_dir,
                            submission,
                            initial.as_ref().unwrap_or(payload),
                            header_policy.as_ref(),
                        );
                        observation = Some(NativeRequestObservation {
                            runtime: Some(runtime.clone()),
                            capture_status: Some(capture_status),
                            schema_version: 1,
                            parameters: DiagnosticParameters::from_request(&value),
                            session_id: extract_session_id_from_body(&value)
                                .filter(|id| id.len() <= 512),
                            previous_message_id: value
                                .pointer("/thread/previous_message_id")
                                .and_then(Value::as_str)
                                .filter(|id| id.len() <= 512)
                                .map(str::to_owned),
                            body_sha256: payload["body_sha256"]
                                .as_str()
                                .ok_or_else(protocol_error)?
                                .to_owned(),
                        });
                    }
                    Some("response_headers") if prepared && !headers_seen => {
                        if let Some(capture) = tls_capture.take() { capture.background(); }
                        let status = payload["status"]
                            .as_u64()
                            .filter(|status| (200..=599).contains(status))
                            .ok_or_else(protocol_error)?;
                        let pairs: Vec<(String, String)> =
                            serde_json::from_value(payload["headers"].clone())
                                .map_err(|_| protocol_error())?;
                        let mut result = Response::new(Body::empty());
                        *result.status_mut() =
                            StatusCode::from_u16(status as u16).map_err(|_| protocol_error())?;
                        for (name, value) in pairs {
                            // Bun 返回的是逻辑字节，长度/压缩及逐跳传输头由当前 HTTP 边界重建。
                            if matches!(
                                name.to_ascii_lowercase().as_str(),
                                "content-length"
                                    | "content-encoding"
                                    | "transfer-encoding"
                                    | "connection"
                                    | "keep-alive"
                                    | "proxy-authenticate"
                                    | "proxy-authorization"
                                    | "trailer"
                                    | "upgrade"
                                    | "x-v3-transport-error"
                                    | NATIVE_OBSERVATION_HEADER
                            ) {
                                continue;
                            }
                            result.headers_mut().append(
                                HeaderName::from_bytes(name.as_bytes())
                                    .map_err(|_| protocol_error())?,
                                HeaderValue::from_str(&value).map_err(|_| protocol_error())?,
                            );
                        }
                        if let Some(observation) = &observation {
                            let value = STANDARD
                                .encode(serde_json::to_vec(observation).map_err(|_| protocol_error())?);
                            result.headers_mut().insert(
                                NATIVE_OBSERVATION_HEADER,
                                HeaderValue::from_str(&value).map_err(|_| protocol_error())?,
                            );
                        }
                        if let Some(sender) = response.take() {
                            sender.send(Ok(result)).map_err(|_| protocol_error())?;
                        }
                        headers_seen = true;
                        stream_log.headers_seen = true;
                    }
                    Some("response_chunk") if headers_seen => {
                        let data = payload["data_b64"].as_str().ok_or_else(protocol_error)?;
                        let bytes = STANDARD.decode(data).map_err(|_| protocol_error())?;
                        if STANDARD.encode(&bytes) != data {
                            return Err(protocol_error());
                        }
                        stream_log.chunks += 1;
                        stream_log.last_chunk = Some(Instant::now());
                        stream_log.bytes = stream_log.bytes.saturating_add(bytes.len() as u64);
                        chunks
                            .send(Ok(Bytes::from(bytes)))
                            .await
                            .map_err(|_| protocol_error())?;
                    }
                    Some("completed") if headers_seen => {
                        reusable = matches!(payload["state"].as_str(), Some("success" | "http_error"));
                        terminal = true;
                    }
                    Some("error") => {
                        if let Some(capture) = tls_capture.take() { capture.background(); }
                        stream_log.reason = "native_error_event";
                        tracing::warn!(native_job_id = %id, worker_id = %self.id,
                            layer = "native_sender", reason = "native_error_event",
                            headers_seen, previous_event_gap_ms, error_code = native_error_code(payload),
                            error_sha256 = %format!("{:x}", Sha256::digest(payload["error"].as_str().unwrap_or("").as_bytes())),
                            "固定运行时返回错误事件");
                        let status = payload["status"]
                            .as_u64()
                            .filter(|s| (400..=599).contains(s))
                            .unwrap_or(502);
                        if let Some(sender) = response.take() {
                            let error_body = json!({"error":{"type":"native_request_error","message":"原生请求校验或执行失败","code":payload["code"].as_str().filter(|code| code.len() <= 64 && code.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')).unwrap_or("native_error")}}).to_string();
                            let mut result = Response::new(Body::empty());
                            *result.status_mut() =
                                StatusCode::from_u16(status as u16).map_err(|_| protocol_error())?;
                            result
                                .headers_mut()
                                .insert("content-type", HeaderValue::from_static("application/json"));
                            if let Some(observation) = &observation {
                                let value = STANDARD.encode(
                                    serde_json::to_vec(observation).map_err(|_| protocol_error())?,
                                );
                                result.headers_mut().insert(
                                    NATIVE_OBSERVATION_HEADER,
                                    HeaderValue::from_str(&value).map_err(|_| protocol_error())?,
                                );
                            }
                            let _ = sender.send(Ok(result));
                            chunks
                                .send(Ok(Bytes::from(error_body)))
                                .await
                                .map_err(|_| protocol_error())?;
                        }
                        terminal = true;
                        reusable = !prepared;
                        if headers_seen {
                            return Err(protocol_error());
                        }
                    }
                    _ => return Err(protocol_error()),
                }
            }
        }.await;
        if stream_log.reason == "execution_future_dropped" {
            stream_log.reason = if result.is_ok() {
                "native_completed"
            } else {
                "native_protocol_error"
            };
        }
        result
    }
}

impl NativePool {
    /// 汇总当前闲置worker的真实TLS槽。@param config 当前绑定配置。@return 安全能力摘要，不创建进程。
    pub(crate) fn tls_status(&self, config: &SenderConfig) -> Value {
        let mut result = json!({"state":"preparing","reason":"CAPTURE_PREPARING",
            "checked_at_ms":now_ms(),"protocol_version":1,"image_admitted":null});
        if config.native_capture_dir.is_none() {
            result["state"] = json!("disabled");
            result["reason"] = Value::Null;
            return result;
        }
        if std::env::var_os("CCRB_TLS_OBSERVER").is_none() {
            result["state"] = json!("unavailable");
            result["reason"] = json!("OBSERVER_NOT_CONFIGURED");
            return result;
        }
        let mut state = self.state.lock().unwrap();
        for worker in &mut state.idle {
            if !worker.reusable() {
                continue;
            }
            if let Some(slot) = &worker.tls_observer {
                let (status, reason) = slot.status();
                result["state"] = json!(status);
                result["reason"] = json!(reason);
                result["image_admitted"] = json!(
                    worker.image_sha256
                        == "af4cea9e21aa959c5e39c389173ba30ebb19b354b67acfe99164946450e9776d"
                );
                if status == "ready" {
                    break;
                }
            }
        }
        result
    }

    /// 检查当前身份的实时可复用进程，检查过程不投递模型或启动进程。
    /// @param config 当前 sender 配置。
    /// @param identity 已验证绑定的账号身份。
    /// @return 本次实际状态，失败或旧身份不沿用历史 ready。
    pub(crate) async fn readiness(
        self: &Arc<Self>,
        config: &SenderConfig,
        identity: &NativeSubmission,
    ) -> Result<NativeReadinessStatus, AppError> {
        let (key, _) = pool_identity(config, identity)?;
        self.verify(config.native.as_ref().ok_or_else(unavailable)?)?;
        let warmup = self.warmup.try_read().ok();
        let generation = self.generation.try_lock().ok();
        let mut retired = Vec::new();
        let mut result = NativeReadinessStatus {
            state: "unprepared".into(),
            account_id: identity.request.account_id,
            environment_id: identity.request.environment_id.clone(),
            generation: identity.request.generation,
            kernel_generation: config
                .native
                .as_ref()
                .and_then(|native| native.kernel_generation.clone()),
            release: config
                .native
                .as_ref()
                .and_then(|native| native.release.clone()),
            checked_at_ms: now_ms(),
            idle_workers: 0,
            live_workers: 0,
            pid: None,
            reason: None,
            tls_observer: None,
        };
        {
            let mut state = self.state.lock().unwrap();
            result.live_workers = state.live;
            if warmup.is_none() || generation.is_none() {
                result.state = "preparing".into();
            } else if state.key != key {
                result.reason = Some("identity_changed".into());
            } else {
                let mut index = 0;
                while index < state.idle.len() {
                    if state.idle[index].reusable() {
                        index += 1;
                    } else {
                        retired.push(state.idle.remove(index));
                    }
                }
                result.idle_workers = state.idle.len();
                result.pid = state
                    .idle
                    .last()
                    .and_then(|worker| worker.child.as_ref().and_then(Child::id));
                result.state = if result.pid.is_some() {
                    "ready"
                } else if state.starting > 0 {
                    "preparing"
                } else if state.live_by_key.get(&key).copied().unwrap_or(0) > retired.len() {
                    "busy"
                } else {
                    "unprepared"
                }
                .into();
                if !retired.is_empty() {
                    result.reason = Some("worker_unavailable".into());
                }
            }
        }
        // 物理退出与计数归还在池锁外收口，检查不会误把退役进程当作空闲。
        for worker in retired {
            worker.retire().await;
        }
        result.live_workers = self.state.lock().unwrap().live;
        result.tls_observer = Some(self.tls_status(config));
        Ok(result)
    }

    /// 暖能力检查复用已核验文件，每次重新核对路径和完整文件身份。
    /// @param config 当前部署配置，预算每次独立验证。
    /// @return 文件与登记身份相同，否则明确拒绝。
    pub(crate) fn verify(&self, config: &NativeSenderConfig) -> Result<(), AppError> {
        validate_distribution_config(config)?;
        let artifact = artifact(config)?;
        let path = fs::canonicalize(&artifact.path).map_err(|_| unavailable())?;
        let identity = DistributionIdentity::from(fs::metadata(&path).map_err(|_| unavailable())?);
        let mut cached = self.verified.lock().unwrap();
        if let Some(verified) = cached.as_ref() {
            if verified.sha256 == artifact.launcher_sha256
                && verified.path == path
                && verified.identity == identity
                && DistributionIdentity::from(verified.file.metadata().map_err(|_| unavailable())?)
                    == identity
            {
                return Ok(());
            }
        }
        // 先失效再摘要，核验失败后不能继续借用旧缓存。
        *cached = None;
        let file = verify_artifact(&artifact)?;
        let verified = DistributionIdentity::from(file.metadata().map_err(|_| unavailable())?);
        if verified != identity
            || DistributionIdentity::from(fs::metadata(&path).map_err(|_| unavailable())?)
                != verified
            || fs::canonicalize(&artifact.path).map_err(|_| unavailable())? != path
        {
            return Err(unavailable());
        }
        *cached = Some(VerifiedDistribution {
            path,
            identity: verified,
            sha256: artifact.launcher_sha256,
            file,
        });
        Ok(())
    }

    async fn retire_for_switch(&self, deadline: Instant) -> Result<(), AppError> {
        let deadline = deadline.min(Instant::now() + Duration::from_secs(8));
        self.state.lock().unwrap().spare.take();
        let old_idle = loop {
            {
                let mut state = self.state.lock().unwrap();
                if state.live == state.idle.len() {
                    break std::mem::take(&mut state.idle);
                }
            }
            if Instant::now() >= deadline {
                return Err(unavailable());
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(20))).await;
        };
        // Drop 把物理许可交给 wait 任务，切换预热超时也不会提前归还。
        drop(old_idle);
        while self.state.lock().unwrap().live != 0 {
            if Instant::now() >= deadline {
                return Err(unavailable());
            }
            sleep_until(deadline.min(Instant::now() + Duration::from_millis(20))).await;
        }
        *self.image.lock().await = None;
        Ok(())
    }

    /// 准备一个可复用进程，全程不写入模型作业指令。
    /// @param config 接收边界冻结的 sender 配置。
    /// @param identity 仅用于共享启动身份和绑定校验的内部结构，input 不执行。
    /// @param deadline 接收时冻结的单调截止。
    /// @return ready 的实际 PID 与空闲数量，或业务占用时的 busy。
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        config: SenderConfig,
        identity: NativeSubmission,
        deadline: Instant,
    ) -> Result<NativePreparationStatus, AppError> {
        self.prepare_internal(config, identity, deadline, false)
            .await
    }

    /// 等旧代物理进程收口后，启动切换使用的零模型暖进程。
    /// @param config 已验证切换选择的 sender 配置。
    /// @param identity 当前账号和绑定。
    /// @param deadline 本轮预热截止。
    /// @return 真实新代回执，旧进程未收口时返回准备错误。
    pub(crate) async fn prepare_switch(
        self: &Arc<Self>,
        config: SenderConfig,
        identity: NativeSubmission,
        deadline: Instant,
    ) -> Result<NativePreparationStatus, AppError> {
        self.prepare_internal(config, identity, deadline, true)
            .await
    }

    async fn prepare_internal(
        self: &Arc<Self>,
        config: SenderConfig,
        identity: NativeSubmission,
        deadline: Instant,
        switching: bool,
    ) -> Result<NativePreparationStatus, AppError> {
        validate_worker_identity(&config, &identity)?;
        if Instant::now() >= deadline {
            return Err(unavailable());
        }
        let id = identity.request.request_id.clone();
        let key = PreparationKey {
            account_id: identity.request.account_id,
            environment_id: identity.request.environment_id.clone(),
            generation: identity.request.generation,
            id: id.clone(),
        };
        let (cancel, mut cancellation) = oneshot::channel();
        {
            let mut pending = self.preparations.lock().unwrap();
            pending.retain(|_, state| !matches!(state, PreparationState::Cancelled(until) if *until <= Instant::now()));
            if pending.contains_key(&key) {
                return Err(AppError::ServiceUnavailable(
                    "原生准备已撤销或正在执行".into(),
                ));
            }
            if pending.len() >= 256
                || pending
                    .values()
                    .filter(|state| matches!(state, PreparationState::Active(_)))
                    .count()
                    >= 16
            {
                return Err(unavailable());
            }
            pending.insert(key.clone(), PreparationState::Active(cancel));
        }
        let _lease = PreparationLease {
            pool: Arc::downgrade(self),
            key,
        };
        let operation = async {
            let _warmup = self.warmup.write().await;
            if switching {
                self.retire_for_switch(deadline).await?;
            }
            let prior: Vec<String> = {
                let state = self.state.lock().unwrap();
                if state.key == pool_identity(&config, &identity)?.0
                    && state.live_by_key.get(&state.key).copied().unwrap_or(0) > 0
                    && state.idle.is_empty()
                {
                    return Ok(NativePreparationStatus {
                        state: "busy".into(),
                        account_id: identity.request.account_id,
                        environment_id: identity.request.environment_id.clone(),
                        generation: identity.request.generation,
                        preparation_id: id.clone(),
                        version: "2.1.293".into(),
                        raw_input_protocol: 1,
                        pid: None,
                        idle_workers: 0,
                        reused: false,
                        image_sha256: None,
                        release: config
                            .native
                            .as_ref()
                            .and_then(|native| native.release.clone()),
                        kernel_generation: config
                            .native
                            .as_ref()
                            .and_then(|native| native.kernel_generation.clone()),
                    });
                }
                state.idle.iter().map(|worker| worker.id.clone()).collect()
            };
            let (key, mut worker) = self.checkout(&config, &identity, deadline, false).await?;
            let pid = worker
                .child
                .as_ref()
                .and_then(Child::id)
                .ok_or_else(unavailable)?;
            let reused = prior.contains(&worker.id);
            // 发布 idle 与取消登记共用线性化点，撤销后的完成结果不能重新留池。
            let registration = self.preparations.lock().unwrap();
            if !matches!(
                registration.get(&_lease.key),
                Some(PreparationState::Active(_))
            ) {
                return Err(AppError::ServiceUnavailable("原生准备已撤销".into()));
            }
            let image_sha256 = worker.image_sha256.clone();
            let mut state = self.state.lock().unwrap();
            if state.key != key {
                return Err(unavailable());
            }
            worker.idle_since = Instant::now();
            state.idle.push(worker);
            Ok(NativePreparationStatus {
                state: "ready".into(),
                account_id: identity.request.account_id,
                environment_id: identity.request.environment_id.clone(),
                generation: identity.request.generation,
                preparation_id: id.clone(),
                version: "2.1.293".into(),
                raw_input_protocol: 1,
                pid: Some(pid),
                idle_workers: state.idle.len(),
                reused,
                image_sha256: Some(image_sha256),
                release: config
                    .native
                    .as_ref()
                    .and_then(|native| native.release.clone()),
                kernel_generation: config
                    .native
                    .as_ref()
                    .and_then(|native| native.kernel_generation.clone()),
            })
        };
        tokio::select! {
            biased;
            _ = &mut cancellation => Err(AppError::ServiceUnavailable("原生准备已取消".into())),
            result = timeout_at(deadline, operation) => result.map_err(|_| unavailable())?,
        }
    }

    /// 登记本环境准备的撤销，覆盖取消先于准备到达的竞态。
    /// @param control 已验证账号和代次的控制标识。
    /// @return 撤销已保存时为 true，容量满时失败关闭。
    pub(crate) fn cancel_preparation(&self, control: &NativePreparationControl) -> bool {
        let now = Instant::now();
        let key = PreparationKey {
            account_id: control.account_id,
            environment_id: control.environment_id.clone(),
            generation: control.generation,
            id: control.preparation_id.clone(),
        };
        let mut pending = self.preparations.lock().unwrap();
        pending.retain(
            |_, state| !matches!(state, PreparationState::Cancelled(until) if *until <= now),
        );
        if pending.len() >= 256 && !pending.contains_key(&key) {
            return false;
        }
        // 保留请求预算、时钟偏差及取消传输宽限，迟到准备仍会被同一撤销记录拒绝。
        let until = now + Duration::from_millis(REQUEST_BUDGET_MS + CLOCK_SKEW_MS + 5_000);
        if let Some(PreparationState::Active(cancel)) =
            pending.insert(key, PreparationState::Cancelled(until))
        {
            let _ = cancel.send(());
        }
        true
    }

    /// 用一个弱引用任务回收过期空闲进程，保留最近使用的一个暖进程。
    fn schedule_idle_reap(self: &Arc<Self>) {
        {
            let mut state = self.state.lock().unwrap();
            if state.reaper_running || state.idle.len() <= 1 {
                return;
            }
            state.reaper_running = true;
        }
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let deadline = {
                    let Some(pool) = weak.upgrade() else {
                        return;
                    };
                    let mut state = pool.state.lock().unwrap();
                    if state.idle.len() <= 1 {
                        state.reaper_running = false;
                        return;
                    }
                    state
                        .idle
                        .iter()
                        .map(|worker| worker.idle_since + pool.idle_timeout)
                        .min()
                        .unwrap()
                };
                // 等待期间只持有Weak，后台回收不能延长账号池的生命周期。
                sleep_until(deadline).await;
                let Some(pool) = weak.upgrade() else {
                    return;
                };
                let mut retired = Vec::new();
                {
                    let mut state = pool.state.lock().unwrap();
                    while state.idle.len() > 1 {
                        let Some(index) = state
                            .idle
                            .iter()
                            .position(|worker| worker.idle_since.elapsed() >= pool.idle_timeout)
                        else {
                            break;
                        };
                        retired.push(state.idle.remove(index));
                    }
                }
                for worker in retired {
                    worker.retire().await;
                }
            }
        });
    }

    async fn checkout(
        self: &Arc<Self>,
        config: &SenderConfig,
        submission: &NativeSubmission,
        deadline: Instant,
        business: bool,
    ) -> Result<(String, Worker), AppError> {
        let prepare_spare =
            business && config.native_adaptive_prewarm && submission.concurrency >= 2;
        let mut generation = self.generation.lock().await;
        let (key, slots) = pool_identity(config, submission)?;
        let retired;
        {
            let mut state = self.state.lock().unwrap();
            retired = if state.key != key {
                state.key = key.clone();
                state.spare.take();
                std::mem::take(&mut state.idle)
            } else {
                Vec::new()
            };
        }
        // WorkerLease 的 Drop 会回到同一个锁，必须在锁外回收。
        for worker in retired {
            worker.retire().await;
        }
        loop {
            let (candidate, preparing) = {
                let mut state = self.state.lock().unwrap();
                let candidate = state.idle.pop();
                let preparing = if candidate.is_none() {
                    state.spare.as_ref().map(|spare| spare.ready.clone())
                } else {
                    None
                };
                (candidate, preparing)
            };
            if let Some(mut preparing) = preparing {
                // 等待者不持有代次锁，补位失败后可沿原 checkout 路径启动。
                drop(generation);
                let _ = timeout_at(deadline, preparing.changed())
                    .await
                    .map_err(|_| unavailable())?;
                generation = self.generation.lock().await;
                if self.state.lock().unwrap().key != key {
                    return Err(unavailable());
                }
                continue;
            }
            let Some(mut worker) = candidate else {
                break;
            };
            // 空闲时的 EOF、部分帧或迟到事件都先隔离，不能写入下一作业后再发现旧归属。
            if worker.reusable() {
                if prepare_spare {
                    self.start_spare(
                        config,
                        submission,
                        &key,
                        worker.execution_image.clone(),
                        deadline,
                    );
                }
                return Ok((key, worker));
            }
            worker.retire().await;
        }
        let initial_start;
        {
            let mut state = self.state.lock().unwrap();
            if state.live >= slots {
                return Err(unavailable());
            }
            initial_start = state.live == 0;
            state.live += 1;
            state.starting += 1;
            *state.live_by_key.entry(key.clone()).or_default() += 1;
        }
        // 首次冷启动持有代次锁直到握手；并发首请求复用这一轮结果再按原容量扩容。
        let generation = if initial_start {
            Some(generation)
        } else {
            drop(generation);
            None
        };
        let _starting = StartupLease(Arc::downgrade(self));
        let identity = WorkerIdentity::from(submission);
        let lease = WorkerLease {
            pool: Arc::downgrade(self),
            key: key.clone(),
        };
        // 即使执行体相同，新分发文件也须实际经过加载器一次。
        let distribution_sha256 =
            artifact(config.native.as_ref().ok_or_else(unavailable)?)?.launcher_sha256;
        let mut cached = self.image.lock().await;
        if cached
            .as_ref()
            .is_some_and(|(digest, _)| digest != &distribution_sha256)
        {
            *cached = None;
        }
        let worker = if let Some((_, image)) = cached.as_ref() {
            let image = image.clone();
            drop(cached);
            Worker::start(config, &identity, lease, Some(image), deadline).await?
        } else {
            // 只让首个进程解压；其它启动者等待封存映像后并行exec。
            let worker = Worker::start(config, &identity, lease, None, deadline).await?;
            *cached = Some((
                distribution_sha256,
                worker
                    .execution_image
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .clone(),
            ));
            drop(cached);
            worker
        };
        drop(generation);
        if prepare_spare {
            self.start_spare(
                config,
                submission,
                &key,
                worker.execution_image.clone(),
                deadline,
            );
        }
        Ok((key, worker))
    }

    /// 使用已加载的映像补位，业务请求无需等待本次握手。
    fn start_spare(
        self: &Arc<Self>,
        config: &SenderConfig,
        submission: &NativeSubmission,
        key: &str,
        image: Option<Arc<File>>,
        deadline: Instant,
    ) {
        let Some(image) = image else { return };
        let (done, ready) = watch::channel(());
        let (cancel, cancelled) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap();
            if state.key != key || state.live != 1 || state.spare.is_some() {
                return;
            }
            // 先登记并占用物理许可；并发 checkout 只能复用这一轮补位。
            state.live += 1;
            state.starting += 1;
            *state.live_by_key.entry(key.to_owned()).or_default() += 1;
            state.spare = Some(SparePreparation {
                ready: ready.clone(),
                _cancel: cancel,
            });
        }
        let pool = self.clone();
        let lease = WorkerLease {
            pool: Arc::downgrade(self),
            key: key.to_owned(),
        };
        let starting = StartupLease(Arc::downgrade(self));
        let config = config.clone();
        let identity = WorkerIdentity::from(submission);
        let key = key.to_owned();
        tokio::spawn(async move {
            let started = Instant::now();
            let worker = tokio::select! {
                biased;
                _ = cancelled => None,
                result = timeout_at(deadline, Worker::start(&config, &identity, lease, Some(image), deadline)) => {
                    result.ok().and_then(Result::ok)
                }
            };
            drop(starting);
            let mut worker = worker;
            let mut outcome = "cancelled";
            {
                let mut state = pool.state.lock().unwrap();
                if state.key == key
                    && state
                        .spare
                        .as_ref()
                        .is_some_and(|spare| spare.ready.same_channel(&ready))
                {
                    state.spare.take();
                    outcome = "failed";
                    if let Some(mut worker) = worker.take() {
                        worker.idle_since = Instant::now();
                        state.idle.push(worker);
                        outcome = "ready";
                    }
                }
            }
            drop(worker);
            drop(done);
            pool.schedule_idle_reap();
            tracing::info!(
                account_id = config.binding.account_id,
                generation = config.binding.generation,
                elapsed_ms = started.elapsed().as_millis() as u64,
                outcome,
                "原生第二 worker 预热"
            );
        });
    }

    /// 在有界通道中执行一次原生请求，响应取消会终止本次物理进程。
    /// @param config 当前账号 sender 的冻结部署配置。
    /// @param submission 网关冻结的原始信封和当前 bearer。
    /// @return 流式 HTTP 响应；终态与 idle 收齐后才发布 EOF 和复用进程。
    pub async fn submit(
        self: &Arc<Self>,
        config: SenderConfig,
        submission: NativeSubmission,
    ) -> Result<Response, AppError> {
        let deadline = validate_request(&submission.request, &config)?;
        self.submit_validated(config, submission, deadline).await
    }

    /// 使用接收边界已校验的信封及单调截止执行一次请求。
    /// @param config 已冻结的账号 sender 配置。
    /// @param submission 已核对归属、目标及期限的原始信封。
    /// @param deadline 接收时冻结的截止，启动和首响应共享同一预算。
    /// @return 单次执行响应或受控错误。
    pub(crate) async fn submit_validated(
        self: &Arc<Self>,
        config: SenderConfig,
        submission: NativeSubmission,
        deadline: Instant,
    ) -> Result<Response, AppError> {
        let received = Instant::now();
        if Instant::now() >= deadline {
            return Err(unavailable());
        }
        let (reply, receive) = oneshot::channel();
        let (chunks, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
        let pool = self.clone();
        tokio::spawn(async move {
            let mut reply = Some(reply);
            let operation = async {
                // 业务等待正在进行的零模型准备，随后从同一 idle 集合复用其结果。
                let warmup = timeout_at(deadline, pool.warmup.read())
                    .await
                    .map_err(|_| unavailable())?;
                let preparation_wait_ms = received.elapsed().as_millis() as u64;
                let checkout_started = Instant::now();
                let (key, mut worker) = timeout_at(
                    deadline,
                    pool.checkout(&config, &submission, deadline, true),
                )
                .await
                .map_err(|_| unavailable())??;
                drop(warmup);
                let runtime = NativeRuntimeObservation {
                    worker_id: worker.id.clone(),
                    worker_pid: worker
                        .child
                        .as_ref()
                        .and_then(Child::id)
                        .ok_or_else(unavailable)?,
                    kernel_generation: config
                        .native
                        .as_ref()
                        .and_then(|native| native.kernel_generation.clone()),
                    warm_hit: worker.ready_at <= received,
                    preparation_wait_ms,
                    checkout_ms: checkout_started.elapsed().as_millis() as u64,
                    worker_start_ms: (worker.started_at >= checkout_started)
                        .then_some(worker.startup_ms),
                    prepared_ms: 0,
                };
                let result = tokio::select! {
                    result = worker.execute(&submission, config.native_capture_dir.as_deref(), &mut reply, &chunks, deadline, received, runtime) => result,
                    _ = chunks.closed() => {
                        let _ = timeout(Duration::from_millis(200), worker.send("request_cancel", Some(&submission.request.request_id), None, None)).await;
                        Err(protocol_error())
                    }
                };
                if matches!(result, Ok(true)) {
                    let mut state = pool.state.lock().unwrap();
                    if state.key == key {
                        worker.idle_since = Instant::now();
                        state.idle.push(worker);
                        drop(state);
                        pool.schedule_idle_reap();
                    } else {
                        drop(state);
                        drop(worker);
                    }
                } else {
                    worker.retire().await;
                }
                result.map(|_| ())
            };
            let result = tokio::select! {
                biased;
                _ = chunks.closed() => {
                    tracing::info!(native_job_id = %submission.request.request_id,
                        account_id = submission.request.account_id, generation = submission.request.generation,
                        layer = "native_sender", reason = "downstream_body_closed",
                        elapsed_ms = received.elapsed().as_millis(), "下游释放原生响应流");
                    Err(protocol_error())
                },
                result = operation => result,
            };
            if let Err(error) = result {
                if let Some(reply) = reply.take() {
                    let _ = reply.send(Err(error));
                } else {
                    let _ = chunks
                        .send(Err(io::Error::other("原生响应未完整收口")))
                        .await;
                }
            }
        });
        let mut response = receive.await.map_err(|_| protocol_error())??;
        // 所有响应通过同一接收端持有取消信号，EOF 晚于 terminal 与 idle。
        if response.body().size_hint().exact() == Some(0) {
            *response.body_mut() = Body::from_stream(ReceiverStream::new(receiver));
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::runtime_transport::{PreparedRequest, TransportBinding, now_ms};
    use axum::body::to_bytes;
    use futures_util::future::join_all;
    use reqwest::Client;
    use std::env::{temp_dir, var};
    use std::fs::FileTimes;
    use std::io::Write;

    #[tokio::test]
    async fn native_execution_logs_error_event_eof_and_deadline() {
        let (subscriber, output) = crate::service::stream_error::test_support::capture();
        let _guard = tracing::subscriber::set_default(subscriber);
        for scenario in ["error", "eof", "deadline", "ttl", "unsupported-ttl"] {
            let worker_id = Uuid::new_v4().to_string();
            let job_id = Uuid::new_v4().to_string();
            let frame = json!({"schema_version":2,"worker_id":worker_id,"request_id":job_id,
                "type":"request_event","payload":{"type":"error","error":"synthetic-private-error-secret",
                "code":"request_timeout","status":502}});
            let idle = json!({"schema_version":2,"worker_id":worker_id,"request_id":job_id,"type":"worker_idle"});
            // 直接驱动私有事件读取路径，合成进程只写受控帧，不启动任何模型或原生构造。
            let mut child = Command::new("python3").args(["-u", "-c",
                "import sys,time,json; command=json.loads(sys.stdin.readline()); mode=sys.argv[1]; assert mode!='ttl' or command['cache_ttl_rewrite']=='1h'; time.sleep(2) if mode=='deadline' else None; print(sys.argv[2],flush=True) if mode in ('error','ttl') else None; print(sys.argv[3],flush=True) if mode in ('error','ttl') else None"])
                .arg(scenario).arg(frame.to_string()).arg(idle.to_string())
                .stdin(Stdio::piped()).stdout(Stdio::piped()).kill_on_drop(true).spawn().unwrap();
            let root = temp_dir().join(format!("native-log-test-{}", Uuid::new_v4()));
            fs::create_dir(&root).unwrap();
            let pid = child.id().unwrap();
            let mut worker = Worker {
                tls_observer: None,
                cache_ttl_rewrite_supported: scenario == "ttl",
                id: worker_id.clone(),
                stdin: child.stdin.take().unwrap(),
                stdout: BufReader::new(child.stdout.take().unwrap()),
                child: Some(child),
                root,
                _image: File::open("/dev/null").unwrap(),
                execution_image: None,
                idle_since: Instant::now(),
                _lease: None,
                _proxy: NativeProxy {
                    url: String::new(),
                    task: None,
                },
                image_sha256: String::new(),
                ready_at: Instant::now(),
                started_at: Instant::now(),
                startup_ms: 0,
            };
            let submission = NativeSubmission {
                cache_ttl_rewrite: scenario.contains("ttl").then(|| "1h".into()),
                ingress_capture: None,
                log_mode: RequestLogMode::Info,
                request: PreparedRequest {
                    account_id: 7,
                    environment_id: "synthetic-env".into(),
                    generation: 2,
                    request_id: job_id.clone(),
                    deadline_unix_ms: 0,
                    method: "POST".into(),
                    target_url: String::new(),
                    headers: Vec::new(),
                    preserve_header_casing: false,
                    tls_profile: Default::default(),
                },
                input: json!({"request_id":job_id}),
                bearer: "synthetic-bearer".into(),
                device_id: String::new(),
                account_uuid: None,
                organization_uuid: None,
                email: String::new(),
                concurrency: 1,
            };
            let (reply, receive) = oneshot::channel();
            let mut reply = Some(reply);
            let (chunks, _receiver) = mpsc::channel(8);
            let result = worker
                .execute(
                    &submission,
                    None,
                    &mut reply,
                    &chunks,
                    Instant::now()
                        + Duration::from_millis(if scenario == "deadline" { 30 } else { 1000 }),
                    Instant::now(),
                    NativeRuntimeObservation {
                        worker_id,
                        worker_pid: pid,
                        kernel_generation: None,
                        warm_hit: true,
                        preparation_wait_ms: 0,
                        checkout_ms: 0,
                        worker_start_ms: None,
                        prepared_ms: 0,
                    },
                )
                .await;
            if matches!(scenario, "error" | "ttl") {
                assert!(result.unwrap());
                assert_eq!(receive.await.unwrap().unwrap().status(), 502);
            } else {
                assert!(result.is_err());
            }
            worker.retire().await;
        }
        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        for reason in [
            "native_error_event",
            "ipc_eof",
            "native_response_deadline",
            "cache_ttl_capability_missing",
        ] {
            assert!(logs.contains(reason), "缺少结束原因 {reason}");
        }
        assert!(logs.contains("request_timeout"));
        assert!(logs.contains("native_job_id="));
        assert!(!logs.contains("synthetic-private-error-secret"));
        assert!(!logs.contains("synthetic-bearer"));
        assert_eq!(
            native_error_code(&json!({"code":"synthetic-private-error-secret"})),
            "unknown"
        );
    }

    /// 用同一正文对比冷请求、稳定热请求与零模型准备后的首轮。
    async fn benchmark_readiness(
        config: &SenderConfig,
        make: impl Fn(&str, &str) -> NativeSubmission,
    ) {
        // 同一大正文与资源条件配对测量，区分冷启动、稳定热请求和预热首轮。
        let mut measurements = Vec::new();
        for round in 0..3 {
            let mut benchmark = Arc::new(NativePool::default());
            let mut prepared_pid = None;
            let mut cold_ms = 0;
            let mut hot_ms = 0;
            for mode in ["cold", "hot", "prewarmed"] {
                let mut submission = make("readiness-latency", "synthetic-bearer-a");
                submission.log_mode = RequestLogMode::Info;
                submission.input["body"]["system"] = json!("x".repeat(780_000));
                submission.input["body"]["tools"] = json!((0..57).map(|index| json!({
                "name": format!("synthetic_tool_{index}"), "description": "合成性能工具",
                "input_schema": {"type":"object","properties":{}}
            })).collect::<Vec<_>>());
                let input_bytes = serde_json::to_vec(&submission.input).unwrap().len();
                if mode == "prewarmed" {
                    let idle = std::mem::take(&mut benchmark.state.lock().unwrap().idle);
                    for worker in idle {
                        worker.retire().await;
                    }
                    benchmark = Arc::new(NativePool::default());
                    prepared_pid = benchmark
                        .prepare(
                            config.clone(),
                            submission.clone(),
                            Instant::now() + Duration::from_secs(120),
                        )
                        .await
                        .unwrap()
                        .pid;
                }
                let mut business_config = config.clone();
                business_config.quota_identity = "synthetic-first-business-quota".into();
                let response = benchmark.submit(business_config, submission).await.unwrap();
                assert_eq!(response.status(), 200);
                let observation: NativeRequestObservation = serde_json::from_slice(
                    &STANDARD
                        .decode(response.headers()[NATIVE_OBSERVATION_HEADER].as_bytes())
                        .unwrap(),
                )
                .unwrap();
                let runtime = observation.runtime.unwrap();
                to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                assert_eq!(runtime.warm_hit, mode != "cold");
                if mode == "cold" {
                    cold_ms = runtime.prepared_ms;
                }
                if mode == "hot" {
                    hot_ms = runtime.prepared_ms;
                }
                if mode == "prewarmed" {
                    assert_eq!(Some(runtime.worker_pid), prepared_pid);
                    assert!(runtime.worker_start_ms.is_none());
                    assert!(runtime.prepared_ms < cold_ms, "预热首轮应消除冷启动开销");
                    assert!(
                        runtime.prepared_ms <= hot_ms.saturating_mul(3) + 200,
                        "预热首轮应处于同条件稳定热请求范围"
                    );
                }
                measurements.push(
                    json!({"round":round,"mode":mode,"input_bytes":input_bytes,"runtime":runtime}),
                );
            }
            let idle = std::mem::take(&mut benchmark.state.lock().unwrap().idle);
            for worker in idle {
                worker.retire().await;
            }
        }
        println!("{}", json!({"native_readiness_benchmark":measurements}));
    }

    #[tokio::test]
    async fn queued_preparation_cancels_without_process_or_model_job() {
        let pool = Arc::new(NativePool::default());
        let id = Uuid::new_v4().to_string();
        let config = SenderConfig {
            native: Some(NativeSenderConfig {
                kernel_generation: None,
                release: None,
                binary_path: "/missing".into(),
                work_root: "/tmp/synthetic-preparation".into(),
                memory_mib: 0,
                max_workers: 2,
            }),
            native_capture_dir: None,
            native_adaptive_prewarm: false,
            binding: TransportBinding {
                account_id: 7,
                environment_id: "synthetic".into(),
                generation: 1,
                endpoint: "http://localhost:8091".into(),
                secret: "synthetic-secret".into(),
                release_id: "synthetic".into(),
            },
            proxy_url: String::new(),
            allowed_origin: "https://api.anthropic.com".into(),
            evidence_file: None,
            quota_identity: String::new(),
        };
        let identity = NativeSubmission {
            cache_ttl_rewrite: None,
            ingress_capture: None,
            log_mode: RequestLogMode::Info,
            request: PreparedRequest {
                account_id: 7,
                environment_id: "synthetic".into(),
                generation: 1,
                request_id: id.clone(),
                deadline_unix_ms: now_ms() + 10_000,
                method: "POST".into(),
                target_url: "https://api.anthropic.com/v1/messages".into(),
                headers: Vec::new(),
                preserve_header_casing: false,
                tls_profile: Default::default(),
            },
            input: json!({"schema_version":1,"request_id":id}),
            bearer: "synthetic-bearer".into(),
            device_id: "a".repeat(64),
            account_uuid: None,
            organization_uuid: None,
            email: "fixture@example.test".into(),
            concurrency: 1,
        };
        // 旧身份仍在忙时，新身份必须进入真实准备；缺失制品应明确失败而非沿用 busy。
        let busy = Arc::new(NativePool::default());
        let old_key = pool_identity(&config, &identity).unwrap().0;
        let mut invalid_policy = identity.clone();
        invalid_policy.cache_ttl_rewrite = Some("invalid".into());
        assert!(matches!(
            pool_identity(&config, &invalid_policy),
            Err(AppError::BadRequest(_))
        ));
        let mut policy_only = identity.clone();
        policy_only.cache_ttl_rewrite = Some("1h".into());
        assert_eq!(
            old_key,
            pool_identity(&config, &policy_only).unwrap().0,
            "逐请求TTL设置必须复用同一暖池身份"
        );
        let mut observation_only = config.clone();
        observation_only.quota_identity = "synthetic-quota-observation".into();
        observation_only.native_adaptive_prewarm = true;
        assert_eq!(
            old_key,
            pool_identity(&observation_only, &identity).unwrap().0,
            "首次额度观察补写及备用开关必须保留已就绪进程"
        );
        let mut other_proxy = config.clone();
        other_proxy.proxy_url = "http://127.0.0.1:19091".into();
        assert_ne!(old_key, pool_identity(&other_proxy, &identity).unwrap().0);
        let mut other_binding = config.clone();
        other_binding.binding.generation += 1;
        assert_ne!(old_key, pool_identity(&other_binding, &identity).unwrap().0);
        let mut other_kernel = config.clone();
        other_kernel.native.as_mut().unwrap().kernel_generation = Some(Uuid::new_v4().to_string());
        assert_ne!(old_key, pool_identity(&other_kernel, &identity).unwrap().0);

        {
            let mut state = busy.state.lock().unwrap();
            state.key = old_key.clone();
            state.live = 1;
            state.live_by_key.insert(old_key, 1);
        }
        assert_eq!(
            busy.prepare(
                config.clone(),
                identity.clone(),
                Instant::now() + Duration::from_secs(2)
            )
            .await
            .unwrap()
            .state,
            "busy"
        );
        let mut changed = identity.clone();
        changed.bearer = "synthetic-next-bearer".into();
        assert!(
            busy.prepare(
                config.clone(),
                changed,
                Instant::now() + Duration::from_secs(2)
            )
            .await
            .is_err()
        );
        assert_eq!(busy.state.lock().unwrap().live, 1);
        let held = pool.warmup.write().await;
        let reusable_identity = identity.clone();
        let reusable_config = config.clone();
        let key = PreparationKey {
            account_id: 7,
            environment_id: "synthetic".into(),
            generation: 1,
            id: id.clone(),
        };
        let clone = pool.clone();
        let preparing = tokio::spawn(async move {
            clone
                .prepare(config, identity, Instant::now() + Duration::from_secs(2))
                .await
        });
        timeout(Duration::from_secs(1), async {
            while !pool.preparations.lock().unwrap().contains_key(&key) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut control = NativePreparationControl {
            account_id: 7,
            environment_id: "synthetic".into(),
            generation: 1,
            preparation_id: Uuid::new_v4().to_string(),
        };
        let early_id = control.preparation_id.clone();
        assert!(pool.cancel_preparation(&control));
        assert!(
            matches!(
                pool.preparations.lock().unwrap().get(&key),
                Some(PreparationState::Active(_))
            ),
            "其它准备的撤销保持归属隔离"
        );
        control.preparation_id = id.clone();
        assert!(pool.cancel_preparation(&control));
        assert!(preparing.await.unwrap().is_err());
        assert!(
            timeout(
                Duration::from_millis(100),
                pool.prepare(
                    reusable_config.clone(),
                    reusable_identity.clone(),
                    Instant::now() + Duration::from_secs(1)
                )
            )
            .await
            .unwrap()
            .is_err(),
            "已撤销准备的迟到重放在取得启动锁之前拒绝"
        );
        let mut early = reusable_identity.clone();
        early.request.request_id = early_id.clone();
        early.input["request_id"] = json!(early_id);
        assert!(
            timeout(
                Duration::from_millis(100),
                pool.prepare(
                    reusable_config.clone(),
                    early,
                    Instant::now() + Duration::from_secs(1)
                )
            )
            .await
            .unwrap()
            .is_err(),
            "取消先到时也阻止之后到达的准备"
        );
        let mut new_identity = reusable_identity;
        new_identity.request.generation = 2;
        let mut new_config = reusable_config;
        new_config.binding.generation = 2;
        let new_key = PreparationKey {
            generation: 2,
            ..key.clone()
        };
        let clone = pool.clone();
        let next = tokio::spawn(async move {
            clone
                .prepare(
                    new_config,
                    new_identity,
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        timeout(Duration::from_secs(1), async {
            while !pool.preparations.lock().unwrap().contains_key(&new_key) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(pool.cancel_preparation(&control));
        assert!(
            matches!(
                pool.preparations.lock().unwrap().get(&new_key),
                Some(PreparationState::Active(_))
            ),
            "旧代撤销不能影响新代的同名准备"
        );
        control.generation = 2;
        assert!(pool.cancel_preparation(&control));
        assert!(next.await.unwrap().is_err());
        drop(held);
        assert_eq!(pool.state.lock().unwrap().live, 0);
        assert!(
            !pool
                .preparations
                .lock()
                .unwrap()
                .values()
                .any(|state| matches!(state, PreparationState::Active(_)))
        );
    }

    #[test]
    fn native_budget_is_explicit_and_covers_weighted_capacity() {
        let config = NativeSenderConfig {
            kernel_generation: None,
            release: None,
            binary_path: "/missing".into(),
            work_root: "/tmp/native".into(),
            memory_mib: 1407,
            max_workers: 6,
        };
        assert!(config.verify().is_err());
        let mut config = config;
        config.max_workers = 0;
        config.memory_mib = u64::MAX;
        assert!(config.verify().is_err());
    }

    #[tokio::test]
    async fn socks_adapter_rejects_other_roles_and_targets_before_proxy_connection() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = NativeProxy::create(
            &format!("socks5h://{}", upstream.local_addr().unwrap()),
            &Uuid::new_v4().to_string(),
        )
        .await
        .unwrap();
        let address = Url::parse(&proxy.url).unwrap();
        for (target, credential) in [
            ("api.anthropic.com:443", "wrong"),
            ("other.example:443", "correct"),
        ] {
            let mut socket =
                TcpStream::connect((address.host_str().unwrap(), address.port().unwrap()))
                    .await
                    .unwrap();
            let credential = if credential == "correct" {
                format!(
                    "Basic {}",
                    STANDARD.encode(format!("ccrb:{}", address.password().unwrap()))
                )
            } else {
                "Basic invalid".into()
            };
            socket
                .write_all(
                    format!(
                        "CONNECT {target} HTTP/1.1\r\nProxy-Authorization: {credential}\r\n\r\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            let mut result = Vec::new();
            timeout(Duration::from_secs(1), socket.read_to_end(&mut result))
                .await
                .unwrap()
                .unwrap();
            assert!(result.is_empty());
        }
        assert!(
            timeout(Duration::from_millis(30), upstream.accept())
                .await
                .is_err()
        );
    }

    /// 只由隔离网络夹具显式启动，使用实际发布候选及合成账号。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "需要 CCRB_TEST_BINARY、CCRB_TEST_PROXY、CCRB_TEST_CA_FILE 和独立网络夹具"]
    async fn native_real_binary() {
        use std::collections::BTreeSet;

        let unlimited = var("CCRB_TEST_UNLIMITED").as_deref() == Ok("1");
        let normal = if unlimited { 2 } else { 3 };
        let root = temp_dir().join(format!("cc2api-native-{}", Uuid::new_v4()));
        let config = SenderConfig {
            native: Some(NativeSenderConfig {
                kernel_generation: None,
                release: var("CCRB_TEST_RELEASE_REF")
                    .ok()
                    .map(|value| serde_json::from_str(&value).unwrap()),
                binary_path: var("CCRB_TEST_BINARY").unwrap(),
                work_root: root.to_string_lossy().into_owned(),
                memory_mib: if unlimited { 0 } else { 1536 },
                max_workers: normal * 2,
            }),
            binding: TransportBinding {
                account_id: 7,
                environment_id: "synthetic-account-a".into(),
                generation: 1,
                endpoint: "http://localhost:8091".into(),
                secret: "synthetic-private-key".repeat(3),
                release_id: "test".into(),
            },
            proxy_url: var("CCRB_TEST_PROXY").unwrap(),
            allowed_origin: "https://api.anthropic.com".into(),
            evidence_file: None,
            native_capture_dir: Some(root.join("captures").to_string_lossy().into_owned()),
            native_adaptive_prewarm: false,
            quota_identity: String::new(),
        };
        let make = |label: &str, bearer: &str| {
            let id = Uuid::new_v4().to_string();
            NativeSubmission {
                cache_ttl_rewrite: None,
                ingress_capture: None,
                log_mode: RequestLogMode::Debug,
                request: PreparedRequest {
                    account_id: 7,
                    environment_id: config.binding.environment_id.clone(),
                    generation: 1,
                    request_id: id.clone(),
                    deadline_unix_ms: now_ms() + 120_000,
                    method: "POST".into(),
                    target_url: "https://api.anthropic.com/v1/messages".into(),
                    headers: Vec::new(),
                    preserve_header_casing: false,
                    tls_profile: Default::default(),
                },
                input: json!({"schema_version":1,"request_id":id,"body":{"model":"claude-sonnet-4-6","max_tokens":64,"stream":false,"messages":[{"role":"user","content":label}]},"headers":[["cf-ray","synthetic-cdn"],["new-api-user","synthetic-new-api"],["x-unregistered","synthetic-client"]]}),
                bearer: bearer.into(),
                device_id: "a".repeat(64),
                account_uuid: Some("11111111-1111-4111-8111-111111111111".into()),
                organization_uuid: Some("22222222-2222-4222-8222-222222222222".into()),
                email: "fixture@example.test".into(),
                concurrency: normal as i32,
            }
        };
        let pool = Arc::new(NativePool {
            idle_timeout: Duration::from_secs(2),
            ..Default::default()
        });
        if var("CCRB_TEST_TLS_ONLY").as_deref() == Ok("1") {
            let mut config = config.clone();
            config.native_capture_dir = Some(
                root.join("environment/flows/api/cc-prepared")
                    .to_string_lossy()
                    .into_owned(),
            );
            let mut workers = Vec::new();
            for _ in 0..6 {
                let identity = make("readiness-latency", "synthetic-bearer-a");
                let (_, worker) = pool
                    .checkout(
                        &config,
                        &identity,
                        Instant::now() + Duration::from_secs(30),
                        false,
                    )
                    .await
                    .unwrap();
                workers.push(worker);
            }
            pool.state.lock().unwrap().idle.extend(workers);
            timeout(Duration::from_secs(5), async {
                loop {
                    let ready = pool.state.lock().unwrap().idle.iter().all(|worker| {
                        worker
                            .tls_observer
                            .as_ref()
                            .is_some_and(|slot| slot.is_ready())
                    });
                    if ready {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let mut ids = Vec::new();
            let operations = (0..6)
                .map(|_| {
                    let mut submission = make("parallel-6", "synthetic-bearer-a");
                    submission.input["body"]["model"] = json!("claude-haiku-4-5");
                    ids.push(submission.request.request_id.clone());
                    let pool = pool.clone();
                    let config = config.clone();
                    async move {
                        let response = pool.submit(config, submission).await.unwrap();
                        assert_eq!(response.status(), 200);
                        to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                    }
                })
                .collect::<Vec<_>>();
            join_all(operations).await;
            let directory = Path::new(config.native_capture_dir.as_ref().unwrap());
            let mut snapshots = Vec::new();
            for id in &ids {
                let path = directory.join(format!("{id}.json"));
                let snapshot = timeout(Duration::from_secs(5), async {
                    loop {
                        if let Ok(bytes) = fs::read(&path) {
                            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                                if value["wire_final"]["reason"] != "CAPTURE_PENDING" {
                                    break value;
                                }
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(
                    snapshot["wire_final"]["complete"], true,
                    "{}",
                    snapshot["wire_final"]
                );
                assert_eq!(snapshot["wire_final"]["request_id"], *id);
                assert!(!snapshot.to_string().contains("synthetic-bearer-"));
                if let Ok(python) = var("CCRB_TEST_CAPTURE_PYTHON") {
                    let checked = Command::new(python).args(["-c", "import sys;from pathlib import Path;from native_capture import read_capture;read_capture(Path(sys.argv[1]),7,sys.argv[2])"])
                        .arg(root.join("environment")).arg(id).env("PYTHONPATH",var("CCRB_TEST_ORCHESTRATOR").unwrap()).status().await.unwrap();
                    assert!(checked.success());
                }
                snapshots.push(snapshot);
            }
            let evidence: Value = Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(var("CCRB_TEST_CONTROL").unwrap())
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let mut pids = BTreeSet::new();
            for snapshot in &snapshots {
                let wire = &snapshot["wire_final"];
                let client = wire["headers"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|pair| {
                        pair[0]
                            .as_str()
                            .unwrap()
                            .eq_ignore_ascii_case("x-client-request-id")
                    })
                    .unwrap()[1]
                    .as_str()
                    .unwrap();
                assert_eq!(
                    wire["body_sha256"],
                    evidence["tls_bodies"][client]["sha256"]
                );
                assert_eq!(
                    wire["observed_body_bytes"],
                    evidence["tls_bodies"][client]["bytes"]
                );
                pids.insert(wire["worker_pid"].as_u64().unwrap());
            }
            assert_eq!(pids.len(), 6);
            if let Ok(path) = var("CCRB_TEST_CAPTURE_FIXTURE") {
                fs::write(path, serde_json::to_vec(&snapshots[0]).unwrap()).unwrap();
            }
            let idle = std::mem::take(&mut pool.state.lock().unwrap().idle);
            for worker in idle {
                worker.retire().await;
            }
            fs::remove_dir_all(root).unwrap();
            return;
        }
        if var("CCRB_TEST_READINESS_BENCH_ONLY").as_deref() == Ok("1") {
            benchmark_readiness(&config, &make).await;
            fs::remove_dir_all(root).unwrap();
            return;
        }
        let mut insufficient = config.clone();
        insufficient.native.as_mut().unwrap().memory_mib =
            (POOL_MEMORY_MIB + (normal * 2) as u64 * WORKER_MEMORY_MIB) - 1;
        assert!(matches!(
            pool.submit(
                insufficient,
                make("insufficient-budget", "synthetic-bearer-a")
            )
            .await,
            Err(AppError::ServiceUnavailable(_))
        ));
        let mut wrong_identity = make("invalid-identity", "synthetic-bearer-a");
        wrong_identity.device_id = "A".repeat(64);
        assert!(matches!(
            pool.submit(config.clone(), wrong_identity).await,
            Err(AppError::ServiceUnavailable(_))
        ));
        let mut cancelling = make("cancelled-initialization", "synthetic-bearer-a");
        cancelling.input = json!({"schema_version":1,"request_id":cancelling.request.request_id});
        let control = NativePreparationControl {
            account_id: config.binding.account_id,
            environment_id: config.binding.environment_id.clone(),
            generation: config.binding.generation,
            preparation_id: cancelling.request.request_id.clone(),
        };
        let clone = pool.clone();
        let initializing_config = config.clone();
        let initializing = tokio::spawn(async move {
            clone
                .prepare(
                    initializing_config,
                    cancelling,
                    Instant::now() + Duration::from_secs(120),
                )
                .await
        });
        timeout(Duration::from_secs(10), async {
            while pool.state.lock().unwrap().live == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(pool.cancel_preparation(&control));
        assert!(initializing.await.unwrap().is_err());
        timeout(Duration::from_secs(5), async {
            while pool.state.lock().unwrap().live != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut preparation = make("zero-model-preparation", "synthetic-bearer-a");
        preparation.input = json!({"schema_version":1,"request_id":preparation.request.request_id});
        let status = pool
            .prepare(
                config.clone(),
                preparation.clone(),
                Instant::now() + Duration::from_secs(120),
            )
            .await
            .unwrap();
        assert_eq!(status.state, "ready");
        assert!(!status.reused);
        assert_eq!(status.idle_workers, 1);
        let repeated = pool
            .prepare(
                config.clone(),
                preparation.clone(),
                Instant::now() + Duration::from_secs(120),
            )
            .await
            .unwrap();
        assert!(repeated.reused);
        assert_eq!(status.pid, repeated.pid);
        let checked = pool.readiness(&config, &preparation).await.unwrap();
        assert_eq!(checked.state, "ready");
        assert_eq!(checked.pid, status.pid);
        let changed = make("changed-readiness", "synthetic-bearer-b");
        let stale = pool.readiness(&config, &changed).await.unwrap();
        assert_eq!(stale.state, "unprepared");
        assert!(stale.pid.is_none());
        // 用实际空闲年龄覆盖长时回收规则，跨真实回收窗口由隔离部署场景补验。
        for seconds in [60, 1800, 14400] {
            pool.state.lock().unwrap().idle[0].idle_since =
                Instant::now() - Duration::from_secs(seconds);
            pool.schedule_idle_reap();
            let checked = pool.readiness(&config, &preparation).await.unwrap();
            assert_eq!(checked.pid, status.pid);
            assert_eq!(checked.idle_workers, 1);
        }
        let idle_started = Instant::now();
        tokio::time::sleep(Duration::from_secs(61)).await;
        assert_eq!(
            pool.readiness(&config, &preparation).await.unwrap().pid,
            status.pid,
            "跨过真实 60 秒窗口仍保留唯一热进程"
        );
        println!(
            "{}",
            json!({"native_idle_keepalive_ms":idle_started.elapsed().as_millis(),"pid":status.pid})
        );
        let native = config.native.as_ref().unwrap();
        let cold = Instant::now();
        pool.verify(native).unwrap();
        let cold_us = cold.elapsed().as_micros();
        let warm = Instant::now();
        for _ in 0..20 {
            pool.verify(native).unwrap();
        }
        println!(
            "{}",
            json!({"native_distribution_verify":{"cold_us":cold_us,"warm_mean_us":warm.elapsed().as_micros()/20}})
        );
        let copied = root.join("verify-copy");
        fs::copy(&native.binary_path, &copied).unwrap();
        let mut mutable = native.clone();
        mutable.binary_path = copied.to_string_lossy().into_owned();
        // 签名发布从冻结目录解析实际路径；副本校验显式沿相同已验收身份。
        let verify_copy = || {
            if native.release.is_some() {
                let mut identity = artifact(native)?;
                identity.path = copied.clone();
                verify_artifact(&identity).map(|_| ())
            } else {
                pool.verify(&mutable)
            }
        };
        verify_copy().unwrap();
        let modified = fs::metadata(&copied).unwrap().modified().unwrap();
        let mut tampered = OpenOptions::new().write(true).open(&copied).unwrap();
        tampered.write_all(&[0]).unwrap();
        tampered
            .set_times(FileTimes::new().set_modified(modified))
            .unwrap();
        assert!(
            verify_copy().is_err(),
            "同长度且恢复 mtime 的原地修改仍由 ctime 与完整摘要拒绝"
        );
        fs::copy(&native.binary_path, &copied).unwrap();
        verify_copy().unwrap();
        fs::set_permissions(&copied, fs::Permissions::from_mode(0o777)).unwrap();
        assert!(verify_copy().is_err(), "权限变化使缓存立即失效");
        fs::remove_file(&copied).unwrap();
        fs::copy(&native.binary_path, &copied).unwrap();
        verify_copy().unwrap();
        pool.verify(native).unwrap();
        let mut adaptive = config.clone();
        adaptive.native_adaptive_prewarm = true;
        let first = make("first-warm-worker", "synthetic-bearer-a");
        let second = make("second-warm-worker", "synthetic-bearer-a");
        let deadline = Instant::now() + Duration::from_secs(30);
        let (key, mut first_worker) = pool
            .checkout(&adaptive, &first, deadline, true)
            .await
            .unwrap();
        assert_eq!(first_worker.child.as_ref().and_then(Child::id), status.pid);
        let second_pid = {
            let state = pool.state.lock().unwrap();
            assert_eq!(state.live, 2, "首条 checkout 已为补位保留唯一物理许可");
            assert!(state.spare.is_some() || state.idle.len() == 1);
            state
                .idle
                .first()
                .and_then(|worker| worker.child.as_ref().and_then(Child::id))
        };
        let (second_key, mut second_worker) = pool
            .checkout(&adaptive, &second, deadline, true)
            .await
            .unwrap();
        assert_eq!(key, second_key);
        let pid = second_worker.child.as_ref().and_then(Child::id);
        assert_ne!(pid, status.pid);
        if second_pid.is_some() {
            assert_eq!(pid, second_pid);
        }
        {
            let mut state = pool.state.lock().unwrap();
            assert_eq!(state.live, 2, "第二次 checkout 没有重复启动");
            assert!(state.spare.is_none());
            first_worker.idle_since = Instant::now();
            second_worker.idle_since = first_worker.idle_since;
            state.idle.extend([second_worker, first_worker]);
        }
        pool.schedule_idle_reap();
        timeout(Duration::from_secs(8), async {
            while pool.state.lock().unwrap().live != 1 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pool.state.lock().unwrap().idle.len(), 1);
        let observed: Value = Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(var("CCRB_TEST_CONTROL").unwrap())
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            observed["messages"].as_array().unwrap().len(),
            0,
            "真实准备和重复准备均为零模型调用"
        );
        let mut invalid = make("invalid", "synthetic-bearer-a");
        invalid.input["body"]["unsupported"] = json!(true);
        let response = pool.submit(config.clone(), invalid).await.unwrap();
        assert_eq!(response.status(), 422);
        to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(pool.state.lock().unwrap().idle.len(), 1);
        // 已就绪的暖进程也必须沿原截止拒绝投递，避免启动后重新授予首响应预算。
        let expired = make("expired-before-send", "synthetic-bearer-a");
        let deadline = validate_request(&expired.request, &config).unwrap();
        let (_, mut worker) = pool
            .checkout(&config, &expired, deadline, false)
            .await
            .unwrap();
        let (reply, _receive) = oneshot::channel();
        let mut reply = Some(reply);
        let (chunks, _receiver) = mpsc::channel(8);
        assert!(matches!(
            worker
                .execute(
                    &expired,
                    None,
                    &mut reply,
                    &chunks,
                    Instant::now() - Duration::from_millis(1),
                    Instant::now(),
                    NativeRuntimeObservation {
                        worker_id: Uuid::new_v4().to_string(),
                        worker_pid: 1,
                        kernel_generation: None,
                        warm_hit: true,
                        preparation_wait_ms: 0,
                        checkout_ms: 0,
                        worker_start_ms: None,
                        prepared_ms: 0,
                    }
                )
                .await,
            Err(AppError::ServiceUnavailable(_))
        ));
        assert!(reply.is_some());
        worker.retire().await;
        assert_eq!(pool.state.lock().unwrap().live, 0);
        for count in [normal, normal * 2] {
            let label = format!("parallel-{count}");
            let operations = (0..count).map(|_| {
                let pool = pool.clone();
                let config = config.clone();
                let mut submission = make(&label, "synthetic-bearer-a");
                if count == normal * 2 {
                    submission.input["body"]["model"] = json!("claude-haiku-4-5");
                }
                async move {
                    let response = pool.submit(config, submission).await.unwrap();
                    assert_eq!(response.status(), 200);
                    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
                    let value: Value = serde_json::from_slice(&bytes).unwrap();
                    assert_eq!(value["type"], "message");
                }
            });
            join_all(operations).await;
            assert_eq!(pool.state.lock().unwrap().live, count.max(normal));
        }
        let shared_inode = {
            let state = pool.state.lock().unwrap();
            let inode = state.idle[0]
                .execution_image
                .as_ref()
                .unwrap()
                .metadata()
                .unwrap()
                .ino();
            for worker in &state.idle {
                assert_eq!(
                    worker
                        .execution_image
                        .as_ref()
                        .unwrap()
                        .metadata()
                        .unwrap()
                        .ino(),
                    inode
                );
                let pid = worker.child.as_ref().unwrap().id().unwrap();
                assert_eq!(
                    fs::metadata(format!("/proc/{pid}/exe")).unwrap().ino(),
                    inode
                );
            }
            inode
        };
        timeout(Duration::from_secs(5), async {
            loop {
                if pool.state.lock().unwrap().live == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pool.state.lock().unwrap().idle.len(), 1);
        let response = pool
            .submit(config.clone(), make("slow", "synthetic-bearer-a"))
            .await
            .unwrap();
        drop(response);
        timeout(Duration::from_secs(5), async {
            loop {
                if pool.state.lock().unwrap().live == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let response = pool
            .submit(config.clone(), make("rotated", "synthetic-bearer-b"))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(pool.state.lock().unwrap().live, 1);
        assert_eq!(
            pool.state.lock().unwrap().idle[0]
                .execution_image
                .as_ref()
                .unwrap()
                .metadata()
                .unwrap()
                .ino(),
            shared_inode
        );
        let response = pool
            .submit(config.clone(), make("http-error", "synthetic-bearer-b"))
            .await
            .unwrap();
        assert_eq!(response.status(), 401);
        to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let mut second = make("second-account", "synthetic-bearer-c");
        second.device_id = "b".repeat(64);
        second.account_uuid = Some("33333333-3333-4333-8333-333333333333".into());
        let other = Arc::new(NativePool::default());
        let mut other_config = config.clone();
        other_config.binding.account_id = 8;
        other_config.binding.environment_id = "synthetic-account-b".into();
        second.request.account_id = 8;
        second.request.environment_id = other_config.binding.environment_id.clone();
        let response = other.submit(other_config, second).await.unwrap();
        to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        let control = var("CCRB_TEST_CONTROL").unwrap();
        let evidence: Value = Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(control)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(
            evidence["messages"].as_array().unwrap().len(),
            normal * 3 + 4
        );
        assert_eq!(evidence["parallel_peak"], normal * 2);
        let old_pid = pool.state.lock().unwrap().idle[0]
            .child
            .as_ref()
            .unwrap()
            .id()
            .unwrap();
        // 用额外物理许可模拟 HTTP 已退出、进程尚未收口，同时保留一个暖进程。
        pool.state.lock().unwrap().live += 1;
        let retiring = WorkerLease {
            key: String::new(),
            pool: Arc::downgrade(&pool),
        };
        let mut probe = make("switch-wait-physical-retirement", "synthetic-bearer-b");
        probe.input = json!({"schema_version":1,"request_id":probe.request.request_id});
        let mut switched = config.clone();
        switched.native.as_mut().unwrap().kernel_generation = Some(Uuid::new_v4().to_string());
        let prepare =
            pool.prepare_switch(switched, probe, Instant::now() + Duration::from_secs(20));
        tokio::pin!(prepare);
        assert!(
            timeout(Duration::from_millis(100), &mut prepare)
                .await
                .is_err()
        );
        assert_eq!(
            pool.state.lock().unwrap().idle[0]
                .child
                .as_ref()
                .unwrap()
                .id(),
            Some(old_pid)
        );
        assert_eq!(pool.state.lock().unwrap().live, 2);
        drop(retiring);
        let status = prepare.await.unwrap();
        assert_ne!(status.pid, Some(old_pid));
        assert!(fs::metadata(format!("/proc/{old_pid}/exe")).is_err());
        let idle = std::mem::take(&mut pool.state.lock().unwrap().idle);
        for worker in idle {
            worker.retire().await;
        }
        let idle = std::mem::take(&mut other.state.lock().unwrap().idle);
        for worker in idle {
            worker.retire().await;
        }
        benchmark_readiness(&config, &make).await;
        fs::remove_dir_all(root).unwrap();
    }
}
