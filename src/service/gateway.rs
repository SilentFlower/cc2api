use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::error::AppError;
use crate::model::account::{Account, AccountStatus};
use crate::model::api_token::ApiToken;
use crate::service::account::AccountService;
use crate::service::rewriter::{
    clean_session_id_from_body, detect_client_type, ClientType, Rewriter,
};
use crate::service::telemetry::TelemetryService;

const UPSTREAM_BASE: &str = "https://api.anthropic.com";
/// 并发槽位等待重试间隔。
const SLOT_WAIT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
/// 并发槽位最大重试次数（500ms × 60 = 30s）。
const SLOT_WAIT_MAX_RETRIES: usize = 60;
/// TTFB（Time To First Byte）超时：从 send() 到收到响应头的最长等待时间。
/// 握手 + 发请求 + 等上游开始响应，60s 足够覆盖正常场景；超过则认为上游卡住。
const UPSTREAM_TTFB_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// 流式读间隔超时：两次 bytes_stream chunk 之间允许的最长静默时间。
/// Anthropic SSE 每几秒至少有一个 ping event，120s 无数据视为连接卡死。
/// 该超时不限制流总时长，健康长流（Opus 扩展思考等）可持续任意时间。
const UPSTREAM_STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// 代理保活心跳间隔：每 N 秒向外部站点发一次 HEAD 请求(走同一代理),
/// 制造客户端→代理方向的字节流动,避免代理按"静默/低速"策略砍断长连接。
const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
/// 心跳目标 URL:选小响应、高可用的公共端点。
const HEARTBEAT_URL: &str = "https://api.ipify.org";

pub struct GatewayService {
    account_svc: Arc<AccountService>,
    rewriter: Arc<Rewriter>,
    telemetry_svc: Arc<TelemetryService>,
}

impl GatewayService {
    pub fn new(
        account_svc: Arc<AccountService>,
        rewriter: Arc<Rewriter>,
        telemetry_svc: Arc<TelemetryService>,
    ) -> Self {
        Self {
            account_svc,
            rewriter,
            telemetry_svc,
        }
    }

    /// 核心网关逻辑 -- axum handler。
    pub async fn handle_request(&self, req: Request, api_token: Option<&ApiToken>) -> Response {
        match self.handle_request_inner(req, api_token).await {
            Ok(resp) => resp,
            Err(e) => e.into_response(),
        }
    }

    async fn handle_request_inner(&self, req: Request, api_token: Option<&ApiToken>) -> Result<Response, AppError> {
        let req_start = std::time::Instant::now();
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();

        // 提取 header
        let headers = extract_headers(req.headers());
        let ua = headers.get("User-Agent").or_else(|| headers.get("user-agent")).cloned().unwrap_or_default();

        // 读取请求体
        let body_bytes = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
            .await
            .map_err(|e| AppError::BadRequest(format!("failed to read body: {}", e)))?;

        // 解析请求体
        let body_map: serde_json::Value = if body_bytes.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_slice(&body_bytes).unwrap_or(serde_json::json!({}))
        };

        // 检测客户端类型
        let client_type = detect_client_type(&ua, &body_map);

        // 生成会话哈希
        let session_hash =
            crate::service::account::generate_session_hash(&ua, &body_map, client_type);

        // 根据令牌限制构建账号过滤条件
        let (allowed_ids, blocked_ids) = if let Some(t) = api_token {
            (t.allowed_account_ids(), t.blocked_account_ids())
        } else {
            (vec![], vec![])
        };

        // 429 自动换号 / 并发降级重试循环
        let mut exclude_ids = blocked_ids.clone();
        let mut last_resp: Option<Response> = None;

        loop {
            let attempt = exclude_ids.len().saturating_sub(blocked_ids.len());
            // 选择账号
            let t0 = std::time::Instant::now();
            let account = match self
                .account_svc
                .select_account(&session_hash, &exclude_ids, &allowed_ids)
                .await
            {
                Ok(a) => {
                    info!("[耗时] 账号选择: {:.0}ms → {}", t0.elapsed().as_millis(), a.name);
                    a
                }
                Err(_) if last_resp.is_some() => {
                    // 无可用账号但有上一次的 429 响应，返回给客户端
                    return Ok(last_resp.unwrap());
                }
                Err(e) => {
                    // 仅当是"无可用账号"且有运行时排除的账号时，返回 429
                    if exclude_ids.len() > blocked_ids.len() {
                        if matches!(&e, AppError::ServiceUnavailable(_)) {
                            return Err(AppError::TooManyRequests(
                                "all accounts are busy".into(),
                            ));
                        }
                    }
                    return Err(AppError::ServiceUnavailable(format!(
                        "no available account: {}",
                        e
                    )));
                }
            };

            if attempt > 0 {
                warn!(
                    "429 retry attempt {} with account {}",
                    attempt, account.id
                );
            }

            // 自动遥测：拦截遥测请求 + 激活会话
            if account.auto_telemetry {
                use crate::service::telemetry::{is_telemetry_path, fake_metrics_enabled_response, fake_telemetry_response};

                if is_telemetry_path(&path) {
                    let body = if path.contains("/metrics_enabled") {
                        fake_metrics_enabled_response()
                    } else {
                        fake_telemetry_response()
                    };
                    debug!("telemetry: intercepted {} for account {}", path, account.id);
                    return Ok(axum::Json(body).into_response());
                }

                if path.starts_with("/v1/messages") {
                    let t_tel = std::time::Instant::now();
                    self.telemetry_svc.activate_session(&account).await;
                    info!("[耗时] 遥测激活: {:.0}ms", t_tel.elapsed().as_millis());
                }
            }

            // 获取并发槽位，满时排队等待
            let t_slot = std::time::Instant::now();
            let mut acquired = self
                .account_svc
                .acquire_slot(account.id, account.concurrency)
                .await
                .map_err(|_| AppError::TooManyRequests("concurrency slot unavailable".into()))?;
            if !acquired {
                // 检查等待队列是否已满
                let wait_key = format!("wait:account:{}", account.id);
                let entered_queue = self
                    .account_svc
                    .cache_acquire_wait(&wait_key, account.concurrency)
                    .await;
                if !entered_queue {
                    // 等待队列满：降级到其他账号，重新绑定粘性
                    warn!(
                        "account {} wait queue full, falling back to another account",
                        account.id
                    );
                    exclude_ids.push(account.id);
                    continue;
                }
                // scopeguard 保证即使请求被取消/丢弃也能释放等待计数
                let wait_svc = self.account_svc.clone();
                let wait_key_clone = wait_key.clone();
                let _wait_guard = scopeguard::guard((), move |_| {
                    let svc = wait_svc;
                    let key = wait_key_clone;
                    tokio::spawn(async move {
                        svc.cache_release_wait(&key).await;
                    });
                });
                // 排队等待槽位释放
                debug!("account {} slots full, queued for availability", account.id);
                for _ in 0..SLOT_WAIT_MAX_RETRIES {
                    tokio::time::sleep(SLOT_WAIT_INTERVAL).await;
                    match self.account_svc.acquire_slot(account.id, account.concurrency).await {
                        Ok(true) => { acquired = true; break; }
                        _ => {}
                    }
                }
                // scopeguard 会在此处自动释放等待队列计数（drop _wait_guard）
                if !acquired {
                    // 等待超时：降级到其他账号，重新绑定粘性
                    warn!(
                        "account {} slot wait timed out, falling back to another account",
                        account.id
                    );
                    exclude_ids.push(account.id);
                    continue;
                }
            }

            // 槽位释放绑定到响应体 stream 的生命周期上。
            // SlotReleaseGuard 兜底：若转发前出错或 panic，drop 时自动释放槽位。
            // 成功包装 stream 后 defuse() 解除，由 SlotGuardStream 接管释放。
            let slot_ms = t_slot.elapsed().as_millis();
            if slot_ms > 10 {
                info!("[耗时] 槽位获取: {:.0}ms (排队等待)", slot_ms);
            } else {
                info!("[耗时] 槽位获取: {:.0}ms", slot_ms);
            }
            let mut slot_guard = SlotReleaseGuard::new(self.account_svc.clone(), account.id);

            // 改写请求体
            let t_rewrite = std::time::Instant::now();
            debug!(
                "request body BEFORE rewrite: {}",
                truncate_body(&body_bytes, 4096)
            );
            let rewritten_body =
                self.rewriter
                    .rewrite_body(&body_bytes, &path, &account, client_type);
            debug!(
                "request body AFTER rewrite: {}",
                truncate_body(&rewritten_body, 4096)
            );

            // 重新解析改写后的 body
            let mut rewritten_body_map: serde_json::Value =
                serde_json::from_slice(&rewritten_body).unwrap_or(serde_json::json!({}));

            // 改写 header
            let model_id = body_map
                .get("model")
                .and_then(|m| m.as_str())
                .unwrap_or("");
            let rewritten_headers = self.rewriter.rewrite_headers(
                &headers,
                &account,
                client_type,
                model_id,
                &rewritten_body_map,
            );

            // 清理 body 中的 _session_id 标记并重新序列化
            let final_body = if client_type == ClientType::API {
                clean_session_id_from_body(&mut rewritten_body_map);
                serde_json::to_vec(&rewritten_body_map).unwrap_or_else(|_| rewritten_body.clone())
            } else {
                rewritten_body.clone()
            };

            let upstream_token = match self.account_svc.resolve_upstream_token(account.id).await {
                Ok(t) => t,
                Err(e) => {
                    // SlotReleaseGuard drop 会自动释放槽位
                    return Err(e);
                }
            };
            info!("[耗时] 请求改写+Token解析: {:.0}ms", t_rewrite.elapsed().as_millis());
            let mut final_headers = rewritten_headers;
            final_headers.insert(
                "authorization".into(),
                format!("Bearer {}", upstream_token),
            );

            // 转发到上游
            let t_upstream = std::time::Instant::now();
            let resp = match self
                .forward_request(
                    &method.to_string(),
                    &path,
                    &query,
                    &final_headers,
                    &final_body,
                    &account,
                )
                .await
            {
                Ok(r) => {
                    info!(
                        "[耗时] 上游响应: {:.0}ms (HTTP {})",
                        t_upstream.elapsed().as_millis(),
                        r.status().as_u16()
                    );
                    r
                }
                Err(e) => {
                    info!("[耗时] 上游失败: {:.0}ms", t_upstream.elapsed().as_millis());
                    // SlotReleaseGuard drop 会自动释放槽位
                    return Err(e);
                }
            };

            // 非 429：将响应体包装为 SlotGuardBody，流结束时释放槽位
            if resp.status() != StatusCode::TOO_MANY_REQUESTS {
                let (svc, account_id) = slot_guard.defuse();
                let (parts, body) = resp.into_parts();
                // 仅当账号配了代理时启动心跳(直连无意义)
                let heartbeat_task = if !account.proxy_url.is_empty() {
                    Some(tokio::spawn(heartbeat_loop(
                        account.proxy_url.clone(),
                        account.name.clone(),
                    )))
                } else {
                    None
                };
                let guarded_body = Body::new(SlotGuardBody::new(
                    body, svc, account_id, req_start, account.name.clone(),
                    heartbeat_task,
                ));
                return Ok(Response::from_parts(parts, guarded_body));
            }

            // 429：guard drop 会释放槽位，排除该账号，尝试下一个
            warn!(
                "account {} returned 429, excluding and retrying (attempt {})",
                account.id,
                attempt + 1,
            );
            exclude_ids.push(account.id);
            drop(slot_guard); // 显式 drop，释放槽位
            last_resp = Some(resp);
        }
    }

    async fn forward_request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &std::collections::HashMap<String, String>,
        body: &[u8],
        account: &Account,
    ) -> Result<Response, AppError> {
        let mut target_url = format!("{}{}", UPSTREAM_BASE, path);
        if !query.is_empty() {
            let q = if query.contains("beta=true") {
                query.to_string()
            } else {
                format!("{}&beta=true", query)
            };
            target_url = format!("{}?{}", target_url, q);
        } else {
            target_url = format!("{}?beta=true", target_url);
        }

        debug!("upstream URL: {}", target_url);

        let client = crate::tlsfp::make_request_client(&account.proxy_url);

        let mut req_builder = match method {
            "GET" => client.get(&target_url),
            "POST" => client.post(&target_url),
            "PUT" => client.put(&target_url),
            "DELETE" => client.delete(&target_url),
            "PATCH" => client.patch(&target_url),
            _ => client.post(&target_url),
        };

        for (k, v) in headers {
            debug!("upstream header: {}: {}", k, v);
            req_builder = req_builder.header(k, v);
        }
        req_builder = req_builder.header("Host", "api.anthropic.com");
        req_builder = req_builder.body(body.to_vec());

        let resp = tokio::time::timeout(UPSTREAM_TTFB_TIMEOUT, req_builder.send())
            .await
            .map_err(|_| {
                warn!("upstream TTFB timeout for account {}", account.id);
                AppError::BadGateway("upstream TTFB timeout".into())
            })?
            .map_err(|e| {
                warn!("upstream error for account {}: {}", account.id, e);
                AppError::BadGateway("upstream request failed".into())
            })?;

        let status_code = resp.status().as_u16();
        debug!("upstream response: {}", status_code);

        // 处理限速：429 根据账号类型分别处理
        // - SetupToken: 保守 5h 限流
        // - OAuth: 查用量判断是撞墙（5h / 7d）还是纯 rate limit，分别设置限流时长
        if status_code == 429 {
            if let Err(e) = self.account_svc.handle_rate_limit(account).await {
                warn!(
                    "failed to handle rate limit for account {}: {}",
                    account.id, e
                );
            }
        }

        // 处理认证失败：403 永久停用（但如果账号已处于 429 限流中则跳过，避免误判）
        if status_code == 403 {
            let is_rate_limited = account
                .rate_limit_reset_at
                .map(|reset| Utc::now() < reset)
                .unwrap_or(false);
            if is_rate_limited {
                warn!(
                    "account {} got 403 while rate-limited, skipping permanent disable",
                    account.id
                );
            } else if let Err(e) = self
                .account_svc
                .disable_account(
                    account.id,
                    AccountStatus::Disabled,
                    "403 认证失败",
                    None,
                )
                .await
            {
                warn!("failed to disable account {} for 403: {}", account.id, e);
            } else {
                warn!("account {} permanently disabled for 403", account.id);
            }
        }

        // 被动采集：从上游响应头提取用量数据，异步合并到数据库。
        // 429 时跳过，因为 handle_rate_limit 已通过 API 获取了完整数据，异步写入会覆盖。
        if status_code != 429 {
            if let Some(usage_json) = extract_passive_usage(resp.headers()) {
                let svc = self.account_svc.clone();
                let aid = account.id;
                tokio::spawn(async move {
                    if let Err(e) = svc.update_passive_usage(aid, usage_json).await {
                        debug!("passive usage update failed for account {}: {}", aid, e);
                    }
                });
            }
        }

        // 构建响应
        let mut response_builder = Response::builder().status(
            StatusCode::from_u16(status_code)
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        );

        for (k, v) in resp.headers() {
            let name = k.as_str();
            // 过滤已知 AI Gateway / 代理指纹响应头，防止客户端检测并上报
            if is_gateway_fingerprint_header(name) {
                continue;
            }
            response_builder = response_builder.header(k.clone(), v.clone());
        }

        // 流式传输响应体，给 bytes_stream 加两次 chunk 间隔超时以检测卡死连接。
        // 该超时只限制"静默时长"，对健康长流无影响。
        // 诊断日志区分三种终止场景：上游错误 / idle 超时 / 正常结束（无日志）。
        // [临时诊断] 追踪 chunk 间隔：任何 >10s 的静默都打 info 日志；错误/超时时汇总最大间隔。
        use tokio_stream::StreamExt;
        let account_name = account.name.clone();
        let idle_secs = UPSTREAM_STREAM_IDLE_TIMEOUT.as_secs();
        let stream_start = std::time::Instant::now();
        let mut last_chunk_at: Option<std::time::Instant> = None;
        let mut max_gap_ms: u128 = 0;
        let mut chunk_count: u64 = 0;
        let body_stream = resp
            .bytes_stream()
            .timeout(UPSTREAM_STREAM_IDLE_TIMEOUT)
            .map(move |r| match r {
                Ok(Ok(bytes)) => {
                    let now = std::time::Instant::now();
                    chunk_count += 1;
                    if let Some(prev) = last_chunk_at {
                        let gap_ms = now.duration_since(prev).as_millis();
                        if gap_ms > max_gap_ms {
                            max_gap_ms = gap_ms;
                        }
                        if gap_ms > 10_000 {
                            info!(
                                "[chunk-gap] {}ms (account: {}, #{} chunk, {} bytes, 已过 {}ms)",
                                gap_ms,
                                account_name,
                                chunk_count,
                                bytes.len(),
                                now.duration_since(stream_start).as_millis()
                            );
                        }
                    }
                    last_chunk_at = Some(now);
                    Ok(bytes)
                }
                Ok(Err(e)) => {
                    warn!(
                        "上游流错误 (account: {}, 已收 {} chunks, 最大间隔 {}ms): {}",
                        account_name, chunk_count, max_gap_ms, e
                    );
                    Err(std::io::Error::other(e))
                }
                Err(_elapsed) => {
                    warn!(
                        "上游流 idle {}s 超时 (account: {}, 已收 {} chunks, 最大间隔 {}ms)",
                        idle_secs, account_name, chunk_count, max_gap_ms
                    );
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "upstream stream idle timeout",
                    ))
                }
            });
        let body = Body::from_stream(body_stream);

        response_builder
            .body(body)
            .map_err(|e| AppError::Internal(format!("build response: {}", e)))
    }

}

fn extract_headers(headers: &HeaderMap) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for (k, v) in headers {
        if let Ok(val) = v.to_str() {
            map.insert(k.to_string(), val.to_string());
        }
    }
    map
}

/// Claude Code 主动扫描响应头检测 AI Gateway/代理（src/services/api/logging.ts）。
/// 过滤这些指纹前缀以防止客户端上报 gateway 类型。
/// Claude Code 扫描的 AI Gateway 响应头前缀（来源: src/services/api/logging.ts）。
const GATEWAY_HEADER_PREFIXES: &[&str] = &[
    "x-litellm-", "helicone-", "x-portkey-", "cf-aig-", "x-kong-", "x-bt-",
];

fn is_gateway_fingerprint_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    GATEWAY_HEADER_PREFIXES.iter().any(|p| lower.starts_with(p))
}

fn truncate_body(b: &[u8], max: usize) -> String {
    if b.len() > max {
        format!(
            "{}...(truncated)",
            String::from_utf8_lossy(&b[..max])
        )
    } else {
        String::from_utf8_lossy(b).to_string()
    }
}

/// 从上游响应头中提取 ratelimit 用量信息，构建与 OAuth usage API 格式一致的 JSON。
/// 仅保留 utilization 和 resets_at 都存在且可解析的完整窗口，避免不完整数据导致前端异常。
/// 没有任何完整窗口时返回 None。
fn extract_passive_usage(headers: &reqwest::header::HeaderMap) -> Option<serde_json::Value> {
    let get_str = |name: &str| -> Option<String> {
        headers.get(name).and_then(|v| v.to_str().ok()).map(|s| s.to_string())
    };

    let mut usage = serde_json::json!({});
    let mut has_window = false;

    // 5 小时窗口：utilization 和 resets_at 必须同时存在且可解析
    if let (Some(util_str), Some(reset_raw)) = (
        get_str("anthropic-ratelimit-unified-5h-utilization"),
        get_str("anthropic-ratelimit-unified-5h-reset"),
    ) {
        if let (Ok(util), Some(reset)) = (util_str.parse::<f64>(), normalize_reset_timestamp(&reset_raw)) {
            // 响应头返回 0~1 的比例，乘以 100 转为百分比，与 OAuth usage API 格式一致
            usage["five_hour"] = serde_json::json!({ "utilization": util * 100.0, "resets_at": reset });
            has_window = true;
        }
    }

    // 7 天窗口：同上
    if let (Some(util_str), Some(reset_raw)) = (
        get_str("anthropic-ratelimit-unified-7d-utilization"),
        get_str("anthropic-ratelimit-unified-7d-reset"),
    ) {
        if let (Ok(util), Some(reset)) = (util_str.parse::<f64>(), normalize_reset_timestamp(&reset_raw)) {
            usage["seven_day"] = serde_json::json!({ "utilization": util * 100.0, "resets_at": reset });
            has_window = true;
        }
    }

    if has_window { Some(usage) } else { None }
}

/// 将响应头中的重置时间统一转为 RFC3339 格式。
/// 响应头可能是 Unix 时间戳（秒）或已经是 ISO8601/RFC3339 字符串。
fn normalize_reset_timestamp(raw: &str) -> Option<String> {
    // 尝试解析为 Unix 时间戳（秒）
    if let Ok(ts) = raw.parse::<i64>() {
        return chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339());
    }
    // 尝试解析为 RFC3339，验证合法性
    if chrono::DateTime::parse_from_rfc3339(raw).is_ok() {
        return Some(raw.to_string());
    }
    None
}

/// 并发槽位释放守卫：drop 时自动释放槽位，防止错误路径或 panic 导致泄漏。
/// 成功包装 stream 后调用 `defuse()` 解除，由 `SlotGuardStream` 接管释放。
struct SlotReleaseGuard {
    inner: Option<(Arc<AccountService>, i64)>,
}

impl SlotReleaseGuard {
    fn new(svc: Arc<AccountService>, account_id: i64) -> Self {
        Self { inner: Some((svc, account_id)) }
    }

    /// 解除守卫并取出释放信息，调用后 drop 不再释放。
    fn defuse(&mut self) -> (Arc<AccountService>, i64) {
        self.inner.take().expect("SlotReleaseGuard already defused")
    }
}

impl Drop for SlotReleaseGuard {
    fn drop(&mut self) {
        if let Some((svc, account_id)) = self.inner.take() {
            tokio::spawn(async move {
                svc.release_slot(account_id).await;
            });
        }
    }
}

/// 代理保活心跳任务:周期性向 HEARTBEAT_URL 发 HEAD 请求,走同一代理。
///
/// 目的: 制造"客户端→代理方向"的字节流动,防止 havefun 等代理按空闲/低速
/// 策略在 ~300s 切断长连接。实测下会有效果;但如果代理按"per-connection"
/// 计时,这个独立连接上的活动未必能救回主流,需实际观察。
///
/// 任务被 `SlotGuardBody::drop` 中的 `abort()` 停止,主流结束立即终止心跳。
async fn heartbeat_loop(proxy_url: String, account_name: String) {
    let client = crate::tlsfp::make_request_client(&proxy_url);
    let mut count: u64 = 0;
    loop {
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
        count += 1;
        match client.head(HEARTBEAT_URL).send().await {
            Ok(resp) => {
                debug!(
                    "heartbeat #{} ok status={} (account: {})",
                    count,
                    resp.status().as_u16(),
                    account_name
                );
            }
            Err(e) => {
                debug!(
                    "heartbeat #{} failed (account: {}): {}",
                    count, account_name, e
                );
            }
        }
    }
}

/// 包装响应体 Body，在 body 传输完成或被丢弃时自动释放并发槽位。
///
/// 解决 Axum 流式响应中 handler 提前返回导致槽位过早释放的问题：
/// 将 release 逻辑绑定到 body 的生命周期上，确保槽位覆盖整个传输过程。
struct SlotGuardBody {
    inner: Body,
    /// `Some(...)` 表示槽位尚未释放，`Drop` 时 take 并执行释放。
    release: Option<(Arc<AccountService>, i64)>,
    /// 请求开始时间，用于计算首字耗时。
    req_start: std::time::Instant,
    /// 账号名称，用于日志输出。
    account_name: String,
    /// 是否已收到第一个 frame。
    first_frame_logged: bool,
    /// 心跳 task 句柄:Drop 时 abort,确保长请求结束后心跳立即停止。
    heartbeat_task: Option<tokio::task::JoinHandle<()>>,
}

impl SlotGuardBody {
    fn new(
        inner: Body,
        account_svc: Arc<AccountService>,
        account_id: i64,
        req_start: std::time::Instant,
        account_name: String,
        heartbeat_task: Option<tokio::task::JoinHandle<()>>,
    ) -> Self {
        Self {
            inner,
            release: Some((account_svc, account_id)),
            req_start,
            account_name,
            first_frame_logged: false,
            heartbeat_task,
        }
    }
}

impl http_body::Body for SlotGuardBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let result = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        if !self.first_frame_logged {
            if let std::task::Poll::Ready(Some(Ok(_))) = &result {
                self.first_frame_logged = true;
                info!(
                    "[耗时] 首字到达: {:.0}ms → {}",
                    self.req_start.elapsed().as_millis(),
                    self.account_name
                );
            }
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for SlotGuardBody {
    fn drop(&mut self) {
        info!(
            "[耗时] 传输结束: {:.0}ms → {}",
            self.req_start.elapsed().as_millis(),
            self.account_name
        );
        // 主流结束立即停掉心跳,避免残留后台任务
        if let Some(task) = self.heartbeat_task.take() {
            task.abort();
        }
        if let Some((svc, account_id)) = self.release.take() {
            tokio::spawn(async move {
                svc.release_slot(account_id).await;
            });
        }
    }
}
