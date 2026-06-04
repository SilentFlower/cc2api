use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;
use tracing::{debug, info, warn};

use crate::error::AppError;
use crate::model::account::{Account, AccountStatus};
use crate::model::api_token::ApiToken;
use crate::service::account::{AccountService, QueueWaitError};
use crate::service::rewriter::{
    clean_session_id_from_body, detect_client_type, ClientType, Rewriter,
};
use crate::service::telemetry::{
    MessageTelemetryContext, MessageTelemetryResult, TelemetryService,
};

const UPSTREAM_BASE: &str = "https://api.anthropic.com";
/// 账号级 FIFO 排队的最长等待时长。超时后会降级到其他账号；队列上限仍由 concurrency 控制。
const SLOT_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// TTFB（Time To First Byte）超时：从 send() 到收到响应头的最长等待时间。
/// 握手 + 发请求 + 等上游开始响应，120s 足以覆盖非流式 Opus + 扩展思考场景；超过则认为上游卡住。
const UPSTREAM_TTFB_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// 流式读间隔超时：两次 bytes_stream chunk 之间允许的最长静默时间。
/// Anthropic SSE 每几秒至少有一个 ping event，120s 无数据视为连接卡死。
/// 该超时不限制流总时长，健康长流（Opus 扩展思考等）可持续任意时间。
const UPSTREAM_STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

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

    async fn handle_request_inner(
        &self,
        req: Request,
        api_token: Option<&ApiToken>,
    ) -> Result<Response, AppError> {
        let req_start = std::time::Instant::now();
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();

        // 提取 header
        let headers = extract_headers(req.headers());
        let ua = headers
            .get("User-Agent")
            .or_else(|| headers.get("user-agent"))
            .cloned()
            .unwrap_or_default();

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
                    info!(
                        "[耗时] 账号选择: {:.0}ms → {}",
                        t0.elapsed().as_millis(),
                        a.name
                    );
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
                            return Err(AppError::TooManyRequests("all accounts are busy".into()));
                        }
                    }
                    return Err(AppError::ServiceUnavailable(format!(
                        "no available account: {}",
                        e
                    )));
                }
            };

            if attempt > 0 {
                warn!("429 retry attempt {} with account {}", attempt, account.id);
            }

            // 自动遥测：拦截遥测请求 + 激活会话
            if account.auto_telemetry {
                use crate::service::telemetry::{
                    fake_metrics_enabled_response, fake_telemetry_response, is_telemetry_path,
                };

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

            // 获取并发槽位：走账号级 FIFO 排队器（tokio Semaphore 按调用顺序授予 permit）
            let t_slot = std::time::Instant::now();
            let queue = self
                .account_svc
                .get_or_create_queue(account.id, account.concurrency)
                .await;
            let slot_permit = match queue.acquire(SLOT_WAIT_TIMEOUT).await {
                Ok(p) => p,
                Err(QueueWaitError::QueueFull) => {
                    warn!(
                        "account {} wait queue full, falling back to another account",
                        account.id
                    );
                    exclude_ids.push(account.id);
                    continue;
                }
                Err(QueueWaitError::Timeout) => {
                    warn!(
                        "account {} slot wait timed out, falling back to another account",
                        account.id
                    );
                    exclude_ids.push(account.id);
                    continue;
                }
                Err(QueueWaitError::Closed) => {
                    return Err(AppError::Internal("slot semaphore closed".into()));
                }
            };

            // 槽位释放绑定到响应体 stream 的生命周期上。
            // SlotReleaseGuard 兜底：若转发前出错或 panic，drop permit 自动归还槽位。
            // 成功包装 stream 后 defuse() 解除，由 SlotGuardBody 持有 permit 直到流结束。
            let slot_ms = t_slot.elapsed().as_millis();
            if slot_ms > 10 {
                info!("[耗时] 槽位获取: {:.0}ms (排队等待)", slot_ms);
            } else {
                info!("[耗时] 槽位获取: {:.0}ms", slot_ms);
            }
            let mut slot_guard = SlotReleaseGuard::new(slot_permit);

            // 改写请求体
            let t_rewrite = std::time::Instant::now();
            debug!(
                "request body summary BEFORE rewrite: {}",
                safe_body_summary(&body_bytes)
            );
            let rewritten_body =
                self.rewriter
                    .rewrite_body(&body_bytes, &path, &account, client_type);
            debug!(
                "request body summary AFTER rewrite: {}",
                safe_body_summary(&rewritten_body)
            );

            // 重新解析改写后的 body
            let mut rewritten_body_map: serde_json::Value =
                serde_json::from_slice(&rewritten_body).unwrap_or(serde_json::json!({}));

            // 改写 header
            let model_id = body_map.get("model").and_then(|m| m.as_str()).unwrap_or("");
            let rewritten_headers = self.rewriter.rewrite_headers(
                &headers,
                &path,
                &account,
                client_type,
                model_id,
                &rewritten_body_map,
            );

            // 清理 body 中的 _session_id 标记并重新序列化
            let telemetry_context = if account.auto_telemetry && path.starts_with("/v1/messages") {
                let context = build_message_telemetry_context(
                    &body_map,
                    &rewritten_body_map,
                    body_bytes.len(),
                    rewritten_body.len(),
                    client_type,
                    attempt,
                );
                self.telemetry_svc
                    .record_message_request(&account, context.clone())
                    .await;
                Some(context)
            } else {
                None
            };

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
            info!(
                "[耗时] 请求改写+Token解析: {:.0}ms",
                t_rewrite.elapsed().as_millis()
            );
            let mut final_headers = rewritten_headers;
            final_headers.insert("authorization".into(), format!("Bearer {}", upstream_token));

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
                    if let Some(context) = telemetry_context.clone() {
                        self.telemetry_svc
                            .record_message_result(
                                &account,
                                context,
                                MessageTelemetryResult {
                                    status_code: Some(r.status().as_u16()),
                                    duration_ms: t_upstream.elapsed().as_millis() as u64,
                                    error_kind: None,
                                },
                            )
                            .await;
                    }
                    info!(
                        "[耗时] 上游响应: {:.0}ms (HTTP {})",
                        t_upstream.elapsed().as_millis(),
                        r.status().as_u16()
                    );
                    r
                }
                Err(e) => {
                    if let Some(context) = telemetry_context.clone() {
                        self.telemetry_svc
                            .record_message_result(
                                &account,
                                context,
                                MessageTelemetryResult {
                                    status_code: None,
                                    duration_ms: t_upstream.elapsed().as_millis() as u64,
                                    error_kind: Some("upstream_error".into()),
                                },
                            )
                            .await;
                    }
                    info!("[耗时] 上游失败: {:.0}ms", t_upstream.elapsed().as_millis());
                    // SlotReleaseGuard drop 会自动释放槽位
                    return Err(e);
                }
            };

            // 非 429：将响应体包装为 SlotGuardBody，流结束时归还槽位
            if resp.status() != StatusCode::TOO_MANY_REQUESTS {
                let permit = slot_guard.defuse();
                let (parts, body) = resp.into_parts();
                let guarded_body = Body::new(SlotGuardBody::new(
                    body,
                    permit,
                    req_start,
                    account.name.clone(),
                ));
                return Ok(Response::from_parts(parts, guarded_body));
            }

            // 429：guard drop 会归还槽位 permit，排除该账号，尝试下一个
            warn!(
                "account {} returned 429, excluding and retrying (attempt {})",
                account.id,
                attempt + 1,
            );
            exclude_ids.push(account.id);
            drop(slot_guard); // 显式 drop → permit drop → 归还槽位
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

        // 429：上游拒绝了这条请求。先缓冲（很小的）响应体,据此区分两类 429：
        // - 「单请求级拒绝」（如长上下文需 usage credits、额度不足）：与账号容量无关,
        //   绝不隔离账号,直接把 429 透传回客户端(否则一条坏请求会把整个号打成「限流中」)。
        // - 「账号级限流」（撞 5h/7d 墙、真·速率限制）：交给 handle_rate_limit 决定隔离时长。
        // 判定依据(响应体关键字 / retry-after 头)由 handle_rate_limit 内部处理。
        if status_code == 429 {
            // bytes() 会消费 resp,故先克隆需要的响应头
            let headers_429 = resp.headers().clone();
            let retry_after = parse_retry_after(&headers_429);
            // 用这条 429 响应自带的 ratelimit 头做撞墙判断(被动,不再主动查 usage 接口)
            let usage_from_headers = extract_passive_usage(&headers_429);
            let body_bytes = resp.bytes().await.unwrap_or_default();
            // 取 owned 字符串,避免后续把 body_bytes move 进 Body 时发生借用冲突。
            // 注:本客户端不自动解压(透传 content-encoding),此处按明文匹配关键字。
            // Anthropic 错误体很小、基本不压缩,匹配可靠;万一被压缩导致匹配落空,
            // 只会退回「短冷却」而非「不隔离」——属安全降级,不会比旧行为更糟。
            let body_snippet = String::from_utf8_lossy(&body_bytes).into_owned();

            if let Err(e) = self
                .account_svc
                .handle_rate_limit(account, retry_after, &body_snippet, usage_from_headers)
                .await
            {
                warn!(
                    "failed to handle rate limit for account {}: {}",
                    account.id, e
                );
            }

            // 用缓冲的 body 重建 429 响应（无需流式）,过滤掉网关指纹响应头后返回。
            let mut rb = Response::builder().status(StatusCode::TOO_MANY_REQUESTS);
            for (k, v) in headers_429.iter() {
                if is_gateway_fingerprint_header(k.as_str()) {
                    continue;
                }
                rb = rb.header(k.clone(), v.clone());
            }
            return rb
                .body(Body::from(body_bytes))
                .map_err(|e| AppError::Internal(format!("build 429 response: {}", e)));
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
                .disable_account(account.id, AccountStatus::Disabled, "403 认证失败", None)
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
        let mut response_builder = Response::builder()
            .status(StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));

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
    "x-litellm-",
    "helicone-",
    "x-portkey-",
    "cf-aig-",
    "x-kong-",
    "x-bt-",
];

fn is_gateway_fingerprint_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    GATEWAY_HEADER_PREFIXES.iter().any(|p| lower.starts_with(p))
}

/// 解析 429 响应的 `retry-after` 头（秒）。仅接受正整数秒;HTTP-date 形式忽略。
/// 上游给了明确退避时间时,据此精确冷却,优于自行猜测。
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<i64> {
    headers
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|n| *n > 0)
}

fn safe_body_summary(b: &[u8]) -> String {
    let digest = Sha256::digest(b);
    format!("{} bytes sha256:{}", b.len(), hex::encode(&digest[..8]))
}

fn build_message_telemetry_context(
    original_body: &serde_json::Value,
    rewritten_body: &serde_json::Value,
    request_body_bytes: usize,
    rewritten_body_bytes: usize,
    client_type: ClientType,
    attempt: usize,
) -> MessageTelemetryContext {
    MessageTelemetryContext {
        model: original_body
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .to_string(),
        session_id: extract_message_session_id(rewritten_body)
            .or_else(|| crate::service::rewriter::extract_session_id_from_body(rewritten_body)),
        request_body_bytes,
        rewritten_body_bytes,
        stream: original_body
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        tool_count: original_body
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|tools| tools.len())
            .unwrap_or(0),
        attachment_count: count_attachment_blocks(original_body),
        system_prompt_block_count: count_system_prompt_blocks(rewritten_body),
        client_type: match client_type {
            ClientType::ClaudeCode => "claude_code",
            ClientType::API => "api",
        }
        .into(),
        attempt,
    }
}

fn extract_message_session_id(body: &serde_json::Value) -> Option<String> {
    let user_id = body
        .get("metadata")
        .and_then(|m| m.get("user_id"))
        .and_then(|u| u.as_str())?;
    let parsed = serde_json::from_str::<serde_json::Value>(user_id).ok()?;
    parsed
        .get("session_id")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

fn count_system_prompt_blocks(body: &serde_json::Value) -> usize {
    match body.get("system") {
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter(|item| item.get("type").and_then(|t| t.as_str()) == Some("text"))
            .count(),
        Some(serde_json::Value::String(s)) if !s.is_empty() => 1,
        _ => 0,
    }
}

fn count_attachment_blocks(body: &serde_json::Value) -> usize {
    let mut count = 0;
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for message in messages {
            count +=
                count_attachment_value(message.get("content").unwrap_or(&serde_json::Value::Null));
        }
    }
    count
}

fn count_attachment_value(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(items) => items.iter().map(count_attachment_value).sum(),
        serde_json::Value::Object(map) => {
            let kind = map.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let self_count = matches!(
                kind,
                "image" | "image_url" | "document" | "file" | "input_image" | "input_file"
            ) as usize;
            self_count
                + map
                    .get("content")
                    .map(count_attachment_value)
                    .unwrap_or_default()
        }
        _ => 0,
    }
}

/// 从上游响应头中提取 ratelimit 用量信息，构建与 OAuth usage API 格式一致的 JSON。
/// 仅保留 utilization 和 resets_at 都存在且可解析的完整窗口，避免不完整数据导致前端异常。
/// 没有任何完整窗口时返回 None。
pub(crate) fn extract_passive_usage(
    headers: &reqwest::header::HeaderMap,
) -> Option<serde_json::Value> {
    let get_str = |name: &str| -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };

    let mut usage = serde_json::json!({});
    let mut has_window = false;

    // 5 小时窗口：utilization 和 resets_at 必须同时存在且可解析
    if let (Some(util_str), Some(reset_raw)) = (
        get_str("anthropic-ratelimit-unified-5h-utilization"),
        get_str("anthropic-ratelimit-unified-5h-reset"),
    ) {
        if let (Ok(util), Some(reset)) = (
            util_str.parse::<f64>(),
            normalize_reset_timestamp(&reset_raw),
        ) {
            // 响应头返回 0~1 的比例，乘以 100 转为百分比，与 OAuth usage API 格式一致
            usage["five_hour"] =
                serde_json::json!({ "utilization": util * 100.0, "resets_at": reset });
            has_window = true;
        }
    }

    // 7 天窗口：同上
    if let (Some(util_str), Some(reset_raw)) = (
        get_str("anthropic-ratelimit-unified-7d-utilization"),
        get_str("anthropic-ratelimit-unified-7d-reset"),
    ) {
        if let (Ok(util), Some(reset)) = (
            util_str.parse::<f64>(),
            normalize_reset_timestamp(&reset_raw),
        ) {
            usage["seven_day"] =
                serde_json::json!({ "utilization": util * 100.0, "resets_at": reset });
            has_window = true;
        }
    }

    if has_window {
        Some(usage)
    } else {
        None
    }
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

/// 并发槽位释放守卫：drop permit 时自动归还 semaphore，防止错误路径或 panic 导致泄漏。
/// 成功包装 stream 后调用 `defuse()` 把 permit 交给 `SlotGuardBody` 管理。
struct SlotReleaseGuard {
    permit: Option<OwnedSemaphorePermit>,
}

impl SlotReleaseGuard {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            permit: Some(permit),
        }
    }

    /// 解除守卫并取出 permit，调用后本 guard drop 不再释放。
    fn defuse(&mut self) -> OwnedSemaphorePermit {
        self.permit
            .take()
            .expect("SlotReleaseGuard already defused")
    }
}

impl Drop for SlotReleaseGuard {
    fn drop(&mut self) {
        // permit 的 Drop 本身就会归还 semaphore,无需额外动作
        let _ = self.permit.take();
    }
}

/// 包装响应体 Body，在 body 传输完成或被丢弃时自动归还并发槽位。
///
/// 解决 Axum 流式响应中 handler 提前返回导致槽位过早释放的问题：
/// 将 permit 的生命周期绑定到 body 上，确保槽位覆盖整个传输过程。
struct SlotGuardBody {
    inner: Body,
    /// `Some(permit)` 表示槽位尚未归还；`Drop` 时 take 让 permit 自动归还 semaphore。
    permit: Option<OwnedSemaphorePermit>,
    /// 请求开始时间，用于计算首字耗时。
    req_start: std::time::Instant,
    /// 账号名称，用于日志输出。
    account_name: String,
    /// 是否已收到第一个 frame。
    first_frame_logged: bool,
}

impl SlotGuardBody {
    fn new(
        inner: Body,
        permit: OwnedSemaphorePermit,
        req_start: std::time::Instant,
        account_name: String,
    ) -> Self {
        Self {
            inner,
            permit: Some(permit),
            req_start,
            account_name,
            first_frame_logged: false,
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
        // permit 的 Drop 负责归还 semaphore
        let _ = self.permit.take();
    }
}

#[cfg(test)]
mod tests {
    use super::{build_message_telemetry_context, extract_message_session_id, safe_body_summary};
    use crate::service::rewriter::ClientType;
    use serde_json::json;

    #[test]
    fn safe_body_summary_hides_raw_body() {
        let body = br#"{"private_text":"raw-prompt-marker","private_token":"raw-token-marker"}"#;
        let summary = safe_body_summary(body);

        assert!(summary.starts_with(&format!("{} bytes sha256:", body.len())));
        assert!(!summary.contains("raw-prompt-marker"));
        assert!(!summary.contains("raw-token-marker"));
    }

    #[test]
    fn message_context_extracts_safe_counts_and_session_id() {
        let original_body = json!({
            "model": "claude-sonnet-4-20250514",
            "stream": true,
            "tools": [{"name": "Read"}, {"name": "Edit"}],
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "raw-prompt-marker"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "redacted"}},
                    {"type": "input_file", "file_id": "file_123"}
                ]
            }]
        });
        let rewritten_body = json!({
            "metadata": {
                "user_id": "{\"session_id\":\"session-from-user-id\"}"
            },
            "system": [
                {"type": "text", "text": "system prompt body"}
            ]
        });

        let context = build_message_telemetry_context(
            &original_body,
            &rewritten_body,
            111,
            222,
            ClientType::ClaudeCode,
            1,
        );

        assert_eq!(context.model, "claude-sonnet-4-20250514");
        assert_eq!(context.session_id.as_deref(), Some("session-from-user-id"));
        assert_eq!(context.request_body_bytes, 111);
        assert_eq!(context.rewritten_body_bytes, 222);
        assert!(context.stream);
        assert_eq!(context.tool_count, 2);
        assert_eq!(context.attachment_count, 2);
        assert_eq!(context.system_prompt_block_count, 1);
        assert_eq!(context.client_type, "claude_code");
        assert_eq!(context.attempt, 1);
    }

    #[test]
    fn message_context_falls_back_to_internal_session_id() {
        let rewritten_body = json!({
            "metadata": {
                "_session_id": "fallback-session"
            },
            "system": "system prompt body"
        });

        assert_eq!(extract_message_session_id(&rewritten_body), None);

        let context = build_message_telemetry_context(
            &json!({}),
            &rewritten_body,
            10,
            20,
            ClientType::API,
            0,
        );

        assert_eq!(context.session_id.as_deref(), Some("fallback-session"));
        assert_eq!(context.system_prompt_block_count, 1);
        assert_eq!(context.client_type, "api");
    }
}
