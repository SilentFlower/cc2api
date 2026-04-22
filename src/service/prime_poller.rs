use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Timelike, Utc};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::model::account::{Account, AccountStatus};
use crate::service::account::AccountService;
use crate::service::gateway::extract_passive_usage;
use crate::service::rewriter::{clean_session_id_from_body, ClientType, Rewriter};
use crate::store::prime_log_store::{PrimeLogEntry, PrimeLogStore};
use crate::store::settings_store::SettingsStore;

/// 上游 `/v1/messages` 地址,与 gateway.rs 的 UPSTREAM_BASE 保持一致。
const UPSTREAM_URL: &str = "https://api.anthropic.com/v1/messages?beta=true";

/// 账号之间的最小间隔,防止瞬间并发打爆上游。
const PER_ACCOUNT_GAP: Duration = Duration::from_millis(500);

/// 预热请求 TTFB 超时,短于主链路的 gateway 超时(预热非关键路径,快速失败为宜)。
const PRIME_TTFB_TIMEOUT: Duration = Duration::from_secs(30);

/// 主循环扫描间隔。触发点为 HH:10 分,30s 扫一次足以确保分钟内命中至少一次。
const TICK_INTERVAL: Duration = Duration::from_secs(30);

/// 约定的触发分钟:每天在 HH:10 分发起预热,避免 :00/:30 的全球流量峰值。
const TRIGGER_MINUTE: u32 = 10;

/// 错误信息截断字符数。按字符而非字节截,避免多字节字符跨边界导致 panic。
const ERROR_SNIPPET_CHARS: usize = 200;

/// 预热解析出的有效配置。
struct PrimeConfig {
    enabled: bool,
    hours: Vec<u32>,
    model: String,
}

/// 预热结果,用于写入日志表。
struct PrimeOutcome {
    success: bool,
    error_message: String,
    duration_ms: i64,
    passive_usage: Option<serde_json::Value>,
    /// 上游 HTTP 状态码,None 表示请求没到达上游(如 token 解析失败、序列化失败)。
    /// 用于后续按状态码做 429/403 处理。
    status: Option<u16>,
}

/// 峰值 5h 窗口预热服务。
///
/// 每天在配置的清晨时间点(默认 4:10 / 5:10 / 6:10 本地时间)对所有活跃且未处于速率冷却
/// 期的账号逐一发送一次 Haiku `/v1/messages` 请求,借此主动启动 Anthropic 侧的 5h 速率
/// 限制窗口,让窗口重置点落在下午用户高峰之前或之中。
///
/// 失败不重试,每次调用(含成功/失败/跳过)都会写入 `prime_logs` 表,供设置页展示。
/// 429 / 403 会联动 `AccountService` 的状态处理,和主链路行为保持一致,防止预热探测到
/// 坏账号却让真实流量继续选中它。
pub struct PrimePollerService {
    account_svc: Arc<AccountService>,
    settings_store: Arc<SettingsStore>,
    log_store: Arc<PrimeLogStore>,
    rewriter: Arc<Rewriter>,
}

impl PrimePollerService {
    /// 构造服务实例。
    pub fn new(
        account_svc: Arc<AccountService>,
        settings_store: Arc<SettingsStore>,
        log_store: Arc<PrimeLogStore>,
        rewriter: Arc<Rewriter>,
    ) -> Self {
        Self {
            account_svc,
            settings_store,
            log_store,
            rewriter,
        }
    }

    /// 启动后台循环。应在 main 中 tokio::spawn 调用。
    pub async fn run(self: Arc<Self>) {
        info!(
            "prime poller: started, trigger minute = :{:02}, tick = {:?}",
            TRIGGER_MINUTE, TICK_INTERVAL
        );

        // 去重键:(本地日期, 小时) 用于避免同一触发点内多次 tick 重复执行。
        let mut last_fired: Option<(NaiveDate, u32)> = None;

        loop {
            self.check_once(&mut last_fired).await;
            sleep(TICK_INTERVAL).await;
        }
    }

    /// 单次扫描:判断当前本地时间是否命中触发点,命中则执行一轮预热。
    async fn check_once(&self, last_fired: &mut Option<(NaiveDate, u32)>) {
        // 使用服务器本地时间(chrono::Local)。部署时需要确保容器 TZ 正确,
        // 否则 4/5/6 点会错位到 UTC 对应小时。
        let now = chrono::Local::now();
        if now.minute() != TRIGGER_MINUTE {
            return;
        }

        let h = now.hour();
        let key = (now.date_naive(), h);
        // 同一(日期,小时)只触发一次,防止 30s 扫描在同一分钟内重复进入。
        if *last_fired == Some(key) {
            return;
        }

        let cfg = match self.load_config().await {
            Some(c) => c,
            None => return,
        };
        if !cfg.enabled || !cfg.hours.contains(&h) {
            return;
        }

        *last_fired = Some(key);
        info!("prime poller: tick at {}:{:02}, model = {}", h, TRIGGER_MINUTE, cfg.model);
        self.run_round(h, &cfg.model).await;
    }

    /// 读取并解析设置,解析失败或总开关关闭返回 None。
    async fn load_config(&self) -> Option<PrimeConfig> {
        let all = match self.settings_store.get_all().await {
            Ok(a) => a,
            Err(e) => {
                warn!("prime poller: load settings failed: {}", e);
                return None;
            }
        };

        let enabled = all
            .get("peak_prime_enabled")
            .map(|s| s == "true")
            .unwrap_or(true);
        let hours_raw = all
            .get("peak_prime_hours")
            .cloned()
            .unwrap_or_else(|| "4,5,6".to_string());
        let hours: Vec<u32> = hours_raw
            .split(',')
            .filter_map(|s| s.trim().parse::<u32>().ok())
            .filter(|h| *h < 24)
            .collect();
        let model = all
            .get("peak_prime_model")
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "claude-haiku-4-5-20251001".to_string());

        Some(PrimeConfig {
            enabled,
            hours,
            model,
        })
    }

    /// 对所有活跃账号依次发起预热请求,账号间间隔 PER_ACCOUNT_GAP。
    /// 仍处于 429 冷却期的账号会被跳过(避免续命式延长冷却),但仍写入 skip 日志。
    async fn run_round(&self, hour: u32, model: &str) {
        let accounts = match self.account_svc.list_accounts().await {
            Ok(list) => list,
            Err(e) => {
                warn!("prime poller: list accounts failed: {}", e);
                return;
            }
        };

        // 只看启用状态的账号;disabled/error 与预热无关。
        // 冷却期判断在循环内做,因为要记 skip 日志。
        let targets: Vec<Account> = accounts
            .into_iter()
            .filter(|a| a.status == AccountStatus::Active)
            .collect();

        debug!("prime poller: evaluating {} active accounts", targets.len());

        for account in targets {
            let triggered_at = chrono::Local::now().to_rfc3339();
            let outcome = if account.is_schedulable() {
                self.prime_one(&account, model).await
            } else {
                // 处于速率冷却期,不发请求,但在日志里留痕,便于排查"今天没预热"的原因。
                let reset = account
                    .rate_limit_reset_at
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_else(|| "unknown".into());
                PrimeOutcome {
                    success: false,
                    error_message: format!("skipped: rate-limited until {}", reset),
                    duration_ms: 0,
                    passive_usage: None,
                    status: None,
                }
            };

            // 落日志:info/warn 级别打点,便于 grep;同时写入 DB 供前端展示。
            if outcome.success {
                info!(
                    "prime: account={} hour={} model={} success=true duration={}ms",
                    account.id, hour, model, outcome.duration_ms
                );
            } else {
                warn!(
                    "prime: account={} hour={} model={} success=false duration={}ms error={}",
                    account.id, hour, model, outcome.duration_ms, outcome.error_message
                );
            }

            let entry = PrimeLogEntry {
                id: 0,
                account_id: account.id,
                account_name: account.name.clone(),
                triggered_at,
                hour: hour as i32,
                model: model.to_string(),
                success: outcome.success,
                error_message: outcome.error_message.clone(),
                duration_ms: outcome.duration_ms,
            };
            if let Err(e) = self.log_store.insert(&entry).await {
                warn!("prime poller: write log failed for account {}: {}", account.id, e);
            }

            // 根据上游状态码联动账号状态,保持与主链路 gateway 一致:
            // - 429: 调用 handle_rate_limit,按账号类型/用量设置冷却窗口。
            // - 403: 若未处于 429 冷却期则永久停用,避免坏账号继续被真实流量选中。
            if let Some(code) = outcome.status {
                self.apply_status_side_effects(&account, code).await;
            }

            // 合并响应头中的 ratelimit 数据,刷新账号窗口信息,供调度/展示使用。
            // 429 场景不覆盖,因为 handle_rate_limit 已写入更完整的数据(可能从 OAuth usage API)。
            if outcome.status != Some(429) {
                if let Some(usage) = outcome.passive_usage {
                    if let Err(e) = self.account_svc.update_passive_usage(account.id, usage).await {
                        warn!(
                            "prime poller: update passive usage failed for account {}: {}",
                            account.id, e
                        );
                    }
                }
            }

            sleep(PER_ACCOUNT_GAP).await;
        }
    }

    /// 根据上游 HTTP 状态码联动账号状态。
    ///
    /// 与 `src/service/gateway.rs:359/369` 的处理保持一致:
    /// - 429 → `handle_rate_limit` 按类型/用量设置冷却;
    /// - 403 且未处于 429 冷却期 → `disable_account` 永久停用,防止真实流量继续选中。
    async fn apply_status_side_effects(&self, account: &Account, status: u16) {
        if status == 429 {
            if let Err(e) = self.account_svc.handle_rate_limit(account).await {
                warn!(
                    "prime poller: handle_rate_limit failed for account {}: {}",
                    account.id, e
                );
            }
            return;
        }
        if status == 403 {
            // 账号可能已在 429 冷却期,此时 403 是冷却期的副作用响应,不做停用。
            // 该判断与 gateway.rs 完全一致,避免误判断账号。
            let is_rate_limited = account
                .rate_limit_reset_at
                .map(|reset| Utc::now() < reset)
                .unwrap_or(false);
            if is_rate_limited {
                warn!(
                    "prime poller: account {} got 403 while rate-limited, skipping disable",
                    account.id
                );
                return;
            }
            if let Err(e) = self
                .account_svc
                .disable_account(account.id, AccountStatus::Disabled, "403 认证失败", None)
                .await
            {
                warn!(
                    "prime poller: disable_account failed for account {}: {}",
                    account.id, e
                );
            } else {
                warn!(
                    "prime poller: account {} permanently disabled for 403",
                    account.id
                );
            }
        }
    }

    /// 对单个账号执行一次预热请求,不抛错,所有结果都归并到 PrimeOutcome。
    async fn prime_one(&self, account: &Account, model: &str) -> PrimeOutcome {
        let started = Instant::now();

        // 取账号凭证(OAuth 会自动刷新,SetupToken 直接返回)。
        let token = match self.account_svc.resolve_upstream_token(account.id).await {
            Ok(t) => t,
            Err(e) => {
                return PrimeOutcome {
                    success: false,
                    error_message: truncate_chars(&format!("resolve token: {}", e), ERROR_SNIPPET_CHARS),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                    status: None,
                };
            }
        };

        // 构造最小请求体,其余字段交给 rewriter 补全,保证与真实 Claude Code 流量对齐:
        // - metadata.user_id 由 rewriter 注入 {device_id, account_uuid, session_id} JSON 字符串
        // - system prompt、tools、stream=true、去除 temperature 等由 rewriter 统一规范化
        let minimal_body = serde_json::json!({
            "model": model,
            "messages": [{
                "role": "user",
                "content": [{ "type": "text", "text": "hi" }]
            }],
            "max_tokens": 512
        });
        let minimal_bytes = match serde_json::to_vec(&minimal_body) {
            Ok(b) => b,
            Err(e) => {
                return PrimeOutcome {
                    success: false,
                    error_message: truncate_chars(&format!("serialize body: {}", e), ERROR_SNIPPET_CHARS),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                    status: None,
                };
            }
        };
        let rewritten_bytes = self.rewriter.rewrite_body(
            &minimal_bytes,
            "/v1/messages",
            account,
            ClientType::API,
        );
        // 去掉 rewriter 注入的内部 `_session_id` 标记,避免上游收到未知字段。
        let final_bytes = match serde_json::from_slice::<serde_json::Value>(&rewritten_bytes) {
            Ok(mut v) => {
                clean_session_id_from_body(&mut v);
                serde_json::to_vec(&v).unwrap_or(rewritten_bytes)
            }
            Err(_) => rewritten_bytes,
        };

        // Rewriter 的 header 改写需要一个 body 视图来抽 session_id / model,
        // 用最终 body 的 Value 参与 header 生成。
        let body_view: serde_json::Value =
            serde_json::from_slice(&final_bytes).unwrap_or(serde_json::Value::Null);
        let empty_in: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        let mut headers = self.rewriter.rewrite_headers(
            &empty_in,
            account,
            ClientType::API,
            model,
            &body_view,
        );
        headers.insert("authorization".into(), format!("Bearer {}", token));

        // 走定制 tlsfp 客户端,复用账号的 proxy_url。
        let client = crate::tlsfp::make_request_client(&account.proxy_url);
        let mut req = client.post(UPSTREAM_URL).body(final_bytes);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        req = req.header("Host", "api.anthropic.com");

        let resp = match tokio::time::timeout(PRIME_TTFB_TIMEOUT, req.send()).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                return PrimeOutcome {
                    success: false,
                    error_message: truncate_chars(&format!("request failed: {}", e), ERROR_SNIPPET_CHARS),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                    status: None,
                };
            }
            Err(_) => {
                return PrimeOutcome {
                    success: false,
                    error_message: "upstream TTFB timeout".into(),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                    status: None,
                };
            }
        };

        let status_code = resp.status().as_u16();
        let passive_usage = extract_passive_usage(resp.headers());

        if resp.status().is_success() {
            // 预热只需头部的 ratelimit 信息,不关心流式响应体;连接随 resp 离开作用域被释放。
            PrimeOutcome {
                success: true,
                error_message: String::new(),
                duration_ms: started.elapsed().as_millis() as i64,
                passive_usage,
                status: Some(status_code),
            }
        } else {
            // 按字符(不是字节)截断,避免多字节 UTF-8 切片 panic 把整个后台任务打掉。
            let snippet = match resp.text().await {
                Ok(t) if !t.is_empty() => truncate_chars(&t, ERROR_SNIPPET_CHARS),
                _ => String::new(),
            };
            PrimeOutcome {
                success: false,
                error_message: format!("http {}: {}", status_code, snippet),
                duration_ms: started.elapsed().as_millis() as i64,
                passive_usage,
                status: Some(status_code),
            }
        }
    }
}

/// 按字符截断字符串,避免直接字节切片在多字节字符上 panic。
fn truncate_chars(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    s.chars().take(max_chars).collect()
}
