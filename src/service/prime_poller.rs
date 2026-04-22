use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{NaiveDate, Timelike};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::model::account::AccountStatus;
use crate::service::account::AccountService;
use crate::service::gateway::extract_passive_usage;
use crate::service::rewriter::{ClientType, Rewriter};
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

/// Claude Code 真实客户端的系统提示词,与 sub2api `createTestPayload` 保持一致,
/// 以便预热请求在上游看来与正常 CC 流量无差异。
const CLAUDE_CODE_SYSTEM_PROMPT: &str = "You are Claude Code, Anthropic's official CLI for Claude.";

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
}

/// 峰值 5h 窗口预热服务。
///
/// 每天在配置的清晨时间点(默认 4:10 / 5:10 / 6:10 本地时间)对所有活跃账号逐一发送一次
/// 极小的 Haiku `/v1/messages` 请求,借此主动启动 Anthropic 侧的 5h 速率限制窗口,
/// 让窗口重置点落在下午用户高峰之前或之中。
///
/// 失败不重试,每次调用(含成功/失败)都会写入 `prime_logs` 表,供设置页展示。
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
    async fn run_round(&self, hour: u32, model: &str) {
        let accounts = match self.account_svc.list_accounts().await {
            Ok(list) => list,
            Err(e) => {
                warn!("prime poller: list accounts failed: {}", e);
                return;
            }
        };

        let targets: Vec<_> = accounts
            .into_iter()
            .filter(|a| a.status == AccountStatus::Active)
            .collect();

        debug!("prime poller: priming {} active accounts", targets.len());

        for account in targets {
            let triggered_at = chrono::Local::now().to_rfc3339();
            let outcome = self.prime_one(&account, model).await;

            // 落日志:info 级别打点,便于 grep;同时写入 DB 供前端展示。
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
                error_message: outcome.error_message,
                duration_ms: outcome.duration_ms,
            };
            if let Err(e) = self.log_store.insert(&entry).await {
                warn!("prime poller: write log failed for account {}: {}", account.id, e);
            }

            // 成功则合并响应头中的 ratelimit 数据,刷新账号窗口信息,供调度/展示使用。
            if let Some(usage) = outcome.passive_usage {
                if let Err(e) = self.account_svc.update_passive_usage(account.id, usage).await {
                    warn!(
                        "prime poller: update passive usage failed for account {}: {}",
                        account.id, e
                    );
                }
            }

            sleep(PER_ACCOUNT_GAP).await;
        }
    }

    /// 对单个账号执行一次预热请求,不抛错,所有结果都归并到 PrimeOutcome。
    async fn prime_one(&self, account: &crate::model::account::Account, model: &str) -> PrimeOutcome {
        let started = Instant::now();

        // 取账号凭证(OAuth 会自动刷新,SetupToken 直接返回)
        let token = match self.account_svc.resolve_upstream_token(account.id).await {
            Ok(t) => t,
            Err(e) => {
                return PrimeOutcome {
                    success: false,
                    error_message: format!("resolve token: {}", e),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                };
            }
        };

        // 构造与真实 Claude Code 流量一致的请求体。
        let body_value = build_prime_body(model);
        let body_bytes = match serde_json::to_vec(&body_value) {
            Ok(b) => b,
            Err(e) => {
                return PrimeOutcome {
                    success: false,
                    error_message: format!("serialize body: {}", e),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                };
            }
        };

        // 由 Rewriter 生成与 API 模式一致的 header 集合,
        // 保证 anthropic-beta/version、User-Agent、X-Stainless-* 等与真实流量完全一致。
        let empty_in: HashMap<String, String> = HashMap::new();
        let mut headers =
            self.rewriter
                .rewrite_headers(&empty_in, account, ClientType::API, model, &body_value);
        headers.insert("authorization".into(), format!("Bearer {}", token));

        // 走定制 tlsfp 客户端,复用账号的 proxy_url。
        let client = crate::tlsfp::make_request_client(&account.proxy_url);
        let mut req = client.post(UPSTREAM_URL).body(body_bytes);
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        req = req.header("Host", "api.anthropic.com");

        let resp = match tokio::time::timeout(PRIME_TTFB_TIMEOUT, req.send()).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                return PrimeOutcome {
                    success: false,
                    error_message: format!("request failed: {}", e),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                };
            }
            Err(_) => {
                return PrimeOutcome {
                    success: false,
                    error_message: "upstream TTFB timeout".into(),
                    duration_ms: started.elapsed().as_millis() as i64,
                    passive_usage: None,
                };
            }
        };

        let status = resp.status();
        let passive_usage = extract_passive_usage(resp.headers());

        if status.is_success() {
            PrimeOutcome {
                success: true,
                error_message: String::new(),
                duration_ms: started.elapsed().as_millis() as i64,
                passive_usage,
            }
        } else {
            // 非 2xx 视为失败,但仍把响应头的 usage 信息带回(429 时上游也会返回窗口状态)。
            let snippet = match resp.text().await {
                Ok(t) if !t.is_empty() => {
                    let max = 200.min(t.len());
                    t[..max].to_string()
                }
                _ => String::new(),
            };
            PrimeOutcome {
                success: false,
                error_message: format!("http {}: {}", status.as_u16(), snippet),
                duration_ms: started.elapsed().as_millis() as i64,
                passive_usage,
            }
        }
    }
}

/// 构造 Claude Code 测试请求体,形状对齐 sub2api 的 `createTestPayload`:
/// `/root/project/sub2api/backend/internal/service/account_test_service.go:126`。
///
/// 关键差异:
/// - `stream: false`:预热是内部轮询,非流式读取更简单;如后续被风控再切回 true。
/// - `max_tokens: 512`:足以触发计费并拿到响应头,远小于 sub2api 测试的 1024。
fn build_prime_body(model: &str) -> serde_json::Value {
    let user_id = uuid::Uuid::new_v4().to_string();
    serde_json::json!({
        "model": model,
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": "hi",
                "cache_control": {"type": "ephemeral"}
            }]
        }],
        "system": [{
            "type": "text",
            "text": CLAUDE_CODE_SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"}
        }],
        "metadata": {
            "user_id": user_id
        },
        "max_tokens": 512,
        "temperature": 1,
        "stream": false
    })
}
