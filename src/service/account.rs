use chrono::Utc;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

use crate::error::AppError;
use crate::model::account::{Account, AccountAuthType};
use crate::service::rewriter::ClientType;
use crate::store::account_store::AccountStore;
use crate::store::cache::CacheStore;

const STICKY_SESSION_TTL: Duration = Duration::from_secs(60 * 60);
const OAUTH_REFRESH_BUFFER_SECONDS: i64 = 5 * 60;
const OAUTH_LOCK_TTL: Duration = Duration::from_secs(30);
const OAUTH_WAIT_RETRY: Duration = Duration::from_millis(500);
const OAUTH_WAIT_ATTEMPTS: usize = 20;

/// 用量利用率达到此阈值即视为“撞墙”。
const USAGE_HIT_THRESHOLD: f64 = 97.0;
/// 撞墙之外的纯速率限制的短冷却时间。
const PURE_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(60);
/// 无法确定限流原因时的保守限流时长（与历史行为一致）。
const FALLBACK_QUARANTINE: Duration = Duration::from_secs(5 * 60 * 60);

/// 单个窗口的用量详情，用于前端展示计算过程。
pub struct WindowDetail {
    /// 原始利用率（0~100）。
    pub utilization: f64,
    /// 时间衰减因子（阶梯档位 0.4~1.0，距重置越近越小）。
    pub decay: f64,
    /// 有效用量 = utilization × 阶梯衰减。
    pub effective: f64,
}

/// 账号调度评分详情，用于前端展示。
pub struct AccountScoreInfo {
    /// 综合得分（越低越优先）。
    pub score: f64,
    /// 7 天窗口有效用量（utilization × 阶梯衰减）。
    pub eff_7d: f64,
    /// 5 小时窗口有效用量（utilization × 阶梯衰减）。
    pub eff_5h: f64,
    /// 7 天窗口计算详情。
    pub detail_7d: WindowDetail,
    /// 5 小时窗口计算详情。
    pub detail_5h: WindowDetail,
    /// 并发负载百分比（当前/最大 × 100）。
    pub concurrency_pct: f64,
    /// 当前使用的权重 (w7d, w5h, w_concurrency)。
    pub weights: (f64, f64, f64),
    /// 当前占用的并发槽位数。
    pub current_concurrency: i64,
    /// 当前等待队列中的请求数。
    pub queued: i64,
}

/// 评分权重默认值。
const DEFAULT_W7D: f64 = 0.5;
const DEFAULT_W5H: f64 = 0.3;
const DEFAULT_WCONC: f64 = 0.2;

/// 单账号并发槽位 FIFO 排队器。
///
/// 底层依赖 [`tokio::sync::Semaphore`]：其 `acquire` 文档保证按调用顺序授予 permit，
/// 因此同一账号上多个等待者天然按到达顺序获得槽位，不存在"抢锁"竞争。
///
/// # 两把信号量的职责
/// - `slots`：目标 permit 数 = `slot_max`（即账号当前的 `concurrency`），控制同时活跃的请求数。
/// - `queue_cap`：目标 permit 数 = `slot_max`，作为"等待位"上限。只有进入等待队列才会持有该 permit，
///   活跃阶段不占用。用它是为了在等待人数达上限时快速降级到其他账号（保持历史行为）。
///
/// # 运行时修改 concurrency
/// 不重建 queue 实例，而是通过 [`AccountQueue::adjust_capacity`] 原地调整两把信号量：
/// - 扩容：直接 `add_permits(delta)`，立即生效；
/// - 缩容：先 `forget_permits(delta)` 扣减可用 permit；若槽位被占满无法立即扣完，
///   spawn 一个后台吞 permit 任务循环读取 `slot_max`（共享 target），只要实际 cap 仍超过 target
///   就继续 acquire+forget，反之立即把 permit 归还并退出。
///   期间如果又被扩容，旧吞任务会在下次循环发现 cap 已达标直接退出——**不会遗留过期吞任务**。
///
/// # 单账号系统内最多请求数
/// `active ≤ slot_max` + `waiting ≤ slot_max` = `2 × slot_max`。缩容时短暂可能超过 `new`
/// 直到已有 waiter 耗尽（被 honor）——这是可接受的过渡态。
///
/// # 客户端中断 / 超时安全
/// `acquire` 返回的 `Future` 在被 drop 时：`queue_permit` 自动归还到 `queue_cap`；
/// 正在 `slots.acquire_owned()` 上挂起的 waiter 会从 semaphore 的 FIFO 链表移除；
/// `waiting` 原子计数通过 scopeguard 反向减回。无泄漏。
pub struct AccountQueue {
    /// 活跃槽位信号量。
    slots: Arc<Semaphore>,
    /// 等待位信号量（仅等待中持有，拿到槽位前归还）。
    queue_cap: Arc<Semaphore>,
    /// 槽位容量目标（== 账号当前 `concurrency`）。shrinker 循环读这个值作为裁决依据,
    /// 扩容时自动撤销旧的 shrink 意图。
    slot_max: Arc<AtomicI32>,
    /// `slots` 信号量当前实际总 permit 数（累计 add_permits - 累计 forget）。用于
    /// `adjust_capacity` 计算 delta 和 shrinker 判断是否继续吞 permit。
    slots_cap: Arc<AtomicI32>,
    /// `queue_cap` 信号量当前实际总 permit 数。同 `slots_cap`。
    qc_cap: Arc<AtomicI32>,
    /// 正在等待槽位的请求数（仅供 UI 展示用）。包 Arc 是为了 scopeguard 的 move 闭包能引用。
    waiting: Arc<AtomicI64>,
}

/// [`AccountQueue::acquire`] 的错误原因。
#[derive(Debug)]
pub enum QueueWaitError {
    /// 等待队列已满，调用方应降级到其他账号。
    QueueFull,
    /// 等待超时（超出 `timeout`），调用方应降级到其他账号。
    Timeout,
    /// 底层 semaphore 被 close(不应出现,视为内部错误)。
    Closed,
}

impl AccountQueue {
    /// 构造一个新的排队器，`slots` 与 `queue_cap` 容量均设置为 `concurrency`。
    fn new(concurrency: i32) -> Self {
        let cap = concurrency.max(1);
        Self {
            slots: Arc::new(Semaphore::new(cap as usize)),
            queue_cap: Arc::new(Semaphore::new(cap as usize)),
            slot_max: Arc::new(AtomicI32::new(cap)),
            slots_cap: Arc::new(AtomicI32::new(cap)),
            qc_cap: Arc::new(AtomicI32::new(cap)),
            waiting: Arc::new(AtomicI64::new(0)),
        }
    }

    /// 原地调整槽位容量（admin 修改 concurrency 时调用），不替换 queue 实例,已占用 permit 不受影响。
    ///
    /// # 关键设计
    /// `slot_max` 是共享的 target，所有后台 shrinker 任务每次循环都读取最新值。因此：
    /// - 扩容 10→1→10：第二次调用 store(10)，旧的 shrinker（从 10→1 留下）下次循环看到
    ///   `cap(10) <= target(10)` 后把 permit 归还并退出，不会继续悄悄吞；
    /// - 缩容 10→5→1：两次调整合并到同一个 target=1，新旧 shrinker 共同工作至 `cap=1`。
    fn adjust_capacity(&self, new: i32) {
        let new = new.max(1);
        self.slot_max.store(new, Ordering::Release);
        Self::adjust_semaphore(&self.slots, &self.slots_cap, &self.slot_max);
        Self::adjust_semaphore(&self.queue_cap, &self.qc_cap, &self.slot_max);
    }

    /// 把指定 semaphore 的实际容量向 `slot_max` 看齐。
    /// `cap` 跟踪该 semaphore 的累计 add_permits - 累计 forget 数。
    fn adjust_semaphore(
        sem: &Arc<Semaphore>,
        cap: &Arc<AtomicI32>,
        slot_max: &Arc<AtomicI32>,
    ) {
        let current = cap.load(Ordering::Acquire);
        let target = slot_max.load(Ordering::Acquire);
        if target == current {
            return;
        }
        if target > current {
            let delta = (target - current) as usize;
            sem.add_permits(delta);
            cap.fetch_add(delta as i32, Ordering::AcqRel);
            return;
        }
        // 缩容：先扣减可用 permit
        let needed = (current - target) as usize;
        let took = sem.forget_permits(needed);
        if took > 0 {
            cap.fetch_sub(took as i32, Ordering::AcqRel);
        }
        if took >= needed {
            return;
        }
        // 剩余部分 spawn 后台任务延迟吞；任务每次循环读 slot_max 判断是否继续。
        let sem_c = sem.clone();
        let cap_c = cap.clone();
        let target_c = slot_max.clone();
        tokio::spawn(async move {
            loop {
                // 每轮先读共享 target：如果此间有扩容把 target 抬到 ≥ cap，直接退出
                let cur = cap_c.load(Ordering::Acquire);
                let tgt = target_c.load(Ordering::Acquire);
                if cur <= tgt {
                    return;
                }
                // 拿一个 permit（可能等待）
                let permit = match sem_c.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return, // semaphore 被 close
                };
                // 原子决定：若 cap 仍 > slot_max 则吞掉，否则归还 permit 并退出
                let decision = cap_c.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |c| {
                        if c > target_c.load(Ordering::Acquire) {
                            Some(c - 1)
                        } else {
                            None
                        }
                    },
                );
                match decision {
                    Ok(_) => permit.forget(), // cap 已 -1,permit 不归还
                    Err(_) => {
                        drop(permit); // 不应再缩,归还
                        return;
                    }
                }
            }
        });
    }

    /// 获取一个活跃槽位 permit，超时或队列满时返回对应错误。
    ///
    /// 成功返回的 [`OwnedSemaphorePermit`] 由调用方持有至请求结束——drop 时自动归还槽位。
    ///
    /// # 流程
    /// 1. `queue_cap.try_acquire_owned()`：拿不到等待位 → `QueueFull`（队列上限，直接降级）；
    /// 2. `waiting += 1`（scopeguard 保证退出路径反向减）；
    /// 3. `tokio::time::timeout(..., slots.acquire_owned())` 按 FIFO 等待；
    ///    即使当前有空闲 permit，若已有 waiter 排在前面，也不会被"插队"。
    /// 4. 拿到槽位后 drop 等待位 permit，让后续请求有空位进队。
    ///
    /// # 为什么不走 try_acquire_owned 快速路径
    /// tokio 对 `Semaphore::acquire` 保证公平性，但 `try_acquire` 是**非公平**的
    /// （官方源码：直接对底层计数 CAS，不走 waker 队列）。一旦走快速路径，新请求就可能
    /// 插队到已在等的老请求前面，破坏 FIFO。故此处统一走 `acquire_owned`——无人等待时
    /// 它自己就会立即 ready，不会有额外开销。
    pub async fn acquire(
        &self,
        timeout: Duration,
    ) -> Result<OwnedSemaphorePermit, QueueWaitError> {
        // 1. 进等待区：拿不到等待位说明队列满，降级。
        //    queue_cap 用 try_acquire 是 OK 的：它仅作上限计数，无用户 waiter 排队。
        let queue_permit = self
            .queue_cap
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueueWaitError::QueueFull)?;

        // 2. 等待计数（仅展示用）+ 退出保护（包括 Future 被 drop 的路径）
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let waiting = self.waiting.clone();
        let _w_guard = scopeguard::guard((), move |_| {
            waiting.fetch_sub(1, Ordering::Relaxed);
        });

        // 3. FIFO 获取槽位（tokio Semaphore 对 acquire 保证公平，不会插队）
        let slot_permit =
            match tokio::time::timeout(timeout, self.slots.clone().acquire_owned()).await {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => return Err(QueueWaitError::Closed),
                Err(_) => return Err(QueueWaitError::Timeout),
            };

        // 4. 已进入活跃区，显式归还等待位，让下一个 waiter 有空间排队
        drop(queue_permit);
        Ok(slot_permit)
    }

    /// 当前活跃槽位占用数 = `slots` 当前总 permit - 可用 permit。
    /// 用 `slots_cap`（信号量实际容量）而非 `slot_max`（目标）——缩容过渡期两者可能暂时不等。
    pub fn active_count(&self) -> i64 {
        let cap = self.slots_cap.load(Ordering::Relaxed) as i64;
        (cap - self.slots.available_permits() as i64).max(0)
    }

    /// 当前等待槽位的请求数(仅用于 UI 展示)。
    pub fn waiting_count(&self) -> i64 {
        self.waiting.load(Ordering::Relaxed)
    }
}

pub struct AccountService {
    store: Arc<AccountStore>,
    cache: Arc<dyn CacheStore>,
    /// 评分权重缓存 (w7d, w5h, w_concurrency)，更新设置后刷新。
    score_weights: RwLock<(f64, f64, f64)>,
    settings_store: Arc<crate::store::settings_store::SettingsStore>,
    /// 账号级 FIFO 排队器缓存：account_id → queue。
    ///
    /// 懒创建(首次调用 `get_or_create_queue` 时建);admin 修改账号 concurrency 后
    /// 由 `get_or_create_queue` → `AccountQueue::adjust_capacity` 原地调整信号量容量,
    /// 不替换 queue 实例,保证已占用的 permit 不丢失。
    queues: RwLock<HashMap<i64, Arc<AccountQueue>>>,
}

impl AccountService {
    pub fn new(
        store: Arc<AccountStore>,
        cache: Arc<dyn CacheStore>,
        settings_store: Arc<crate::store::settings_store::SettingsStore>,
    ) -> Self {
        Self {
            store,
            cache,
            score_weights: RwLock::new((DEFAULT_W7D, DEFAULT_W5H, DEFAULT_WCONC)),
            settings_store,
            queues: RwLock::new(HashMap::new()),
        }
    }

    /// 从数据库加载评分权重到内存缓存，启动时和更新设置后调用。
    pub async fn reload_score_weights(&self) {
        if let Ok(settings) = self.settings_store.get_all().await {
            let w7d = settings.get("score_weight_7d")
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(DEFAULT_W7D);
            let w5h = settings.get("score_weight_5h")
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(DEFAULT_W5H);
            let wconc = settings.get("score_weight_concurrency")
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(DEFAULT_WCONC);
            *self.score_weights.write().await = (w7d, w5h, wconc);
        }
    }

    /// 获取当前评分权重。
    async fn get_weights(&self) -> (f64, f64, f64) {
        *self.score_weights.read().await
    }

    /// 创建新账号并自动生成身份信息。
    pub async fn create_account(&self, a: &mut Account) -> Result<(), AppError> {
        let (device_id, env, prompt, process) =
            crate::model::identity::generate_canonical_identity();
        a.device_id = device_id;
        a.canonical_env = env;
        a.canonical_prompt = prompt;
        a.canonical_process = process;

        if a.status == crate::model::account::AccountStatus::Active && a.status.to_string() == "active" {
            // default already active
        }
        if a.concurrency == 0 {
            a.concurrency = 3;
        }
        if a.priority == 0 {
            a.priority = 50;
        }
        if a.billing_mode == crate::model::account::BillingMode::Strip
            && a.billing_mode.to_string() == "strip"
        {
            // default already strip
        }

        normalize_account_auth(a)?;

        self.store.create(a).await
    }

    pub async fn update_account(&self, a: &Account) -> Result<(), AppError> {
        let mut normalized = a.clone();
        normalize_account_auth(&mut normalized)?;
        self.store.update(&normalized).await
    }

    pub async fn delete_account(&self, id: i64) -> Result<(), AppError> {
        self.store.delete(id).await
    }

    pub async fn get_account(&self, id: i64) -> Result<Account, AppError> {
        self.store.get_by_id(id).await
    }

    pub async fn list_accounts(&self) -> Result<Vec<Account>, AppError> {
        self.store.list().await
    }

    pub async fn list_accounts_paged(&self, page: i64, page_size: i64) -> Result<(Vec<Account>, i64), AppError> {
        let total = self.store.count().await?;
        let accounts = self.store.list_paged(page, page_size).await?;
        Ok((accounts, total))
    }

    /// 使用粘性会话为请求选择账号。
    /// `exclude_ids` 为令牌的不可用账号，`allowed_ids` 为令牌的可用账号（空表示不限制）。
    pub async fn select_account(
        &self,
        session_hash: &str,
        exclude_ids: &[i64],
        allowed_ids: &[i64],
    ) -> Result<Account, AppError> {
        // 检查粘性会话
        if !session_hash.is_empty() {
            if let Ok(Some(account_id)) = self.cache.get_session_account_id(session_hash).await {
                if account_id > 0 {
                    if let Ok(account) = self.store.get_by_id(account_id).await {
                        let id_allowed = allowed_ids.is_empty() || allowed_ids.contains(&account_id);
                        if account.is_schedulable()
                            && !exclude_ids.contains(&account_id)
                            && id_allowed
                        {
                            // 粘性命中：刷新 TTL，保持活跃会话不过期
                            let _ = self.cache.set_session_account_id(
                                session_hash, account_id, STICKY_SESSION_TTL,
                            ).await;
                            return Ok(account);
                        }
                    }
                    // 过期绑定，删除
                    let _ = self.cache.delete_session(session_hash).await;
                }
            }
        }

        // 获取可调度账号
        let accounts = self.store.list_schedulable().await?;

        // 过滤：排除项 + 可用账号限制
        let candidates: Vec<Account> = accounts
            .into_iter()
            .filter(|a| {
                !exclude_ids.contains(&a.id)
                    && (allowed_ids.is_empty() || allowed_ids.contains(&a.id))
            })
            .collect();

        if candidates.is_empty() {
            return Err(AppError::ServiceUnavailable(
                "no available accounts".into(),
            ));
        }

        // 按优先级分组，同优先级内按综合评分选择（5h 用量 + 并发负载）
        let selected = self.select_by_score(&candidates).await;

        // 绑定粘性会话
        if !session_hash.is_empty() {
            let _ = self
                .cache
                .set_session_account_id(session_hash, selected.id, STICKY_SESSION_TTL)
                .await;
        }

        Ok(selected)
    }

    /// 获取或懒创建账号的 FIFO 排队器。
    ///
    /// 当已缓存 queue 的 `slot_max` 与当前 `concurrency` 不一致(admin 在管理页面改过),
    /// 调用 [`AccountQueue::adjust_capacity`] **原地**调整信号量容量,不替换 queue 实例,
    /// 确保旧的已占用 permit 不丢失 —— 避免瞬间超发。
    pub async fn get_or_create_queue(
        &self,
        account_id: i64,
        concurrency: i32,
    ) -> Arc<AccountQueue> {
        let concurrency = concurrency.max(1);
        // 读锁快路径:容量匹配直接返回
        {
            let map = self.queues.read().await;
            if let Some(q) = map.get(&account_id) {
                if q.slot_max.load(Ordering::Relaxed) == concurrency {
                    return q.clone();
                }
            }
        }
        // 写锁:二次检查后原地调整或创建新实例
        let mut map = self.queues.write().await;
        if let Some(q) = map.get(&account_id) {
            if q.slot_max.load(Ordering::Relaxed) != concurrency {
                q.adjust_capacity(concurrency);
            }
            return q.clone();
        }
        let q = Arc::new(AccountQueue::new(concurrency));
        map.insert(account_id, q.clone());
        q
    }

    /// 从 Anthropic API 获取账号用量并缓存到数据库。
    /// 仅支持 OAuth 账号，SetupToken 账号无法查询用量。
    pub async fn refresh_usage(&self, id: i64) -> Result<serde_json::Value, AppError> {
        let account = self.store.get_by_id(id).await?;
        if account.auth_type != crate::model::account::AccountAuthType::Oauth {
            return Err(AppError::BadRequest(
                "usage query is only supported for OAuth accounts, SetupToken accounts cannot query usage via this endpoint".into(),
            ));
        }
        let token = self.resolve_oauth_access_token(&account).await?;
        let usage = crate::service::oauth::fetch_usage(&token, &account.proxy_url).await?;
        let usage_str = serde_json::to_string(&usage).unwrap_or_else(|_| "{}".into());
        self.store.update_usage(id, &usage_str).await?;
        Ok(usage)
    }

    /// 从上游响应头被动采集的用量数据合并到数据库。
    /// 不发起任何 API 请求。与已有 usage_data 做 merge，不会丢失响应头中未包含的窗口（如 seven_day_sonnet）。
    pub async fn update_passive_usage(&self, id: i64, partial: serde_json::Value) -> Result<(), AppError> {
        let account = self.store.get_by_id(id).await?;
        let mut existing = if account.usage_data.is_object() {
            account.usage_data
        } else {
            serde_json::json!({})
        };
        // 将被动采集的窗口逐一合并到已有数据
        if let (Some(target), Some(src)) = (existing.as_object_mut(), partial.as_object()) {
            for (k, v) in src {
                target.insert(k.clone(), v.clone());
            }
        }
        let merged_str = serde_json::to_string(&existing).unwrap_or_else(|_| "{}".into());
        self.store.update_usage(id, &merged_str).await
    }

    pub async fn resolve_upstream_token(&self, id: i64) -> Result<String, AppError> {
        let account = self.store.get_by_id(id).await?;
        match account.auth_type {
            AccountAuthType::SetupToken => {
                if account.setup_token.is_empty() {
                    return Err(AppError::ServiceUnavailable(
                        "setup token is empty".into(),
                    ));
                }
                Ok(account.setup_token)
            }
            AccountAuthType::Oauth => self.resolve_oauth_access_token(&account).await,
        }
    }

    async fn resolve_oauth_access_token(&self, account: &Account) -> Result<String, AppError> {
        if account.has_valid_oauth_access_token(OAUTH_REFRESH_BUFFER_SECONDS) {
            return Ok(account.access_token.clone());
        }
        if account.refresh_token.is_empty() {
            let _ = self
                .store
                .update_auth_error(account.id, "missing refresh token")
                .await;
            return Err(AppError::ServiceUnavailable(
                "oauth refresh token is empty".into(),
            ));
        }

        let lock_key = format!("oauth:refresh:account:{}", account.id);
        let lock_owner = Uuid::new_v4().to_string();
        let acquired = self
            .cache
            .acquire_lock(&lock_key, &lock_owner, OAUTH_LOCK_TTL)
            .await?;

        if acquired {
            let result = self.refresh_oauth_access_token(account.id).await;
            self.cache.release_lock(&lock_key, &lock_owner).await;
            return result;
        }

        for _ in 0..OAUTH_WAIT_ATTEMPTS {
            sleep(OAUTH_WAIT_RETRY).await;
            let latest = self.store.get_by_id(account.id).await?;
            if latest.has_valid_oauth_access_token(OAUTH_REFRESH_BUFFER_SECONDS) {
                return Ok(latest.access_token);
            }
        }

        Err(AppError::ServiceUnavailable(
            "oauth token refresh timeout".into(),
        ))
    }

    async fn refresh_oauth_access_token(&self, id: i64) -> Result<String, AppError> {
        let latest = self.store.get_by_id(id).await?;
        if latest.has_valid_oauth_access_token(OAUTH_REFRESH_BUFFER_SECONDS) {
            return Ok(latest.access_token);
        }
        if latest.refresh_token.is_empty() {
            let _ = self
                .store
                .update_auth_error(id, "missing refresh token")
                .await;
            return Err(AppError::ServiceUnavailable(
                "oauth refresh token is empty".into(),
            ));
        }

        let fallback_access_token = latest.access_token.clone();
        let fallback_is_still_valid = latest
            .expires_at
            .map(|expires_at| expires_at > Utc::now())
            .unwrap_or(false);

        match crate::service::oauth::refresh_oauth_token(&latest.refresh_token, &latest.proxy_url).await {
            Ok(tokens) => {
                self.store
                    .update_oauth_tokens(
                        id,
                        &tokens.access_token,
                        &tokens.refresh_token,
                        tokens.expires_at,
                    )
                    .await?;
                Ok(tokens.access_token)
            }
            Err(err) => {
                let msg = err.to_string();
                let _ = self.store.update_auth_error(id, &msg).await;
                if fallback_is_still_valid && !fallback_access_token.is_empty() {
                    warn!(
                        "oauth refresh failed for account {}, using current access token until expiry: {}",
                        id, msg
                    );
                    return Ok(fallback_access_token);
                }
                Err(AppError::ServiceUnavailable(format!(
                    "oauth refresh failed: {}",
                    msg
                )))
            }
        }
    }

    pub async fn set_rate_limit(
        &self,
        id: i64,
        reset_at: chrono::DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.store.set_rate_limit(id, reset_at).await
    }

    pub async fn disable_account(
        &self,
        id: i64,
        status: crate::model::account::AccountStatus,
        reason: &str,
        rate_limit_reset_at: Option<chrono::DateTime<Utc>>,
    ) -> Result<(), AppError> {
        self.store
            .disable_account(id, status, reason, rate_limit_reset_at)
            .await
    }

    pub async fn enable_account(&self, id: i64) -> Result<(), AppError> {
        self.store.enable_account(id).await
    }

    /// 处理上游返回 429 的情况：根据账号类型和用量数据决定限流时长和原因。
    ///
    /// - **SetupToken**：无法查询用量接口，保守限流 5h（与历史行为一致）。
    /// - **OAuth**：立即拉取 `/api/oauth/usage` 判断是否撞墙：
    ///   - 命中 7 天墙 → 限流到周重置时间
    ///   - 命中 5 小时墙 → 限流到 5h 重置时间
    ///   - 都没撞墙 → 纯速率限制，短冷却 1 分钟
    ///   - usage 接口调用失败 → 回退到 5h 保守限流
    ///
    /// Sonnet 7 天墙暂不纳入判断（上游可能只对 Sonnet 请求返回 429，不影响其他模型）。
    pub async fn handle_rate_limit(&self, account: &Account) -> Result<(), AppError> {
        let (reason, reset_at) = self.determine_rate_limit_window(account).await;
        warn!(
            "account {} rate limited ({}) until {}",
            account.id,
            reason,
            reset_at.to_rfc3339()
        );
        self.store
            .disable_account(
                account.id,
                crate::model::account::AccountStatus::Active,
                reason,
                Some(reset_at),
            )
            .await
    }

    async fn determine_rate_limit_window(
        &self,
        account: &Account,
    ) -> (&'static str, chrono::DateTime<Utc>) {
        let now = Utc::now();
        let fallback = || {
            (
                "429 速率限制",
                now + chrono::Duration::from_std(FALLBACK_QUARANTINE).unwrap(),
            )
        };

        if account.auth_type != AccountAuthType::Oauth {
            return fallback();
        }

        let usage = match self.refresh_usage(account.id).await {
            Ok(u) => u,
            Err(e) => {
                warn!(
                    "failed to fetch usage for rate-limited oauth account {}: {}",
                    account.id, e
                );
                return fallback();
            }
        };

        match classify_rate_limit(&usage, USAGE_HIT_THRESHOLD) {
            Some(RateLimitWindow::SevenDay(reset_at)) => ("周限额已满", reset_at),
            Some(RateLimitWindow::FiveHour(reset_at)) => ("5 小时限额已满", reset_at),
            None => (
                "速率限制（未达用量墙）",
                now + chrono::Duration::from_std(PURE_RATE_LIMIT_COOLDOWN).unwrap(),
            ),
        }
    }

    /// 综合评分选择账号：同优先级内按 7d/5h 有效用量和并发负载加权评分，分数越低越优先。
    ///
    /// 评分公式：`score = eff_7d * w7d + eff_5h * w5h + concurrency_load_pct * wconc`（权重可在设置页面配置）
    /// - `eff_Xd = utilization × 阶梯衰减`，快重置的窗口衰减更大（分 3 档：1.0/0.8/0.6）
    /// - 无 resets_at 数据时衰减因子为 1.0（最保守）
    /// - 并发已满的账号优先排除，仅在所有账号都满时才参与评分
    /// - 得分相同时随机选择
    async fn select_by_score(&self, accounts: &[Account]) -> Account {
        if accounts.len() == 1 {
            return accounts[0].clone();
        }

        // 找到最高优先级（最小数值）
        let best_priority = accounts.iter().map(|a| a.priority).min().unwrap_or(50);
        let best: Vec<&Account> = accounts
            .iter()
            .filter(|a| a.priority == best_priority)
            .collect();

        if best.len() == 1 {
            return best[0].clone();
        }

        let now = Utc::now();
        let (w7d, w5h, wconc) = self.get_weights().await;

        // 计算每个候选的综合得分，同时记录并发是否已满
        let mut scored: Vec<(&Account, f64, bool)> = Vec::with_capacity(best.len());
        for acc in &best {
            let eff_5h = effective_utilization_detail(acc, "five_hour", 5.0 * 3600.0, now).effective;
            let eff_7d = effective_utilization_detail(acc, "seven_day", 7.0 * 24.0 * 3600.0, now).effective;
            // 从 FIFO 排队器读取实时活跃数,替代原 cache.get_slot_count("concurrency:account:{}") 查询
            let queue = self.get_or_create_queue(acc.id, acc.concurrency).await;
            let current = queue.active_count();
            let full = acc.concurrency > 0 && current >= acc.concurrency as i64;
            let conc_pct = if acc.concurrency > 0 {
                (current as f64) / (acc.concurrency as f64) * 100.0
            } else {
                0.0
            };
            let score = eff_7d * w7d + eff_5h * w5h + conc_pct * wconc;
            scored.push((acc, score, full));
        }

        // 优先从未满的账号中选择；全都满了才回退到全部候选
        let pool: Vec<(&Account, f64)> = {
            let not_full: Vec<_> = scored.iter().filter(|(_, _, full)| !full).map(|(a, s, _)| (*a, *s)).collect();
            if not_full.is_empty() {
                scored.iter().map(|(a, s, _)| (*a, *s)).collect()
            } else {
                not_full
            }
        };

        // 找到最低分
        let min_score = pool.iter().map(|(_, s)| *s).fold(f64::MAX, f64::min);

        // 收集得分最低的候选（容差 0.01 处理浮点误差）
        let winners: Vec<&Account> = pool
            .iter()
            .filter(|(_, s)| (*s - min_score).abs() < 0.01)
            .map(|(a, _)| *a)
            .collect();

        // 同分随机选择
        let idx = rand::thread_rng().gen_range(0..winners.len());
        winners[idx].clone()
    }

    /// 计算单个账号的调度评分及实时状态（供 API 展示用）。
    pub async fn get_account_score_info(&self, account: &Account) -> AccountScoreInfo {
        let now = Utc::now();
        let (w7d, w5h, wconc) = self.get_weights().await;
        let d5h = effective_utilization_detail(account, "five_hour", 5.0 * 3600.0, now);
        let d7d = effective_utilization_detail(account, "seven_day", 7.0 * 24.0 * 3600.0, now);

        // 从 FIFO 排队器读取实时活跃/等待数(替代原 cache.get_slot_count 查询)
        let queue = self.get_or_create_queue(account.id, account.concurrency).await;
        let current_concurrency = queue.active_count();
        let concurrency_pct = if account.concurrency > 0 {
            (current_concurrency as f64) / (account.concurrency as f64) * 100.0
        } else {
            0.0
        };

        let queued = queue.waiting_count();

        let score = d7d.effective * w7d + d5h.effective * w5h + concurrency_pct * wconc;

        AccountScoreInfo {
            score,
            eff_7d: d7d.effective,
            eff_5h: d5h.effective,
            detail_7d: d7d,
            detail_5h: d5h,
            concurrency_pct,
            weights: (w7d, w5h, wconc),
            current_concurrency,
            queued,
        }
    }
}

/// 计算指定窗口的有效用量详情：utilization × 阶梯衰减因子。
/// 使用固定阶梯而非线性比例，避免时间衰减过度影响评分：
///   剩余比例 > 50% → 1.0 | 20%~50% → 0.8 | ≤ 20% → 0.6
/// 无 resets_at 或无用量数据时衰减因子为 1.0（最保守）。
/// 当 resets_at 已过期时，说明窗口已重置，直接视为用量清零。
fn effective_utilization_detail(
    account: &Account,
    window: &str,
    max_window_secs: f64,
    now: chrono::DateTime<Utc>,
) -> WindowDetail {
    let util = account
        .usage_data
        .get(window)
        .and_then(|w| w.get("utilization"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    if util == 0.0 {
        return WindowDetail { utilization: 0.0, decay: 1.0, effective: 0.0 };
    }

    // 解析 resets_at，计算剩余时间占窗口总时长的比例，再映射到阶梯档位
    let decay = account
        .usage_data
        .get(window)
        .and_then(|w| w.get("resets_at"))
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|reset| {
            let remaining = (reset.with_timezone(&Utc) - now).num_seconds().max(0) as f64;
            if remaining == 0.0 {
                // resets_at 已过期 → 窗口已重置，用量归零
                return 0.0;
            }
            let ratio = (remaining / max_window_secs).clamp(0.0, 1.0);
            // 固定阶梯：将连续比例映射为离散档位，防止线性衰减过度压低高用量账号
            tiered_decay(ratio)
        })
        .unwrap_or(1.0); // 无数据按最保守处理

    // resets_at 已过期 → 窗口已重置，用量归零
    if decay == 0.0 {
        return WindowDetail { utilization: 0.0, decay: 1.0, effective: 0.0 };
    }

    WindowDetail { utilization: util, decay, effective: util * decay }
}

/// 将剩余时间比例映射为阶梯衰减系数。
/// 比例越低（越接近重置），衰减系数越小，有效用量越低（越优先选中）。
/// 仅分 3 档，最低 0.6，避免时间因素过度影响评分。
fn tiered_decay(ratio: f64) -> f64 {
    match () {
        _ if ratio > 0.5 => 1.0,
        _ if ratio > 0.2 => 0.8,
        _ => 0.6,
    }
}
enum RateLimitWindow {
    /// 7 天窗口命中，携带其 resets_at。
    SevenDay(chrono::DateTime<Utc>),
    /// 5 小时窗口命中，携带其 resets_at。
    FiveHour(chrono::DateTime<Utc>),
}

/// 根据 usage_data JSON 判断哪个窗口撞墙。
/// 优先检查 7 天窗口（同时命中时 7 天 reset 更晚，限流更久）。
/// Sonnet 7 天窗口暂不纳入判断。
fn classify_rate_limit(
    usage: &serde_json::Value,
    threshold: f64,
) -> Option<RateLimitWindow> {
    if let Some(reset_at) = check_usage_window(usage, "seven_day", threshold) {
        return Some(RateLimitWindow::SevenDay(reset_at));
    }
    if let Some(reset_at) = check_usage_window(usage, "five_hour", threshold) {
        return Some(RateLimitWindow::FiveHour(reset_at));
    }
    None
}

/// 检查单个窗口是否达到撞墙阈值，返回其 resets_at（若命中且在未来）。
fn check_usage_window(
    usage: &serde_json::Value,
    key: &str,
    threshold: f64,
) -> Option<chrono::DateTime<Utc>> {
    let window = usage.get(key)?;
    let util = window.get("utilization")?.as_f64()?;
    if util < threshold {
        return None;
    }
    let resets_at_str = window.get("resets_at")?.as_str()?;
    let dt = chrono::DateTime::parse_from_rfc3339(resets_at_str)
        .ok()?
        .with_timezone(&Utc);
    if dt <= Utc::now() {
        return None;
    }
    Some(dt)
}

fn normalize_account_auth(account: &mut Account) -> Result<(), AppError> {
    match account.auth_type {
        AccountAuthType::SetupToken => {
            if account.setup_token.trim().is_empty() {
                return Err(AppError::BadRequest("setup_token is required".into()));
            }
            account.access_token.clear();
            account.refresh_token.clear();
            account.expires_at = None;
            account.oauth_refreshed_at = None;
            account.auth_error.clear();
        }
        AccountAuthType::Oauth => {
            if account.refresh_token.trim().is_empty() {
                return Err(AppError::BadRequest("refresh_token is required".into()));
            }
            account.setup_token.clear();
            account.auth_error.clear();
            if account.access_token.trim().is_empty() {
                account.access_token.clear();
                account.expires_at = None;
            }
        }
    }
    Ok(())
}

/// 根据客户端类型创建会话哈希。
/// CC 客户端：使用 metadata.user_id 中的 session_id。
/// API 客户端：使用 sha256(UA + 系统提示词/首条消息 + 小时窗口)。
pub fn generate_session_hash(
    user_agent: &str,
    body: &serde_json::Value,
    client_type: ClientType,
) -> String {
    if client_type == ClientType::ClaudeCode {
        if let Some(metadata) = body.get("metadata").and_then(|m| m.as_object()) {
            if let Some(user_id_str) = metadata.get("user_id").and_then(|u| u.as_str()) {
                // JSON 格式
                if let Ok(uid) = serde_json::from_str::<serde_json::Value>(user_id_str) {
                    if let Some(sid) = uid.get("session_id").and_then(|s| s.as_str()) {
                        if !sid.is_empty() {
                            return sid.to_string();
                        }
                    }
                }
                // 旧格式
                if let Some(idx) = user_id_str.rfind("_session_") {
                    return user_id_str[idx + 9..].to_string();
                }
            }
        }
    }

    // API 模式：UA + 系统提示词/首条消息 + 小时窗口
    let mut content = String::new();

    // Try system prompt first
    match body.get("system") {
        Some(serde_json::Value::String(sys)) => {
            content = sys.clone();
        }
        Some(serde_json::Value::Array(arr)) => {
            for item in arr {
                if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                    content = text.to_string();
                    break;
                }
            }
        }
        _ => {}
    }

    // 回退到首条消息
    if content.is_empty() {
        if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
            if let Some(msg) = messages.first().and_then(|m| m.as_object()) {
                match msg.get("content") {
                    Some(serde_json::Value::String(c)) => {
                        content = c.clone();
                    }
                    Some(serde_json::Value::Array(arr)) => {
                        for item in arr {
                            if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                                content = text.to_string();
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let hour_window = Utc::now().format("%Y-%m-%dT%H").to_string();
    let raw = format!("{}|{}|{}", user_agent, content, hour_window);
    let hash = Sha256::digest(raw.as_bytes());
    hex::encode(&hash[..16])
}


#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use serde_json::json;

    /// 生成一个相对当前时间指定偏移的 RFC3339 字符串。
    fn rfc3339_at(offset: ChronoDuration) -> String {
        (Utc::now() + offset).to_rfc3339()
    }

    fn make_window(util: serde_json::Value, resets_at: &str) -> serde_json::Value {
        json!({ "utilization": util, "resets_at": resets_at })
    }

    // ---- check_usage_window ----

    #[test]
    fn check_window_below_threshold_returns_none() {
        let future = rfc3339_at(ChronoDuration::hours(1));
        let usage = json!({ "five_hour": make_window(json!(96.9), &future) });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_at_threshold_returns_some() {
        let future = rfc3339_at(ChronoDuration::hours(1));
        let usage = json!({ "five_hour": make_window(json!(97.0), &future) });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_some());
    }

    #[test]
    fn check_window_above_threshold_returns_some() {
        let future = rfc3339_at(ChronoDuration::hours(1));
        let usage = json!({ "five_hour": make_window(json!(99.9), &future) });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_some());
    }

    #[test]
    fn check_window_integer_utilization_works() {
        let future = rfc3339_at(ChronoDuration::hours(1));
        let usage = json!({ "five_hour": make_window(json!(100), &future) });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_some());
    }

    #[test]
    fn check_window_missing_key_returns_none() {
        let usage = json!({});
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_missing_utilization_returns_none() {
        let future = rfc3339_at(ChronoDuration::hours(1));
        let usage = json!({ "five_hour": { "resets_at": future } });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_missing_resets_at_returns_none() {
        let usage = json!({ "five_hour": { "utilization": 100 } });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_invalid_rfc3339_returns_none() {
        let usage = json!({
            "five_hour": { "utilization": 100, "resets_at": "not-a-date" }
        });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_past_time_returns_none() {
        let past = rfc3339_at(ChronoDuration::hours(-1));
        let usage = json!({ "five_hour": make_window(json!(100), &past) });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_null_utilization_returns_none() {
        let future = rfc3339_at(ChronoDuration::hours(1));
        let usage = json!({
            "five_hour": { "utilization": null, "resets_at": future }
        });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_string_utilization_returns_none() {
        let future = rfc3339_at(ChronoDuration::hours(1));
        let usage = json!({
            "five_hour": { "utilization": "100", "resets_at": future }
        });
        assert!(check_usage_window(&usage, "five_hour", 97.0).is_none());
    }

    #[test]
    fn check_window_returns_parsed_reset_at() {
        let future = rfc3339_at(ChronoDuration::hours(3));
        let usage = json!({ "five_hour": make_window(json!(100), &future) });
        let result = check_usage_window(&usage, "five_hour", 97.0).unwrap();
        let expected = chrono::DateTime::parse_from_rfc3339(&future)
            .unwrap()
            .with_timezone(&Utc);
        // 允许纳秒级精度差
        assert_eq!(result.timestamp(), expected.timestamp());
    }

    // ---- classify_rate_limit ----

    #[test]
    fn classify_empty_usage_returns_none() {
        let usage = json!({});
        assert!(classify_rate_limit(&usage, 97.0).is_none());
    }

    #[test]
    fn classify_only_five_hour_hit_returns_five_hour() {
        let future = rfc3339_at(ChronoDuration::hours(2));
        let usage = json!({
            "five_hour": make_window(json!(100), &future),
            "seven_day": make_window(json!(50), &rfc3339_at(ChronoDuration::days(5))),
        });
        match classify_rate_limit(&usage, 97.0) {
            Some(RateLimitWindow::FiveHour(_)) => {}
            other => panic!("expected FiveHour, got {:?}", match other {
                Some(RateLimitWindow::SevenDay(_)) => "SevenDay",
                Some(RateLimitWindow::FiveHour(_)) => "FiveHour",
                None => "None",
            }),
        }
    }

    #[test]
    fn classify_only_seven_day_hit_returns_seven_day() {
        let usage = json!({
            "five_hour": make_window(json!(50), &rfc3339_at(ChronoDuration::hours(2))),
            "seven_day": make_window(json!(99), &rfc3339_at(ChronoDuration::days(5))),
        });
        assert!(matches!(
            classify_rate_limit(&usage, 97.0),
            Some(RateLimitWindow::SevenDay(_))
        ));
    }

    #[test]
    fn classify_both_hit_prioritizes_seven_day() {
        // 同时命中时，7 天窗口优先（限流更久）
        let usage = json!({
            "five_hour": make_window(json!(100), &rfc3339_at(ChronoDuration::hours(2))),
            "seven_day": make_window(json!(100), &rfc3339_at(ChronoDuration::days(5))),
        });
        assert!(matches!(
            classify_rate_limit(&usage, 97.0),
            Some(RateLimitWindow::SevenDay(_))
        ));
    }

    #[test]
    fn classify_only_sonnet_hit_is_ignored() {
        // Sonnet 7 天窗口命中，但其他两个未命中 → 返回 None（暂不处理 sonnet）
        let usage = json!({
            "five_hour": make_window(json!(10), &rfc3339_at(ChronoDuration::hours(2))),
            "seven_day": make_window(json!(10), &rfc3339_at(ChronoDuration::days(5))),
            "seven_day_sonnet": make_window(json!(100), &rfc3339_at(ChronoDuration::days(5))),
        });
        assert!(classify_rate_limit(&usage, 97.0).is_none());
    }

    #[test]
    fn classify_all_below_threshold_returns_none() {
        let usage = json!({
            "five_hour": make_window(json!(80), &rfc3339_at(ChronoDuration::hours(2))),
            "seven_day": make_window(json!(50), &rfc3339_at(ChronoDuration::days(5))),
        });
        assert!(classify_rate_limit(&usage, 97.0).is_none());
    }

    #[test]
    fn classify_boundary_at_exactly_97() {
        let usage = json!({
            "five_hour": make_window(json!(97), &rfc3339_at(ChronoDuration::hours(2))),
        });
        assert!(matches!(
            classify_rate_limit(&usage, 97.0),
            Some(RateLimitWindow::FiveHour(_))
        ));
    }

    #[test]
    fn classify_boundary_just_below_97() {
        let usage = json!({
            "five_hour": make_window(json!(96.99), &rfc3339_at(ChronoDuration::hours(2))),
        });
        assert!(classify_rate_limit(&usage, 97.0).is_none());
    }

    #[test]
    fn classify_seven_day_expired_reset_falls_through_to_five_hour() {
        // 7d utilization 命中但 resets_at 已过期 → check_usage_window 返回 None，降级到 5h 检查
        let usage = json!({
            "five_hour": make_window(json!(100), &rfc3339_at(ChronoDuration::hours(2))),
            "seven_day": make_window(json!(100), &rfc3339_at(ChronoDuration::hours(-1))),
        });
        assert!(matches!(
            classify_rate_limit(&usage, 97.0),
            Some(RateLimitWindow::FiveHour(_))
        ));
    }

    #[test]
    fn classify_invalid_json_structure_returns_none() {
        let usage = json!("not-an-object");
        assert!(classify_rate_limit(&usage, 97.0).is_none());
    }

    #[test]
    fn classify_threshold_config_is_honored() {
        // 测试不同 threshold 参数行为
        let usage = json!({
            "five_hour": make_window(json!(95), &rfc3339_at(ChronoDuration::hours(2))),
        });
        assert!(classify_rate_limit(&usage, 97.0).is_none());
        assert!(classify_rate_limit(&usage, 90.0).is_some());
    }

    // ---- tiered_decay ----

    #[test]
    fn tiered_decay_high_ratio() {
        assert_eq!(tiered_decay(0.9), 1.0);
        assert_eq!(tiered_decay(0.51), 1.0);
    }

    #[test]
    fn tiered_decay_boundary_values() {
        assert_eq!(tiered_decay(0.5), 0.8);  // 不满足 >0.5，落入中档
        assert_eq!(tiered_decay(0.2), 0.6);  // 不满足 >0.2，落入最低档
    }

    #[test]
    fn tiered_decay_low_ratio() {
        assert_eq!(tiered_decay(0.1), 0.6);
        assert_eq!(tiered_decay(0.0), 0.6);
    }

    #[test]
    fn tiered_decay_mid_range() {
        assert_eq!(tiered_decay(0.4), 0.8);
        assert_eq!(tiered_decay(0.21), 0.8);
    }
}
