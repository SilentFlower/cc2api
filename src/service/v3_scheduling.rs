use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use uuid::Uuid;

use crate::error::AppError;
use crate::model::account::Account;
use crate::model::request_diagnostics::DiagnosticParameters;
use crate::service::diagnostics;
pub use crate::store::scheduling_store::send_interval_ms;
use crate::store::scheduling_store::{SchedulingStore, SendClaim, SessionBinding};
use crate::store::settings_store::{SettingsStore, V3_SCHEDULING_DEFAULTS};

const HISTORY_MS: i64 = 5 * 60 * 1000;
const PER_ACCOUNT_EVENTS: usize = 500;
const GLOBAL_EVENTS: usize = 10000;
const MAX_OBSERVED_ACCOUNTS: usize = 10000;
const MAX_LATENCY_CLASSES: usize = 64;

/// 一次请求冻结的 v3 调度设置，不包含账号或凭据。
#[derive(Clone, Debug, Serialize)]
pub struct SchedulingConfig {
    pub mode: String,
    pub wait_seconds: u64,
    pub weights: [f64; 4],
}

impl SchedulingConfig {
    /// 从完整字符串设置解析一个原子快照。
    /// @param values 已合并本次更新的完整设置。
    /// @return 合法配置；非法值在持久化前拒绝。
    pub fn parse(values: &HashMap<String, String>) -> Result<Self, AppError> {
        let value = |key: &str| {
            values.get(key).map(String::as_str).unwrap_or_else(|| {
                V3_SCHEDULING_DEFAULTS
                    .iter()
                    .find(|(k, _)| *k == key)
                    .map(|(_, v)| *v)
                    .unwrap_or("")
            })
        };
        let mode = value("v3_scheduling_mode");
        if !matches!(mode, "smart" | "legacy_score") {
            return Err(AppError::BadRequest(
                "v3 调度模式必须为 smart 或 legacy_score".into(),
            ));
        }
        let wait_seconds = value("v3_admission_wait_seconds")
            .parse::<u64>()
            .ok()
            .filter(|n| (1..=120).contains(n))
            .ok_or_else(|| AppError::BadRequest("v3 准入等待必须为 1–120 秒整数".into()))?;
        let mut weights = [0.0; 4];
        for (i, key) in [
            "v3_smart_weight_load",
            "v3_smart_weight_quota",
            "v3_smart_weight_health",
            "v3_smart_weight_latency",
        ]
        .iter()
        .enumerate()
        {
            let parsed = value(key).parse::<f64>().ok().filter(|n| {
                n.is_finite()
                    && (0.0..=10.0).contains(n)
                    && (n * 10.0 - (n * 10.0).round()).abs() < 1e-8
            });
            weights[i] = parsed
                .ok_or_else(|| AppError::BadRequest(format!("{key} 必须为 0–10、最多一位小数")))?;
        }
        if weights.iter().sum::<f64>() == 0.0 {
            return Err(AppError::BadRequest("智能调度权重不能全部为零".into()));
        }
        Ok(Self {
            mode: mode.into(),
            wait_seconds,
            weights,
        })
    }
}

impl Default for SchedulingConfig {
    /// 从同一默认设置表构造有效配置。
    /// @return 默认配置。
    fn default() -> Self {
        Self::parse(&HashMap::new()).expect("内置 v3 配置必须有效")
    }
}

/// 新会话候选的硬资格和瞬时容量快照。
pub struct SchedulingCandidate {
    pub account: Account,
    pub active_units: i64,
    pub max_units: i64,
    pub queued: i64,
    pub queued_units: i64,
    pub legacy_score: f64,
    pub next_send_ms: i64,
}

/// 智能选择的归一化因素；高分优先，与旧评分独立。
#[derive(Clone, Debug, Serialize)]
pub struct SmartFactors {
    pub load: f64,
    pub quota: f64,
    pub health: f64,
    pub latency: f64,
    pub score: f64,
}

/// 常规请求的容量快照；不代表通过任意 Token／模型的权限判断。
#[derive(Clone, Serialize)]
pub struct CapacityView {
    pub immediate: bool,
    pub can_queue: bool,
    pub reason: String,
    pub earliest_submit_at_ms: i64,
}

/// 单个账号的管理摘要；旧评分字段保持独立。
#[derive(Serialize)]
pub struct SchedulingAccountSummary {
    pub config: SchedulingConfig,
    pub factors: SmartFactors,
    pub capacity: CapacityView,
    pub observed_at_ms: i64,
    pub bound_sessions: i64,
    pub active_sessions: usize,
    pub unbound_requests: usize,
    pub phases: PhaseCounts,
    pub next_send_at_ms: Option<i64>,
    pub submissions_last_60s: u64,
    pub history_available_from_ms: i64,
    pub history_truncated: bool,
}

/// 请求阶段的独立计数；不能当作会话数或已占槽位数。
#[derive(Default, Clone, Serialize)]
pub struct PhaseCounts {
    pub waiting_concurrency: usize,
    pub preparing: usize,
    pub waiting_send: usize,
    pub running: usize,
    pub backoff: usize,
}

impl PhaseCounts {
    /// 统计一组处于唯一阶段的请求。
    /// @param requests 当前状态快照。
    /// @return 各阶段请求数。
    pub fn of<'a>(requests: impl Iterator<Item = &'a ActiveRequest>) -> Self {
        let mut counts = Self::default();
        for request in requests {
            match request.phase {
                RequestPhase::WaitingConcurrency => counts.waiting_concurrency += 1,
                RequestPhase::Preparing => counts.preparing += 1,
                RequestPhase::WaitingSend => counts.waiting_send += 1,
                RequestPhase::Running => counts.running += 1,
                RequestPhase::Backoff => counts.backoff += 1,
                RequestPhase::Selecting => {}
            }
        }
        counts
    }
}

/// 当前请求所处阶段，一次请求只在一个阶段计数。
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequestPhase {
    Selecting,
    WaitingConcurrency,
    Preparing,
    WaitingSend,
    Running,
    Backoff,
}

/// 无正文、无凭据的当前请求状态。
#[derive(Clone, Serialize)]
pub struct ActiveRequest {
    pub request_id: String,
    pub session_key: Option<String>,
    pub account_id: i64,
    pub bound: bool,
    pub phase: RequestPhase,
    pub started_at_ms: i64,
    pub phase_since_ms: i64,
    #[serde(skip)]
    binding: Option<SessionBinding>,
}

/// 真实 Gateway → sender 提交点；不含未来预订。
#[derive(Clone, Serialize)]
pub struct SubmissionEvent {
    pub request_id: String,
    pub account_id: i64,
    pub submitted_at_ms: i64,
}

/// 只读运行快照；历史不足和丢失显式标记。
#[derive(Serialize)]
pub struct SchedulingSnapshot {
    pub observed_at_ms: i64,
    pub history_available_from_ms: i64,
    pub history_truncated: bool,
    pub dropped_events: u64,
    pub submissions_last_60s: u64,
    pub requests: Vec<ActiveRequest>,
    pub events: Vec<SubmissionEvent>,
}

/// 同模型、思考配置和流式方式才能比较首输出，不保存任意请求内容。
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq)]
struct LatencyClass {
    model: Option<String>,
    thinking_type: Option<String>,
    thinking_budget: Option<u64>,
    effort: Option<String>,
    stream: Option<bool>,
}

impl From<&DiagnosticParameters> for LatencyClass {
    /// 冻结已经过白名单过滤的请求事实，缺失参数不猜默认值。
    /// @param parameters 既有诊断参数摘要。
    /// @return 本请求使用的延迟分组。
    fn from(parameters: &DiagnosticParameters) -> Self {
        Self {
            model: parameters.model.clone(),
            thinking_type: parameters.thinking_type.clone(),
            thinking_budget: parameters.thinking_budget,
            effort: parameters.effort.clone(),
            stream: parameters.stream,
        }
    }
}

/// 一个分组的首输出平滑值和最近成功采样时间。
struct LatencySample {
    ewma_ms: f64,
    sampled_at_ms: i64,
}

/// 进程内软反馈；健康按账号共享，首输出按请求参数隔离。
#[derive(Default)]
struct Feedback {
    failure: f64,
    sampled_at_ms: i64,
    latency_ms: Option<f64>,
    latencies: HashMap<LatencyClass, LatencySample>,
    observed: bool,
}

/// 有界观测索引和轮转游标，不承担持久绑定的权威。
#[derive(Default)]
struct RuntimeState {
    active: HashMap<String, ActiveRequest>,
    events: VecDeque<SubmissionEvent>,
    buckets: HashMap<i64, VecDeque<(i64, u64)>>,
    dropped: HashMap<i64, u64>,
    feedback: HashMap<i64, Feedback>,
    rotations: HashMap<i32, i64>,
}

/// 发送队首独占的单调时钟，释放后继续保留一个最大间隔。
#[derive(Default)]
struct SendClock {
    last_released_at: Option<Instant>,
}

/// 每账号的公平提交队列及配置变更唤醒信号。
#[derive(Default)]
struct SendQueue {
    clock: Arc<AsyncMutex<SendClock>>,
    changed: Notify,
}

/// 已认领的当前发送许可，持有 FIFO 到提交记点；丢弃时保守消耗本次名额。
#[must_use = "发送许可须持有到 submitted，取消时显式 drop 释放"]
pub struct SendPermit {
    request: SchedulingRequest,
    account_id: i64,
    clock: OwnedMutexGuard<SendClock>,
    // 许可在途时保留队列，旁路清理不能创建第二条同账号 FIFO。
    _queue: Arc<SendQueue>,
}

impl SendPermit {
    /// 在 Gateway 发起实际提交时记录事件并释放队首，不等待上游响应。
    /// @return 无；本次提交后的间隔从许可释放时开始。
    pub fn submitted(self) {
        self.request.submitted(self.account_id);
    }
}

impl Drop for SendPermit {
    fn drop(&mut self) {
        // 提交前的事务或准备耗时不能抵扣后一个请求的发送间隔。
        self.clock.last_released_at = Some(Instant::now());
    }
}

/// v3 专用调度器；持久化强约束与进程内软反馈分别管理。
pub struct V3Scheduler {
    pub store: Arc<SchedulingStore>,
    config: RwLock<SchedulingConfig>,
    runtime: Mutex<RuntimeState>,
    send_queues: Mutex<HashMap<i64, Arc<SendQueue>>>,
    started_at_ms: i64,
}

/// 请求级准入期限与状态租约，最后一个持有者释放时收尾。
#[derive(Clone)]
pub struct SchedulingRequest(Arc<RequestLease>);

/// HTTP 请求及响应流共享的租约，Drop 移除活动登记。
struct RequestLease {
    scheduler: Arc<V3Scheduler>,
    id: String,
    key: Option<String>,
    config: SchedulingConfig,
    latency_class: LatencyClass,
    deadline: Instant,
    binding: Mutex<Option<SessionBinding>>,
    attempt_started: Mutex<Option<Instant>>,
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        self.scheduler.runtime().active.remove(&self.id);
    }
}

impl V3Scheduler {
    /// 观测锁不跨 await；异常展开不能把旁路状态变成新的业务故障。
    /// @return 短期状态锁。
    fn runtime(&self) -> MutexGuard<'_, RuntimeState> {
        self.runtime
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// 创建调度器，持久状态复用当前业务数据库。
    /// @param store 调度状态存储。
    /// @return 尚未装配的调度器。
    pub fn new(store: Arc<SchedulingStore>) -> Self {
        Self {
            store,
            config: RwLock::new(SchedulingConfig::default()),
            runtime: Mutex::new(RuntimeState::default()),
            send_queues: Mutex::new(HashMap::new()),
            started_at_ms: now_ms(),
        }
    }

    /// 一次读取并替换完整配置，失败保留旧快照。
    /// @param settings 全局字符串设置存储。
    /// @return 成功或设置错误。
    pub async fn reload(&self, settings: &SettingsStore) -> Result<(), AppError> {
        let values = settings.get_all().await?;
        let parsed = SchedulingConfig::parse(&values)?;
        *self
            .config
            .write()
            .unwrap_or_else(|error| error.into_inner()) = parsed;
        Ok(())
    }

    /// 保存成功后原子替换已校验配置，不再二次读库引入并发旧快照。
    /// @param config 本次在设置锁内解析并成功持久化的配置。
    /// @return 无。
    pub(crate) fn apply_config(&self, config: SchedulingConfig) {
        *self
            .config
            .write()
            .unwrap_or_else(|error| error.into_inner()) = config;
    }

    /// 取得用于管理展示的生效配置。
    /// @return 配置快照。
    pub fn config(&self) -> SchedulingConfig {
        self.config
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// 为请求创建 Token 隔离标识和唯一准入期限。
    /// @param token_id 现有已鉴权 Token ID，无显式 Token 为零命名空间。
    /// @param session_id 改写前解析的显式会话 ID。
    /// @return 会随流结束释放的租约。
    pub fn begin(self: &Arc<Self>, token_id: i64, session_id: Option<&str>) -> SchedulingRequest {
        self.begin_with_parameters(token_id, session_id, &DiagnosticParameters::default())
    }

    /// 创建请求租约并冻结延迟比较分组，不改变绑定键或准入期限。
    /// @param token_id 已鉴权的 Token 命名空间。
    /// @param session_id 改写前显式会话 ID。
    /// @param parameters 已通过白名单过滤的模型和思考参数。
    /// @return 流结束时释放的请求租约。
    pub fn begin_with_parameters(
        self: &Arc<Self>,
        token_id: i64,
        session_id: Option<&str>,
        parameters: &DiagnosticParameters,
    ) -> SchedulingRequest {
        let config = self.config();
        let key = session_id.filter(|s| !s.is_empty()).map(|sid| {
            let mut hash = Sha256::new();
            hash.update(b"cc2api:v3:session\0");
            hash.update(token_id.to_be_bytes());
            hash.update(sid.as_bytes());
            hex::encode(hash.finalize())
        });
        let id = diagnostics::current_request_id().unwrap_or_else(|| Uuid::new_v4().to_string());
        let time = now_ms();
        self.runtime().active.insert(
            id.clone(),
            ActiveRequest {
                request_id: id.clone(),
                session_key: key.clone(),
                account_id: 0,
                bound: false,
                phase: RequestPhase::Selecting,
                started_at_ms: time,
                phase_since_ms: time,
                binding: None,
            },
        );
        let request = SchedulingRequest(Arc::new(RequestLease {
            scheduler: self.clone(),
            id,
            key,
            deadline: Instant::now() + Duration::from_secs(config.wait_seconds),
            config,
            latency_class: LatencyClass::from(parameters),
            binding: Mutex::new(None),
            attempt_started: Mutex::new(None),
        }));
        request
    }

    /// 启动独立于管理读取的活动续租和有界清理。
    /// @return 无；仅持有调度器弱引用。
    pub fn start_cleanup(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(60)).await;
                let Some(scheduler) = weak.upgrade() else {
                    break;
                };
                let _ = timeout(Duration::from_secs(2), scheduler.cleanup_expired()).await;
                scheduler.prune(now_ms());
            }
        });
    }

    /// 账号成功删除后回收短期观测，保留原请求资源由各自 Drop 收尾。
    /// @param account_id 已删除的账号。
    /// @return 无。
    pub fn forget_account(&self, account_id: i64) {
        let mut state = self.runtime();
        state.events.retain(|event| event.account_id != account_id);
        state.buckets.remove(&account_id);
        state.dropped.remove(&account_id);
        state.feedback.remove(&account_id);
        self.send_queues
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&account_id);
    }

    /// 账号配置持久化后唤醒发送队首，重新读取 RPM，不重置原请求期限。
    /// @param account_id 已保存的账号 ID。
    /// @return 无；没有等待者时只保留一次通知。
    pub fn account_updated(&self, account_id: i64) {
        let queues = self
            .send_queues
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(queue) = queues.get(&account_id) {
            queue.changed.notify_one();
        }
    }

    /// 返回活动租约携带的版本提示；仍须经数据库 CAS 续租和重读确认。
    /// @param key 匿名会话 key。
    /// @return 不同版本的活动绑定提示。
    pub fn live_bindings(&self, key: &str) -> Vec<SessionBinding> {
        let state = self.runtime();
        let mut seen = HashSet::new();
        state
            .active
            .values()
            .filter_map(|r| r.binding.as_ref())
            .filter(|b| b.session_key == key && seen.insert(b.revision.clone()))
            .cloned()
            .collect()
    }

    /// 有界清理闲置绑定，保护仍有请求持有的版本。
    /// @return 清理结果。
    pub async fn cleanup_expired(&self) -> Result<(), AppError> {
        let mut seen = HashSet::new();
        let bindings = self
            .runtime()
            .active
            .values()
            .filter(|r| r.bound)
            .filter_map(|r| r.binding.as_ref())
            .filter(|b| seen.insert(b.revision.clone()))
            .cloned()
            .collect::<Vec<_>>();
        let protected = bindings
            .iter()
            .map(|b| b.session_key.clone())
            .collect::<Vec<_>>();
        // 先对当前版本批量续租，避免存储恢复后的活动行挡在过期清理队首。
        self.store.renew_active(&bindings, now_ms()).await?;
        for _ in 0..50 {
            if self.store.cleanup_batch(now_ms(), &protected).await? == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// 计算管理汇总智能因素，实际选号复用同一公式并限定请求分组。
    /// @param candidate 已经过硬资格过滤的容量快照。
    /// @param config 本请求冻结的设置。
    /// @param now_ms 当前 UTC 毫秒。
    /// @return 归一化因素与综合分。
    pub fn factors(
        &self,
        candidate: &SchedulingCandidate,
        config: &SchedulingConfig,
        now_ms: i64,
    ) -> SmartFactors {
        self.factors_for_class(candidate, config, now_ms, None)
    }

    fn factors_for_class(
        &self,
        candidate: &SchedulingCandidate,
        config: &SchedulingConfig,
        now_ms: i64,
        class: Option<&LatencyClass>,
    ) -> SmartFactors {
        let pressure = (candidate.active_units + candidate.queued_units).max(0) as f64
            / candidate.max_units.max(1) as f64;
        let wait = (candidate.next_send_ms - now_ms).max(0) as f64;
        let load = 1.0 / (1.0 + pressure + wait / 1000.0);
        let quota = remaining_quota(&candidate.account, now_ms);
        let state = self.runtime();
        let feedback = state.feedback.get(&candidate.account.id);
        let health = feedback
            .map(|f| {
                let decay = 0.5_f64.powf((now_ms - f.sampled_at_ms).max(0) as f64 / 900000.0);
                if f.observed {
                    1.0 - f.failure * decay
                } else {
                    0.5
                }
            })
            .unwrap_or(0.5);
        // 只有管理页使用账号汇总；实际选号缺少同类样本时保持中性，不能借用其它模型。
        let latency_ms = feedback.and_then(|f| match class {
            Some(class) => f.latencies.get(class).map(|sample| sample.ewma_ms),
            None => f.latency_ms,
        });
        let latency = latency_ms
            .map(|ms| 1.0 / (1.0 + ms / 10000.0))
            .unwrap_or(0.5);
        let values = [load, quota, health, latency];
        let score = values
            .iter()
            .zip(config.weights)
            .map(|(v, w)| v * w)
            .sum::<f64>()
            / config.weights.iter().sum::<f64>();
        SmartFactors {
            load,
            quota,
            health,
            latency,
            score,
        }
    }

    /// 共享容量判断，避免管理展示与实际选择使用不同算法。
    /// @param candidate 账号的实时容量。
    /// @param slot_units 当前请求单位。
    /// @param budget_ms 剩余等待预算。
    /// @param now 当前 UTC 毫秒。
    /// @return 即时／有界等待资格及可解释原因。
    pub fn capacity(
        &self,
        candidate: &SchedulingCandidate,
        slot_units: u32,
        budget_ms: i64,
        now: i64,
    ) -> CapacityView {
        let pending = self
            .runtime()
            .active
            .values()
            .filter(|r| {
                r.account_id == candidate.account.id
                    && matches!(r.phase, RequestPhase::Preparing | RequestPhase::WaitingSend)
            })
            .count() as i64;
        let earliest = candidate.next_send_ms.max(now).saturating_add(
            (candidate.queued + pending)
                .saturating_mul(send_interval_ms(candidate.account.rpm_limit)),
        );
        let full = candidate.queued >= i64::from(candidate.account.concurrency.max(1)) * 2;
        let slots = candidate.active_units + i64::from(slot_units) <= candidate.max_units;
        let immediate = !full
            && candidate.queued == 0
            && pending == 0
            && slots
            && candidate.next_send_ms <= now;
        let can_queue = !full && earliest.saturating_sub(now) <= budget_ms;
        let reason = if full {
            "queue_full"
        } else if !can_queue {
            "wait_budget"
        } else if !slots || candidate.queued > 0 {
            "concurrency"
        } else if pending > 0 {
            "pending_submissions"
        } else if candidate.next_send_ms > now {
            "pacing"
        } else {
            "ready"
        };
        CapacityView {
            immediate,
            can_queue,
            reason: reason.into(),
            earliest_submit_at_ms: earliest,
        }
    }

    /// 在容量许可范围内选择新会话；近优候选稳定轮转。
    /// @param candidates 资格合格的账号及实时容量。
    /// @param request 当前请求租约。
    /// @param slot_units 当前请求的加权单位。
    /// @return 选择结果；没有有界容量则返回本地限流。
    pub fn choose(
        &self,
        mut candidates: Vec<SchedulingCandidate>,
        request: &SchedulingRequest,
        slot_units: u32,
    ) -> Result<Account, AppError> {
        request.check_deadline()?;
        if candidates.is_empty() {
            return Err(AppError::ServiceUnavailable("无合格账号".into()));
        }
        let now = now_ms();
        let remaining = request.remaining()?.as_millis() as i64;
        candidates.retain(|c| self.capacity(c, slot_units, remaining, now).can_queue);
        if candidates.is_empty() {
            return Err(limited());
        }
        // 彻底没有等待容量的账号不阻挡低优先组；可接纳时仍遵守人工优先级。
        let priority = candidates.iter().map(|c| c.account.priority).min().unwrap();
        candidates.retain(|c| c.account.priority == priority);
        let immediate = candidates
            .iter()
            .any(|c| self.capacity(c, slot_units, remaining, now).immediate);
        if immediate {
            candidates.retain(|c| self.capacity(c, slot_units, remaining, now).immediate);
        }
        let mut scored: Vec<(Account, f64)> = candidates
            .into_iter()
            .map(|c| {
                let score = if request.0.config.mode == "legacy_score" {
                    -c.legacy_score
                } else {
                    self.factors_for_class(
                        &c,
                        &request.0.config,
                        now,
                        Some(&request.0.latency_class),
                    )
                    .score
                };
                (c.account, score)
            })
            .collect();
        let best = scored
            .iter()
            .map(|(_, s)| *s)
            .fold(f64::NEG_INFINITY, f64::max);
        let tolerance = if request.0.config.mode == "smart" {
            0.05
        } else {
            0.01
        };
        scored.retain(|(_, score)| best - score <= tolerance);
        scored.sort_by_key(|(a, _)| a.id);
        let mut state = self.runtime();
        let previous = state.rotations.get(&priority).copied().unwrap_or(0);
        let index = scored
            .iter()
            .position(|(a, _)| a.id > previous)
            .unwrap_or(0);
        let account = scored.swap_remove(index).0;
        if state.rotations.len() >= MAX_OBSERVED_ACCOUNTS
            && !state.rotations.contains_key(&priority)
        {
            if let Some(oldest) = state.rotations.keys().next().copied() {
                state.rotations.remove(&oldest);
            }
        }
        state.rotations.insert(priority, account.id);
        Ok(account)
    }

    /// 只读取得当前账号的阶段与真实提交历史。
    /// @param account_id 目标账号。
    /// @return 有采集完整性信息的快照。
    pub fn snapshot(&self, account_id: i64) -> SchedulingSnapshot {
        let now = now_ms();
        let state = self.runtime();
        SchedulingSnapshot {
            observed_at_ms: now,
            history_available_from_ms: self.started_at_ms.max(now - HISTORY_MS),
            history_truncated: state.dropped.get(&account_id).copied().unwrap_or(0) > 0,
            dropped_events: state.dropped.get(&account_id).copied().unwrap_or(0),
            submissions_last_60s: state
                .buckets
                .get(&account_id)
                .map(|b| {
                    b.iter()
                        .filter(|(s, _)| *s * 1000 > now - 60000)
                        .map(|(_, n)| *n)
                        .sum()
                })
                .unwrap_or(0),
            requests: state
                .active
                .values()
                .filter(|r| r.account_id == account_id)
                .cloned()
                .collect(),
            events: state
                .events
                .iter()
                .filter(|e| e.account_id == account_id && e.submitted_at_ms >= now - HISTORY_MS)
                .cloned()
                .collect(),
        }
    }

    /// 返回当前账号中有活动请求的已绑定会话 key。
    /// @param account_id 目标账号。
    /// @return 去重后的匿名 key。
    pub fn active_keys(&self, account_id: i64) -> Vec<String> {
        self.snapshot(account_id)
            .requests
            .into_iter()
            .filter(|r| r.bound)
            .filter_map(|r| r.session_key)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn prune(&self, now: i64) {
        let mut state = self.runtime();
        while state
            .events
            .front()
            .is_some_and(|e| e.submitted_at_ms < now - HISTORY_MS)
        {
            state.events.pop_front();
        }
        state.buckets.retain(|_, b| {
            while b.front().is_some_and(|(s, _)| *s * 1000 < now - HISTORY_MS) {
                b.pop_front();
            }
            !b.is_empty()
        });
        let live_accounts = state
            .buckets
            .keys()
            .copied()
            .chain(state.active.values().map(|r| r.account_id))
            .collect::<HashSet<_>>();
        state.dropped.retain(|id, _| live_accounts.contains(id));
        state.feedback.retain(|id, feedback| {
            live_accounts.contains(id) || feedback.sampled_at_ms > now - 24 * 60 * 60 * 1000
        });
        self.send_queues
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, queue| {
                Arc::strong_count(queue) > 1
                    || queue
                        .clock
                        .try_lock()
                        .map(|clock| {
                            clock
                                .last_released_at
                                .is_some_and(|at| at.elapsed() < Duration::from_secs(60))
                        })
                        .unwrap_or(true)
            });
    }

    fn feedback(
        &self,
        account_id: i64,
        failure: bool,
        capacity: bool,
        latency_ms: Option<f64>,
        class: &LatencyClass,
    ) {
        let now = now_ms();
        let mut state = self.runtime();
        let f = state.feedback.entry(account_id).or_default();
        if !f.observed {
            f.failure = 0.5;
            f.sampled_at_ms = now;
        }
        let alpha = if capacity { 0.4 } else { 0.2 };
        let decayed = f.failure * 0.5_f64.powf((now - f.sampled_at_ms).max(0) as f64 / 900000.0);
        f.failure = decayed * (1.0 - alpha) + if failure { alpha } else { 0.0 };
        f.sampled_at_ms = now;
        f.observed = true;
        if let Some(ms) = latency_ms.filter(|ms| ms.is_finite() && *ms >= 0.0) {
            f.latency_ms = Some(f.latency_ms.map(|old| old * 0.8 + ms * 0.2).unwrap_or(ms));
            // 参数组合可能持续增长；淘汰最旧统计只使该组恢复中性，不限制业务准入。
            if !f.latencies.contains_key(class) && f.latencies.len() >= MAX_LATENCY_CLASSES {
                if let Some(oldest) = f
                    .latencies
                    .iter()
                    .min_by_key(|(_, sample)| sample.sampled_at_ms)
                    .map(|(key, _)| key.clone())
                {
                    f.latencies.remove(&oldest);
                }
            }
            let sample = f.latencies.entry(class.clone()).or_insert(LatencySample {
                ewma_ms: ms,
                sampled_at_ms: now,
            });
            sample.ewma_ms = sample.ewma_ms * 0.8 + ms * 0.2;
            sample.sampled_at_ms = now;
        }
        if state.feedback.len() > MAX_OBSERVED_ACCOUNTS {
            if let Some(oldest) = state
                .feedback
                .iter()
                .min_by_key(|(_, f)| f.sampled_at_ms)
                .map(|(id, _)| *id)
            {
                state.feedback.remove(&oldest);
            }
        }
    }
}

impl SchedulingRequest {
    /// 获取与现有上游 session 改写分离的匿名绑定 key。
    /// @return Token 隔离的可选 key。
    pub fn key(&self) -> Option<&str> {
        self.0.key.as_deref()
    }

    /// 获取本请求的唯一准入截止时刻。
    /// @return 单调时钟截止时刻。
    pub fn deadline(&self) -> Instant {
        self.0.deadline
    }

    /// 返回尚可使用的等待预算。
    /// @return 剩余预算或本地 429。
    pub fn remaining(&self) -> Result<Duration, AppError> {
        self.0
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(limited)
    }

    /// 检查准入期限，不中断已经提交的响应流。
    /// @return 期限内成功，否则本地 429。
    pub fn check_deadline(&self) -> Result<(), AppError> {
        self.remaining().map(|_| ())
    }

    /// 设置本轮候选及所读绑定，供发送前 CAS 使用。
    /// @param account 通过硬门禁的账号。
    /// @param binding 当前权威绑定；迁移时仍保留旧版本。
    /// @return 无。
    pub fn selected(&self, account: &Account, binding: Option<SessionBinding>) {
        let bound = binding.as_ref().is_some_and(|b| b.account_id == account.id);
        *self
            .0
            .binding
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = binding.clone();
        if let Some(state) = self.0.scheduler.runtime().active.get_mut(&self.0.id) {
            state.account_id = account.id;
            state.bound = bound;
            state.binding = binding;
        }
        self.phase(RequestPhase::WaitingConcurrency);
    }

    /// 更新当前唯一阶段；不修改持久绑定。
    /// @param phase 当前阶段。
    /// @return 无。
    pub fn phase(&self, phase: RequestPhase) {
        if let Some(state) = self.0.scheduler.runtime().active.get_mut(&self.0.id) {
            state.phase = phase;
            state.phase_since_ms = now_ms();
        }
    }

    /// 环境就绪后以 FIFO 获取当前一个发送许可。
    /// @param account 已验证身份的账号快照。
    /// @return 必须持有到真实提交记点的许可，或期限、存储、绑定竞争错误。
    pub async fn admit(&self, account: &Account) -> Result<SendPermit, AppError> {
        self.check_deadline()?;
        self.phase(RequestPhase::WaitingSend);
        let queue = {
            let mut queues = self
                .0
                .scheduler
                .send_queues
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            queues.entry(account.id).or_default().clone()
        };
        let clock = timeout_at(self.deadline(), queue.clock.clone().lock_owned())
            .await
            .map_err(|_| limited())?;
        loop {
            self.check_deadline()?;
            let expected = self
                .0
                .binding
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone();
            let claim = timeout_at(
                self.deadline(),
                self.0.scheduler.store.claim_current(
                    account.id,
                    self.key(),
                    expected.as_ref(),
                    clock.last_released_at,
                ),
            )
            .await
            .map_err(|_| limited())??;
            match claim {
                SendClaim::Acquired(binding) => {
                    *self
                        .0
                        .binding
                        .lock()
                        .unwrap_or_else(|error| error.into_inner()) = binding.clone();
                    let mut runtime = self.0.scheduler.runtime();
                    for state in runtime
                        .active
                        .values_mut()
                        .filter(|state| state.session_key.as_deref() == self.key())
                    {
                        state.bound = self.key().is_some() && state.account_id == account.id;
                        if state.bound {
                            state.binding = binding.clone();
                        }
                    }
                    return Ok(SendPermit {
                        request: self.clone(),
                        account_id: account.id,
                        clock,
                        _queue: queue,
                    });
                }
                SendClaim::BindingConflict => return Err(AppError::SchedulingConflict),
                SendClaim::WaitUntil(at) => {
                    let wait = Duration::from_millis((at - now_ms()).max(1) as u64);
                    timeout_at(self.deadline(), async {
                        tokio::select! {
                            _ = sleep(wait) => {},
                            _ = queue.changed.notified() => {},
                        }
                    })
                    .await
                    .map_err(|_| limited())?;
                }
            }
        }
    }

    /// 在实际内部 POST 被发起时记点，不把许可时间伪造为发送。
    /// @param account_id 当前账号。
    /// @return 无。
    pub fn submitted(&self, account_id: i64) {
        let now = now_ms();
        self.phase(RequestPhase::Running);
        *self
            .0
            .attempt_started
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(Instant::now());
        self.0.scheduler.prune(now);
        let mut state = self.0.scheduler.runtime();
        let buckets = state.buckets.entry(account_id).or_default();
        let second = now / 1000;
        if let Some((last, count)) = buckets.back_mut().filter(|(s, _)| *s == second) {
            *last = second;
            *count += 1;
        } else {
            buckets.push_back((second, 1));
        }
        if state
            .events
            .iter()
            .filter(|e| e.account_id == account_id)
            .count()
            >= PER_ACCOUNT_EVENTS
        {
            if let Some(index) = state.events.iter().position(|e| e.account_id == account_id) {
                state.events.remove(index);
                *state.dropped.entry(account_id).or_default() += 1;
            }
        }
        if state.events.len() >= GLOBAL_EVENTS {
            if let Some(event) = state.events.pop_front() {
                *state.dropped.entry(event.account_id).or_default() += 1;
            }
        }
        state.events.push_back(SubmissionEvent {
            request_id: self.0.id.clone(),
            account_id,
            submitted_at_ms: now,
        });
    }

    /// 无诊断观察器时仅按响应头分类健康，不能把头部耗时当作有效输出。
    /// @param account_id 当前账号。
    /// @param status 实际上游 HTTP 状态。
    /// @return 无。
    pub fn response_headers(&self, account_id: i64, status: u16) {
        let started = self
            .0
            .attempt_started
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if started.is_some() {
            if (200..300).contains(&status) || status >= 500 {
                self.0.scheduler.feedback(
                    account_id,
                    status >= 500,
                    false,
                    None,
                    &self.0.latency_class,
                );
            }
        }
    }

    /// 复用已有观察器的终态，HTTP 200 错误事件不能被当作成功。
    /// @param account_id 实际账号。
    /// @param status 实际上游状态，未知不造状态码。
    /// @param outcome 已有观察器判定的终态。
    /// @param error_type 白名单错误类型。
    /// @param latency_ms 已观察到的首个有效输出耗时；未采到时为空。
    /// @return 无；本地取消、采样失败和用户输入错误不惩罚账号。
    pub(crate) fn finish_observed_attempt(
        &self,
        account_id: i64,
        status: Option<u16>,
        outcome: &str,
        error_type: Option<&str>,
        latency_ms: Option<u64>,
    ) {
        if outcome == "success" {
            self.0.scheduler.feedback(
                account_id,
                false,
                false,
                latency_ms.map(|ms| ms as f64),
                &self.0.latency_class,
            );
        } else if !matches!(outcome, "cancelled" | "transport_error")
            && (status.is_some_and(|s| s >= 500)
                || (matches!(outcome, "upstream_error" | "body_error")
                    && !error_type.is_some_and(|kind| {
                        matches!(
                            kind,
                            "invalid_request_error"
                                | "authentication_error"
                                | "permission_error"
                                | "not_found_error"
                        )
                    })))
        {
            self.0
                .scheduler
                .feedback(account_id, true, false, None, &self.0.latency_class);
        }
    }

    /// 已发起提交但传输结果未知时记录失败；确定的发送前拒绝不调用。
    /// @param account_id 当前账号。
    /// @return 无。
    pub fn submission_failed(&self, account_id: i64) {
        if self
            .0
            .attempt_started
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            .is_some()
        {
            self.0
                .scheduler
                .feedback(account_id, true, false, None, &self.0.latency_class);
        }
    }

    /// 仅在已经确认上游容量拒绝后反馈，不用于本地 429。
    /// @param account_id 当前账号。
    /// @return 无。
    pub fn capacity_rejected(&self, account_id: i64) {
        self.0
            .scheduler
            .feedback(account_id, true, true, None, &self.0.latency_class);
    }
}

/// 当前绝对时间，仅用于持久化和展示；等待使用单调时钟。
/// @return UTC 毫秒。
pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn limited() -> AppError {
    AppError::AdmissionLimited { retry_after: 1 }
}

fn remaining_quota(account: &Account, now: i64) -> f64 {
    let windows = ["five_hour", "seven_day"]
        .into_iter()
        .filter_map(|name| {
            let window = account.usage_data.get(name)?;
            let usage = window
                .get("utilization")?
                .as_f64()
                .filter(|v| v.is_finite() && *v >= 0.0)?;
            let reset = DateTime::parse_from_rfc3339(window.get("resets_at")?.as_str()?).ok()?;
            (reset.timestamp_millis() > now).then_some(1.0 - (usage / 100.0).clamp(0.0, 1.0))
        })
        .collect::<Vec<_>>();
    windows.into_iter().reduce(f64::min).unwrap_or(0.5)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sqlx::any::AnyPoolOptions;

    fn scheduler() -> Arc<V3Scheduler> {
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .connect_lazy("sqlite::memory:")
            .unwrap();
        let scheduler = Arc::new(V3Scheduler::new(Arc::new(SchedulingStore::new(pool))));
        scheduler.apply_config(SchedulingConfig {
            weights: [0.0, 0.0, 0.0, 1.0],
            ..Default::default()
        });
        scheduler
    }

    fn candidate(id: i64) -> SchedulingCandidate {
        let account = serde_json::from_value(json!({
            "id":id,"name":"合成延迟样本","email":format!("latency-{id}@example.invalid"),"status":"active",
            "device_id":"synthetic-device","canonical_env":{},"canonical_prompt_env":{},"canonical_process":{},
            "billing_mode":"strip","concurrency":4,"priority":50,"created_at":Utc::now(),"updated_at":Utc::now()
        })).unwrap();
        SchedulingCandidate {
            account,
            active_units: 0,
            max_units: 8,
            queued: 0,
            queued_units: 0,
            legacy_score: 0.0,
            next_send_ms: 0,
        }
    }

    /// 同类成功样本更新 EWMA，其他模型、思考与流式分组保持未知，健康仍按账号共享。
    #[tokio::test]
    async fn first_output_latency_is_scoped_and_only_successful_output_updates_ewma() {
        let scheduler = scheduler();
        let params = json!({"model":"claude-sonnet-5","thinking":{"type":"enabled","budget_tokens":1024},"output_config":{"effort":"high"},"stream":true});
        let request =
            scheduler.begin_with_parameters(0, None, &DiagnosticParameters::from_request(&params));
        let candidate = candidate(7);
        request.finish_observed_attempt(7, Some(200), "success", None, Some(2000));
        let factors = || {
            scheduler.factors_for_class(
                &candidate,
                &scheduler.config(),
                now_ms(),
                Some(&request.0.latency_class),
            )
        };
        assert!((factors().latency - 1.0 / 1.2).abs() < 1e-9);
        for (outcome, status, error) in [
            ("cancelled", Some(200), None),
            ("incomplete", Some(200), Some("sampling_limit")),
            ("body_error", Some(200), Some("decode_error")),
            ("upstream_error", Some(200), Some("overloaded_error")),
            ("upstream_rejected", Some(500), None),
        ] {
            request.finish_observed_attempt(7, status, outcome, error, Some(1));
            assert!((factors().latency - 1.0 / 1.2).abs() < 1e-9);
        }
        request.finish_observed_attempt(7, Some(200), "success", None, None);
        request.submitted(7);
        request.response_headers(7, 200);
        assert!((factors().latency - 1.0 / 1.2).abs() < 1e-9);
        for (pointer, value) in [
            ("/model", json!("claude-opus-5")),
            ("/thinking/type", json!("adaptive")),
            ("/thinking/budget_tokens", json!(2048)),
            ("/output_config/effort", json!("low")),
            ("/stream", json!(false)),
        ] {
            let mut different = params.clone();
            *different.pointer_mut(pointer).unwrap() = value;
            let class = LatencyClass::from(&DiagnosticParameters::from_request(&different));
            let other = scheduler.factors_for_class(
                &candidate,
                &scheduler.config(),
                now_ms(),
                Some(&class),
            );
            assert_eq!(other.latency, 0.5);
            assert!(other.health > 0.0);
        }
        request.finish_observed_attempt(7, Some(200), "success", None, Some(4000));
        assert!((factors().latency - 1.0 / 1.24).abs() < 1e-9);
        assert!(
            (scheduler
                .factors(&candidate, &scheduler.config(), now_ms())
                .latency
                - factors().latency)
                .abs()
                < 1e-9
        );
    }

    /// 不同模型的快慢顺序相反时，实际选号必须跟随该请求的分组。
    #[tokio::test]
    async fn first_output_scoped_latency_changes_actual_selection_without_header_fallback() {
        let scheduler = scheduler();
        let fast_sonnet = scheduler.begin_with_parameters(
            0,
            None,
            &DiagnosticParameters::from_request(&json!({"model":"claude-sonnet-5"})),
        );
        let fast_opus = scheduler.begin_with_parameters(
            0,
            None,
            &DiagnosticParameters::from_request(&json!({"model":"claude-opus-5"})),
        );
        fast_sonnet.finish_observed_attempt(7, Some(200), "success", None, Some(100));
        fast_sonnet.finish_observed_attempt(8, Some(200), "success", None, Some(30000));
        fast_opus.finish_observed_attempt(7, Some(200), "success", None, Some(30000));
        fast_opus.finish_observed_attempt(8, Some(200), "success", None, Some(100));
        assert_eq!(
            scheduler
                .choose(vec![candidate(7), candidate(8)], &fast_sonnet, 2)
                .unwrap()
                .id,
            7
        );
        assert_eq!(
            scheduler
                .choose(vec![candidate(7), candidate(8)], &fast_opus, 2)
                .unwrap()
                .id,
            8
        );
        let unknown = scheduler.begin(0, None);
        unknown.submitted(9);
        unknown.response_headers(9, 200);
        assert_eq!(
            scheduler
                .factors(&candidate(9), &scheduler.config(), now_ms())
                .latency,
            0.5
        );
    }

    /// 分组数量有界，淘汰只恢复未知，不改变业务可用性。
    #[tokio::test]
    async fn first_output_latency_classes_are_bounded() {
        let scheduler = scheduler();
        for budget in 1..=65 {
            let request = scheduler.begin_with_parameters(
                0,
                None,
                &DiagnosticParameters::from_request(&json!({
                    "model":"claude-sonnet-5","thinking":{"type":"enabled","budget_tokens":budget}
                })),
            );
            request.finish_observed_attempt(7, Some(200), "success", None, Some(20000));
        }
        let state = scheduler.runtime();
        let feedback = state.feedback.get(&7).unwrap();
        assert_eq!(feedback.latencies.len(), MAX_LATENCY_CLASSES);
        assert!(feedback.latencies.contains_key(&LatencyClass::from(
            &DiagnosticParameters::from_request(&json!({
                "model":"claude-sonnet-5","thinking":{"type":"enabled","budget_tokens":65}
            }))
        )));
    }
}
