//! 原生账号的有界后台准备；通知只携带账号归属，执行时重新解析权威状态。

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout_at};
use uuid::Uuid;

use crate::error::AppError;
use crate::model::account::{Account, AccountAuthType, AccountStatus};
use crate::service::account::AccountService;
use crate::service::native_contract::{
    NativeKernelTarget, NativePreparation, NativePreparationStatus,
};
use crate::service::runtime_transport::{
    PreparedTransport, TransportBinding, now_ms, prepare_existing,
};
use crate::service::system_update;

const QUEUE_LIMIT: usize = 128;
const PREPARE_PARALLELISM: usize = 2;

/// 管理准备生命周期与受控状态；RT 和业务正文不进入此对象。
pub(crate) struct NativePrewarmer {
    accounts: Arc<AccountService>,
    states: Arc<Mutex<HashMap<i64, Value>>>,
    started: AtomicBool,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

impl NativePrewarmer {
    /// 创建尚未启动的准备协调器。
    /// @param accounts 权威账号和凭据所有者。
    /// @return 独立协调器。
    pub(crate) fn new(accounts: Arc<AccountService>) -> Self {
        Self {
            accounts,
            states: Arc::new(Mutex::new(HashMap::new())),
            started: AtomicBool::new(false),
            shutdown: Mutex::new(None),
        }
    }

    /// 在网关装配完成后启动，首次扫描仅触及已运行环境。
    /// @return 无返回值；重复启动沿用现有任务。
    pub(crate) fn start(&self) {
        self.start_with_retries([5, 20, 60]);
    }

    /// 使用明确的启动补偿时刻，隔离夹具可将后续补偿移出竞态验证窗口。
    /// @param retry_seconds 相对于协调器启动的三次补偿秒数。
    /// @return 无返回值。
    pub(super) fn start_with_retries(&self, retry_seconds: [u64; 3]) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut events = self.accounts.subscribe_native_changes();
        let accounts = self.accounts.clone();
        let states = self.states.clone();
        let (stop, mut stopping) = oneshot::channel();
        *self.shutdown.lock().unwrap() = Some(stop);
        tokio::spawn(async move {
            let mut pending = BTreeSet::new();
            if let Ok(items) = accounts.list_accounts().await {
                pending.extend(
                    items
                        .into_iter()
                        .filter(|account| {
                            account.status == AccountStatus::Active
                                && account.request_backend == "native293"
                        })
                        .take(QUEUE_LIMIT)
                        .map(|account| account.id),
                );
            }
            let mut active: HashMap<i64, Option<oneshot::Sender<()>>> = HashMap::new();
            let mut jobs = JoinSet::new();
            let startup = Instant::now();
            let retries = retry_seconds.map(|seconds| startup + Duration::from_secs(seconds));
            let mut retry_index = 0;
            let mut inspections =
                interval_at(startup + Duration::from_secs(20), Duration::from_secs(20));
            inspections.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let mut failures: HashMap<i64, (u32, Instant)> = HashMap::new();
            let mut scan_after = 0;
            loop {
                while jobs.len() < PREPARE_PARALLELISM {
                    let Some(id) = pending.iter().copied().find(|id| !active.contains_key(id))
                    else {
                        break;
                    };
                    pending.remove(&id);
                    let preparation_id = Uuid::new_v4().to_string();
                    states
                        .lock()
                        .unwrap()
                        .entry(id)
                        .or_insert_with(|| json!({"state":"preparing"}))["checking"] = json!(true);
                    let (cancel, cancellation) = oneshot::channel();
                    active.insert(id, Some(cancel));
                    let accounts = accounts.clone();
                    jobs.spawn(async move {
                        let result =
                            check_one(accounts, id, preparation_id.clone(), cancellation, true)
                                .await;
                        (id, preparation_id, result)
                    });
                }
                // select 的禁用分支仍会构造 future，取值必须在最后一次补偿后保持有效。
                let retry_deadline = retries.get(retry_index).copied().unwrap_or(retries[2]);
                tokio::select! {
                    _ = inspections.tick() => {
                        if let Ok(mut items) = accounts.list_accounts().await {
                            // 队列有界时轮转扫描起点，避免高 ID 的运行账号一直被前批挤出。
                            items.sort_by_key(|account| (account.id <= scan_after, account.id));
                            for account in items {
                                if pending.len() >= QUEUE_LIMIT { break; }
                                if account.status == AccountStatus::Active && account.request_backend == "native293"
                                    && !active.contains_key(&account.id)
                                    && !pending.contains(&account.id)
                                    && failures.get(&account.id).is_none_or(|(_, next)| Instant::now() >= *next) {
                                    pending.insert(account.id);
                                    scan_after = account.id;
                                }
                            }
                        }
                    }
                    _ = &mut stopping => {
                        for cancel in active.values_mut().filter_map(Option::take) { let _ = cancel.send(()); }
                        // 留出私有取消与进程收口时间，超时后 Drop 继续隔离子进程。
                        let _ = timeout_at(Instant::now() + Duration::from_secs(6), async { while jobs.join_next().await.is_some() {} }).await;
                        break;
                    }
                    _ = sleep_until(retry_deadline), if retry_index < retries.len() => {
                        retry_index += 1;
                        // 控制面独立启动可能迟到；成功和在途账号跳过，休眠解析仍保持只读。
                        if let Ok(items) = accounts.list_accounts().await {
                            let states = states.lock().unwrap();
                            for account in items {
                                if pending.len() >= QUEUE_LIMIT { break; }
                                if account.status == AccountStatus::Active && account.request_backend == "native293"
                                    && !active.contains_key(&account.id)
                                    && states.get(&account.id).is_none_or(|state| state["state"] != "ready") {
                                    pending.insert(account.id);
                                }
                            }
                        }
                    }
                    event = events.recv() => match event {
                        Ok((id, invalidate)) => {
                            failures.remove(&id);
                            if invalidate {
                                if let Some(cancel) = active.get_mut(&id).and_then(Option::take) { let _ = cancel.send(()); }
                                states.lock().unwrap().insert(id, json!({"state":"preparing","reason":"invalidated"}));
                            }
                            // 在途任务结束后仍消费一次新提示，环境重建的唯一通知不能丢失。
                            if pending.len() < QUEUE_LIMIT || pending.contains(&id) { pending.insert(id); }
                            else { states.lock().unwrap().insert(id, json!({"state":"dropped","reason":"queue_full"})); }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            for cancel in active.values_mut().filter_map(Option::take) { let _ = cancel.send(()); }
                            if let Ok(items) = accounts.list_accounts().await {
                                for account in items.into_iter().take(QUEUE_LIMIT) {
                                    if pending.len() < QUEUE_LIMIT { pending.insert(account.id); }
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    completed = jobs.join_next(), if !jobs.is_empty() => {
                        if let Some(Ok((id, preparation_id, result))) = completed {
                            let cancelled = matches!(active.remove(&id), Some(None));
                            let mut result = if cancelled { json!({"state":"cancelled","reason":"invalidated"}) }
                                else { result.unwrap_or_else(|_| json!({"state":"failed","reason":"preparation_failed"})) };
                            result["preparation_id"] = json!(preparation_id);
                            result["account_id"] = json!(id);
                            result["checking"] = json!(false);
                            if matches!(result["state"].as_str(), Some("failed" | "unknown")) {
                                let count = failures.get(&id).map_or(1, |(count, _)| count.saturating_add(1));
                                let delay = 20_u64.saturating_mul(1 << count.min(4)).min(300);
                                failures.insert(id, (count, Instant::now() + Duration::from_secs(delay)));
                            } else { failures.remove(&id); }
                            let previous = states.lock().unwrap().get(&id).and_then(|value| value.get("last_preparation")).cloned();
                            if result.get("last_preparation").is_none() {
                                if let Some(previous) = previous { result["last_preparation"] = previous; }
                            }
                            states.lock().unwrap().insert(id, result);
                        }
                    }
                }
            }
        });
    }

    /// 查询发生时的准备状态，与环境基础 ready 独立。
    /// @param id 当前账号 ID。
    /// @return 受控状态摘要。
    pub(crate) fn status(&self, id: i64) -> Value {
        let mut status = self
            .states
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({"state":"unprepared"}));
        if status["state"] == "ready"
            && status["checked_at_ms"]
                .as_u64()
                .is_none_or(|checked| now_ms().saturating_sub(checked) > 45_000)
        {
            status["state"] = json!("unknown");
            status["reason"] = json!("check_expired");
        }
        status
    }

    /// 批量读取最近巡检快照，统一应用新鲜度规则。
    /// @return 按账号归属的安全状态数组。
    pub(crate) fn statuses(&self) -> Vec<Value> {
        let ids: Vec<i64> = self.states.lock().unwrap().keys().copied().collect();
        ids.into_iter()
            .map(|id| {
                let mut status = self.status(id);
                status["account_id"] = json!(id);
                status
            })
            .collect()
    }

    /// 主动状态读取只做本地检查，恢复统一交给有界后台协调器。
    /// @param id 已授权账号 ID。
    /// @return 当前身份对应的安全观测或明确未知状态。
    pub(crate) async fn inspect(&self, id: i64) -> Value {
        let (_cancel, cancellation) = oneshot::channel();
        let result = check_one(
            self.accounts.clone(),
            id,
            Uuid::new_v4().to_string(),
            cancellation,
            false,
        )
        .await;
        let mut result =
            result.unwrap_or_else(|_| json!({"state":"unknown","reason":"check_failed"}));
        if let Some(previous) = self.status(id).get("last_preparation") {
            result["last_preparation"] = previous.clone();
        }
        result
    }
}

impl Drop for NativePrewarmer {
    fn drop(&mut self) {
        if let Some(stop) = self.shutdown.lock().unwrap().take() {
            let _ = stop.send(());
        }
    }
}

async fn check_one(
    accounts: Arc<AccountService>,
    id: i64,
    preparation_id: String,
    mut cancellation: oneshot::Receiver<()>,
    allow_prepare: bool,
) -> Result<Value, AppError> {
    let _update_activity = system_update::admission()?;
    let deadline = Instant::now() + Duration::from_secs(120);
    let account = accounts.get_account(id).await?;
    if account.status != AccountStatus::Active || account.request_backend != "native293" {
        return Ok(json!({"state":"disabled"}));
    }
    let queue = accounts.get_or_create_queue(id, account.concurrency).await;
    if queue.runtime_gate.snapshot()["holders"]
        .as_array()
        .is_some_and(|holders| holders.iter().any(|holder| holder["exclusive"] == true))
    {
        return Ok(json!({"state":"maintenance","reason":"maintenance"}));
    }
    let Some(transport) = prepare_existing(&account).await? else {
        return Ok(json!({"state":"sleeping"}));
    };
    let activity = accounts.upstream_activity(id).await?;
    // 凭据刷新由现有所有者完整完成；取消不能打断 RT 的持久归属事务。
    let bearer = accounts.resolve_upstream_token(id).await?;
    let account = accounts.get_account(id).await?;
    if account.status != AccountStatus::Active || account.request_backend != "native293" {
        return Ok(json!({"state":"disabled"}));
    }
    let preparation = NativePreparation {
        account_id: id,
        environment_id: String::new(),
        generation: 0,
        preparation_id: preparation_id.clone(),
        deadline_unix_ms: now_ms()
            + deadline
                .saturating_duration_since(Instant::now())
                .as_millis() as u64,
        bearer,
        device_id: account.device_id.clone(),
        account_uuid: account.account_uuid.clone(),
        organization_uuid: account.organization_uuid.clone(),
        email: account.email.clone(),
        concurrency: account.concurrency,
    };
    if !matches_identity(&account, &preparation) {
        return Ok(json!({"state":"cancelled","reason":"identity_changed"}));
    }
    let mut last_readiness = None;
    let result = tokio::select! {
        biased;
        _ = &mut cancellation => None,
        _ = queue.runtime_gate.wait_exclusive() => None,
        _ = activity.closed() => None,
        result = timeout_at(deadline, async {
            let current = transport.native_readiness(preparation.clone()).await?;
            let mut status = serde_json::to_value(current).map_err(|_| AppError::Internal("检查状态编码失败".into()))?;
            last_readiness = Some(status.clone());
            if allow_prepare && matches!(status["state"].as_str(), Some("unprepared" | "failed")) {
                let prepared = transport.prepare_native(preparation.clone()).await?;
                let current = transport.native_readiness(preparation.clone()).await?;
                status = serde_json::to_value(current).map_err(|_| AppError::Internal("检查状态编码失败".into()))?;
                status["last_preparation"] = serde_json::to_value(prepared).map_err(|_| AppError::Internal("准备状态编码失败".into()))?;
            }
            // sender 回执只代表原绑定；发布前重新读取运行环境和权威身份。
            let current_transport = prepare_existing(&account).await?;
            let current_account = accounts.get_account(id).await?;
            if !current_transport.as_ref().is_some_and(|current| transport.same_binding(current))
                || !matches_identity(&current_account, &preparation) {
                return Err(AppError::ServiceUnavailable("原生准备快照已失效".into()));
            }
            Ok::<_, AppError>(status)
        }) => Some(result),
    };
    match result {
        Some(Ok(Ok(status))) => Ok(status),
        other => {
            let cancelled = other.is_none();
            if allow_prepare {
                transport.cancel_native_preparation(&preparation_id).await?;
            }
            let mut failure = json!({"state":if cancelled {"cancelled"} else if allow_prepare {"failed"} else {"unknown"},
                "reason":if cancelled {"invalidated"} else if allow_prepare {"preparation_failed"} else {"check_failed"}});
            if let Some(observation) = last_readiness {
                // 失败只沿用本次检查的归属和时间，禁止沿用旧 ready 或进程数量。
                for key in [
                    "account_id",
                    "environment_id",
                    "generation",
                    "kernel_generation",
                    "checked_at_ms",
                ] {
                    failure[key] = observation[key].clone();
                }
            }
            Ok(failure)
        }
    }
}

/// 调度器持有排他维护时，核对当前身份并对精确候选做零模型准备。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeKernelProbe {
    owner: String,
    lease_generation: String,
    binding: TransportBinding,
    target: NativeKernelTarget,
}

/// 在已声明的账号维护租约中准备精确内核，继续使用当前凭据归属。
/// @param accounts 权威账号服务。
/// @param id 网关账号 ID。
/// @param input 调度器冻结的租约、绑定和候选。
/// @return 核对过的实际 worker 回执。
pub(crate) async fn probe_kernel(
    accounts: &AccountService,
    id: i64,
    input: NativeKernelProbe,
) -> Result<NativePreparationStatus, AppError> {
    if input
        .owner
        .strip_prefix("kernel:")
        .and_then(|id| Uuid::parse_str(id).ok())
        .is_none()
        || Uuid::parse_str(&input.target.kernel_generation).is_err()
    {
        return Err(AppError::BadRequest("内核切换归属无效".into()));
    }
    let _activity = accounts.native_kernel_credentials(id).await?;
    let account = accounts.get_account(id).await?;
    if account.request_backend != "native293" || account.status != AccountStatus::Active {
        return Err(AppError::ServiceUnavailable("账号的原生后端未启用".into()));
    }
    let queue = accounts.get_or_create_queue(id, account.concurrency).await;
    let owns_lease = || {
        let snapshot = queue.runtime_gate.snapshot();
        let lease = &snapshot["lease"];
        snapshot["active"] == 0
            && lease["exclusive"] == true
            && lease["owner"] == input.owner
            && lease["generation"] == input.lease_generation
            && matches!(lease["phase"].as_str(), Some("claimed" | "reconcile"))
    };
    if !owns_lease() {
        return Err(AppError::ServiceUnavailable(
            "账号尚未排空或维护租约已变化".into(),
        ));
    }
    let bearer = match account.auth_type {
        AccountAuthType::SetupToken => account.setup_token.clone(),
        AccountAuthType::Oauth => account.access_token.clone(),
    };
    let preparation = NativePreparation {
        account_id: id,
        environment_id: String::new(),
        generation: 0,
        preparation_id: Uuid::new_v4().to_string(),
        deadline_unix_ms: now_ms().saturating_add(100_000),
        bearer,
        device_id: account.device_id.clone(),
        account_uuid: account.account_uuid.clone(),
        organization_uuid: account.organization_uuid.clone(),
        email: account.email.clone(),
        concurrency: account.concurrency,
    };
    let result =
        PreparedTransport::probe_native_kernel(input.binding, preparation.clone(), input.target)
            .await?;
    let current = accounts.get_account(id).await?;
    if !owns_lease()
        || !matches_identity(&current, &preparation)
        || current.proxy_url != account.proxy_url
        || current.auth_type != account.auth_type
    {
        return Err(AppError::ServiceUnavailable(
            "预热期间账号或维护归属已变化".into(),
        ));
    }
    Ok(result)
}

fn matches_identity(account: &Account, preparation: &NativePreparation) -> bool {
    let bearer = match account.auth_type {
        AccountAuthType::SetupToken => &account.setup_token,
        AccountAuthType::Oauth => &account.access_token,
    };
    account.status == AccountStatus::Active
        && account.request_backend == "native293"
        && account.id == preparation.account_id
        && bearer == &preparation.bearer
        && account.device_id == preparation.device_id
        && account.account_uuid == preparation.account_uuid
        && account.organization_uuid == preparation.organization_uuid
        && account.email == preparation.email
        && account.concurrency == preparation.concurrency
}
