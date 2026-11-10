//! 原生账号的有界后台准备；通知只携带账号归属，执行时重新解析权威状态。

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep_until, timeout_at};
use uuid::Uuid;

use crate::error::AppError;
use crate::model::account::{Account, AccountAuthType, AccountStatus};
use crate::service::account::AccountService;
use crate::service::native_contract::NativePreparation;
use crate::service::runtime_transport::{now_ms, prepare_existing};

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
            loop {
                while jobs.len() < PREPARE_PARALLELISM {
                    let Some(id) = pending.iter().copied().find(|id| !active.contains_key(id))
                    else {
                        break;
                    };
                    pending.remove(&id);
                    let preparation_id = Uuid::new_v4().to_string();
                    states.lock().unwrap().insert(
                        id,
                        json!({"state":"preparing","preparation_id":preparation_id}),
                    );
                    let (cancel, cancellation) = oneshot::channel();
                    active.insert(id, Some(cancel));
                    let accounts = accounts.clone();
                    jobs.spawn(async move {
                        let result =
                            prepare_one(accounts, id, preparation_id.clone(), cancellation).await;
                        (id, preparation_id, result)
                    });
                }
                // select 的禁用分支仍会构造 future，取值必须在最后一次补偿后保持有效。
                let retry_deadline = retries.get(retry_index).copied().unwrap_or(retries[2]);
                tokio::select! {
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
                            if invalidate {
                                if let Some(cancel) = active.get_mut(&id).and_then(Option::take) { let _ = cancel.send(()); }
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
        self.states
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({"state":"unprepared"}))
    }
}

impl Drop for NativePrewarmer {
    fn drop(&mut self) {
        if let Some(stop) = self.shutdown.lock().unwrap().take() {
            let _ = stop.send(());
        }
    }
}

async fn prepare_one(
    accounts: Arc<AccountService>,
    id: i64,
    preparation_id: String,
    mut cancellation: oneshot::Receiver<()>,
) -> Result<Value, AppError> {
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
        return Ok(json!({"state":"cancelled","reason":"maintenance"}));
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
    let result = tokio::select! {
        biased;
        _ = &mut cancellation => None,
        _ = queue.runtime_gate.wait_exclusive() => None,
        _ = activity.closed() => None,
        result = timeout_at(deadline, async {
            let status = transport.prepare_native(preparation.clone()).await?;
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
        Some(Ok(Ok(status))) => {
            serde_json::to_value(status).map_err(|_| AppError::Internal("准备状态编码失败".into()))
        }
        other => {
            let cancelled = other.is_none();
            transport.cancel_native_preparation(&preparation_id).await?;
            Ok(
                json!({"state":if cancelled {"cancelled"} else {"failed"},"reason":if cancelled {"invalidated"} else {"preparation_failed"}}),
            )
        }
    }
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
