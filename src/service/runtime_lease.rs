//! 账号有界 CLI 槽位、原 CLI 独占及排他维护门禁。

use std::fs::{self, File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, from_slice, json, to_vec};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::error::AppError;

const CLI_CAPACITY: usize = 2;

/// 一个执行来源持有的持久租约；只有维护排他会关闭 API 入场。
#[derive(Clone, Deserialize, Serialize)]
pub struct RuntimeLease {
    pub owner: String,
    pub generation: String,
    pub expires_at: u64,
    pub phase: String,
    #[serde(default = "exclusive_by_default")]
    pub exclusive: bool,
    #[serde(default)]
    pub shared_cli: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct RuntimeState {
    schema: u8,
    lease: Option<RuntimeLease>,
    holders: Vec<RuntimeLease>,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            schema: 2,
            lease: None,
            holders: Vec::new(),
        }
    }
}

impl RuntimeState {
    fn empty(&self) -> bool {
        self.lease.is_none() && self.holders.is_empty()
    }

    fn valid(&self) -> bool {
        self.schema == 2
            && !(self.lease.is_some() && !self.holders.is_empty())
            && self.holders.len() <= CLI_CAPACITY
            && self.holders.iter().enumerate().all(|(index, lease)| {
                !lease.exclusive
                    && lease.shared_cli
                    && !lease.owner.is_empty()
                    && lease.owner.len() <= 160
                    && !lease.generation.is_empty()
                    && !self.holders[..index]
                        .iter()
                        .any(|other| other.owner == lease.owner)
            })
            && self.lease.as_ref().is_none_or(|lease| {
                !lease.shared_cli
                    && !lease.owner.is_empty()
                    && lease.owner.len() <= 160
                    && !lease.generation.is_empty()
            })
    }

    fn replace(&mut self, lease: RuntimeLease) {
        if lease.shared_cli {
            if let Some(current) = self
                .holders
                .iter_mut()
                .find(|item| item.owner == lease.owner)
            {
                *current = lease;
            }
        } else {
            self.lease = Some(lease);
        }
    }
}

/// 原子保护账号 CLI 占用与 API 活跃请求增加的门禁。
pub struct RuntimeGate {
    state: Mutex<RuntimeState>,
    active: Arc<AtomicI64>,
    importing: Arc<AtomicI64>,
    changed: Notify,
    path: Mutex<Option<PathBuf>>,
}

/// 凭据提交期间保持维护门禁，调用方断线也不能提前释放。
pub struct OAuthImportPermit {
    importing: Arc<AtomicI64>,
}

impl Drop for OAuthImportPermit {
    fn drop(&mut self) {
        self.importing.fetch_sub(1, Ordering::SeqCst);
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn exclusive_by_default() -> bool {
    // 旧记录缺少模式时，不能将未完成的重授权当成普通 CLI。
    true
}

impl RuntimeGate {
    /// 创建初始未占用门禁。
    /// @param active 账号 API 请求的共享活动计数。
    /// @return 新门禁。
    pub fn new(active: Arc<AtomicI64>) -> Self {
        Self {
            state: Mutex::new(RuntimeState::default()),
            active,
            importing: Arc::new(AtomicI64::new(0)),
            changed: Notify::new(),
            path: Mutex::new(None),
        }
    }

    /// 关联账号持久文件；任何遗留占用均须按原代次对账。
    /// @param account_id 当前账号 ID。
    /// @return 无返回值。
    pub fn recover(&self, account_id: i64) {
        let Ok(directory) = std::env::var("V3_LEASE_DIR") else {
            return;
        };
        self.recover_path(PathBuf::from(directory).join(format!("{account_id}.json")));
    }

    fn recover_path(&self, path: PathBuf) {
        if path.exists() {
            let bytes = fs::read(&path).unwrap_or_default();
            let restored = from_slice::<RuntimeState>(&bytes)
                .ok()
                .filter(|state| state.valid() && !state.empty())
                .or_else(|| {
                    from_slice::<RuntimeLease>(&bytes)
                        .ok()
                        .map(|lease| RuntimeState {
                            lease: Some(lease),
                            ..RuntimeState::default()
                        })
                        .filter(RuntimeState::valid)
                });
            let mut restored = restored.unwrap_or_else(|| RuntimeState {
                lease: Some(RuntimeLease {
                    owner: "reconcile".into(),
                    generation: Uuid::new_v4().to_string(),
                    expires_at: 0,
                    phase: "reconcile".into(),
                    exclusive: true,
                    shared_cli: false,
                }),
                ..RuntimeState::default()
            });
            if let Some(lease) = restored.lease.as_mut() {
                lease.phase = "reconcile".into();
            }
            for lease in &mut restored.holders {
                lease.phase = "reconcile".into();
            }
            *self.state.lock().unwrap() = restored;
        }
        *self.path.lock().unwrap() = Some(path);
    }

    fn persist(&self, state: &RuntimeState) -> Result<(), AppError> {
        let path_guard = self.path.lock().unwrap();
        let Some(path) = path_guard.as_ref() else {
            return Ok(());
        };
        if !state.empty() {
            let parent = path.parent().unwrap();
            fs::create_dir_all(parent)
                .map_err(|_| AppError::Internal("persist runtime lease".into()))?;
            let temporary = path.with_extension("tmp");
            let bytes =
                to_vec(state).map_err(|_| AppError::Internal("encode runtime lease".into()))?;
            let mut options = OpenOptions::new();
            options.write(true).create(true).truncate(true);
            #[cfg(unix)]
            options.mode(0o600);
            options
                .open(&temporary)
                .and_then(|mut file| {
                    file.write_all(&bytes)?;
                    file.sync_all()
                })
                .and_then(|_| fs::rename(&temporary, path))
                .map_err(|_| AppError::Internal("persist runtime lease".into()))?;
        } else if path.exists() {
            fs::remove_file(path)
                .map_err(|_| AppError::Internal("release runtime lease".into()))?;
        }
        #[cfg(unix)]
        File::open(path.parent().unwrap())
            .and_then(|directory| directory.sync_all())
            .map_err(|_| AppError::Internal("sync runtime lease directory".into()))?;
        Ok(())
    }

    /// 等待维护门禁并原子进入 API 活动区，普通 CLI 不阻塞 API。
    /// @param timeout 最长等待时长。
    /// @return 成功返回 true 并增加 API 活动计数，超时返回 false。
    pub async fn enter(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let state = self.state.lock().unwrap();
                    if state.lease.as_ref().is_none_or(|lease| !lease.exclusive) {
                        self.active.fetch_add(1, Ordering::SeqCst);
                        return;
                    }
                }
                changed.await;
            }
        })
        .await
        .is_ok()
    }

    /// 认领排他维护门禁，随后等待既有 API 排空。
    /// @param owner 环境维护来源。
    /// @return 新建或同来源的既有维护租约。
    pub fn prepare(&self, owner: &str) -> Result<RuntimeLease, AppError> {
        self.prepare_mode(owner, true, false)
    }

    /// 保留旧 CLI 独占语义，允许独立 API；用于捕获及旧调用方。
    /// @param owner 当前 CLI 执行来源。
    /// @return 新建或同来源的既有 CLI 独占租约。
    pub fn prepare_cli(&self, owner: &str) -> Result<RuntimeLease, AppError> {
        self.prepare_mode(owner, false, false)
    }

    /// 认领普通 CLI 有界共享槽位，不与维护或旧独占 CLI 重叠。
    /// @param owner 当前普通 CLI 执行来源。
    /// @return 本来源租约，容量已满或存在待对账占用时拒绝。
    pub fn prepare_shared_cli(&self, owner: &str) -> Result<RuntimeLease, AppError> {
        self.prepare_mode(owner, false, true)
    }

    fn prepare_mode(
        &self,
        owner: &str,
        exclusive: bool,
        shared_cli: bool,
    ) -> Result<RuntimeLease, AppError> {
        if owner.is_empty() || owner.len() > 160 {
            return Err(AppError::BadRequest("invalid runtime owner".into()));
        }
        let mut state = self.state.lock().unwrap();
        let current = state
            .lease
            .as_ref()
            .filter(|lease| lease.owner == owner)
            .or_else(|| state.holders.iter().find(|lease| lease.owner == owner));
        if let Some(lease) = current {
            if lease.exclusive == exclusive
                && lease.shared_cli == shared_cli
                && lease.expires_at > now()
                && lease.phase != "reconcile"
            {
                return Ok(lease.clone());
            }
            return Err(AppError::ServiceUnavailable(
                "account runtime occupied or needs reconciliation".into(),
            ));
        }
        let unavailable = state.lease.is_some()
            || if shared_cli {
                state.holders.len() >= CLI_CAPACITY
                    || state
                        .holders
                        .iter()
                        .any(|lease| lease.expires_at <= now() || lease.phase == "reconcile")
            } else {
                !state.holders.is_empty()
            };
        if unavailable {
            return Err(AppError::ServiceUnavailable(
                "account runtime occupied or needs reconciliation".into(),
            ));
        }
        let lease = RuntimeLease {
            owner: owner.into(),
            generation: Uuid::new_v4().to_string(),
            expires_at: now() + 30,
            phase: if exclusive { "draining" } else { "prepared" }.into(),
            exclusive,
            shared_cli,
        };
        let mut next = state.clone();
        if shared_cli {
            next.holders.push(lease.clone());
        } else {
            next.lease = Some(lease.clone());
        }
        self.persist(&next)?;
        *state = next;
        Ok(lease)
    }

    /// 推进或释放自己的同代租约，不能更新其他 CLI holder。
    /// @param owner 租约来源。
    /// @param generation 租约代次。
    /// @param operation claim、heartbeat、fence 或 release。
    /// @return 当前来源的租约；释放成功返回 None。
    pub fn update(
        &self,
        owner: &str,
        generation: &str,
        operation: &str,
    ) -> Result<Option<RuntimeLease>, AppError> {
        let mut state = self.state.lock().unwrap();
        let lease = state
            .lease
            .as_ref()
            .filter(|lease| lease.owner == owner)
            .or_else(|| state.holders.iter().find(|lease| lease.owner == owner))
            .ok_or_else(|| AppError::BadRequest("runtime lease missing or stale".into()))?;
        if lease.generation != generation {
            return Err(AppError::BadRequest("stale runtime lease".into()));
        }
        let mut next = state.clone();
        if operation == "release" {
            if self.importing.load(Ordering::SeqCst) != 0 {
                return Err(AppError::ServiceUnavailable("凭据提交尚未收口".into()));
            }
            if lease.shared_cli {
                next.holders.retain(|lease| lease.owner != owner);
            } else {
                next.lease = None;
            }
            self.persist(&next)?;
            *state = next;
            self.changed.notify_waiters();
            return Ok(None);
        }
        let mut lease = lease.clone();
        if operation == "fence" && lease.exclusive && lease.owner.starts_with("login:") {
            // 先持久撤销导入许可，迟到 HTTP 不能在取消后继续提交。
            lease.phase = "reconcile".into();
            lease.expires_at = 0;
        } else {
            if lease.expires_at <= now() || lease.phase == "reconcile" {
                return Err(AppError::ServiceUnavailable(
                    "runtime lease needs reconciliation".into(),
                ));
            }
            match operation {
                "claim" if !lease.exclusive || self.active.load(Ordering::SeqCst) == 0 => {
                    lease.phase = "claimed".into()
                }
                "claim" => {
                    return Err(AppError::ServiceUnavailable(
                        "account requests still draining".into(),
                    ));
                }
                "heartbeat" => (),
                _ => return Err(AppError::BadRequest("invalid runtime operation".into())),
            }
            lease.expires_at = now() + 30;
        }
        next.replace(lease.clone());
        self.persist(&next)?;
        *state = next;
        Ok(Some(lease))
    }

    /// 校验登录维护租约并固定提交占用，直到数据库事务结束。
    /// @param owner 原登录来源。
    /// @param generation 原登录代次。
    /// @return 提交期间禁止提前释放维护门禁的句柄。
    pub fn begin_oauth_import(
        &self,
        owner: &str,
        generation: &str,
    ) -> Result<OAuthImportPermit, AppError> {
        let state = self.state.lock().unwrap();
        let lease = state
            .lease
            .as_ref()
            .ok_or_else(|| AppError::BadRequest("登录租约不存在".into()))?;
        if lease.owner != owner
            || lease.generation != generation
            || !lease.exclusive
            || !state.holders.is_empty()
            || !owner.starts_with("login:")
            || lease.phase != "claimed"
            || lease.expires_at <= now()
            || self.active.load(Ordering::SeqCst) != 0
            || self.importing.load(Ordering::SeqCst) != 0
        {
            return Err(AppError::ServiceUnavailable(
                "登录维护租约失效或仍有活动请求".into(),
            ));
        }
        self.importing.fetch_add(1, Ordering::SeqCst);
        Ok(OAuthImportPermit {
            importing: self.importing.clone(),
        })
    }

    /// 返回全部占用；旧 lease 字段仅作非空占用提示。
    /// @return 不含凭据的租约、容量与 API 活动状态。
    pub fn snapshot(&self) -> Value {
        let state = self.state.lock().unwrap();
        let holders: Vec<_> = state
            .lease
            .iter()
            .chain(state.holders.iter())
            .cloned()
            .collect();
        json!({"lease": holders.first(), "holders": holders, "cli_capacity": CLI_CAPACITY,
            "active": self.active.load(Ordering::SeqCst), "importing": self.importing.load(Ordering::SeqCst)})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn two_cli_holders_share_capacity_and_release_only_their_own_slot() {
        let gate = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
        let first = gate.prepare_shared_cli("run:first").unwrap();
        let second = gate.prepare_shared_cli("run:second").unwrap();
        assert_ne!(first.generation, second.generation);
        assert_eq!(gate.snapshot()["holders"].as_array().unwrap().len(), 2);
        assert!(gate.enter(Duration::from_millis(10)).await);
        assert!(gate.prepare_shared_cli("run:third").is_err());
        assert!(gate.prepare_cli("capture:exclusive").is_err());
        assert!(gate.prepare("login:exclusive").is_err());
        assert_eq!(
            gate.prepare_shared_cli(&first.owner).unwrap().generation,
            first.generation
        );
        assert!(
            gate.update(&first.owner, &second.generation, "release")
                .is_err()
        );
        gate.update(&first.owner, &first.generation, "release")
            .unwrap();
        gate.update(&second.owner, &second.generation, "claim")
            .unwrap();
        assert_eq!(gate.snapshot()["holders"].as_array().unwrap().len(), 1);
        assert_eq!(gate.snapshot()["lease"]["owner"], second.owner);
        let third = gate.prepare_shared_cli("run:third").unwrap();
        gate.update(&third.owner, &third.generation, "release")
            .unwrap();
        gate.update(&second.owner, &second.generation, "release")
            .unwrap();
        assert!(gate.snapshot()["lease"].is_null());
    }

    #[tokio::test]
    async fn shared_cli_restart_keeps_each_holder_fenced_until_explicit_release() {
        let path = std::env::temp_dir().join(format!("v3-multiple-{}.json", Uuid::new_v4()));
        let gate = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
        gate.recover_path(path.clone());
        let first = gate.prepare_shared_cli("run:first").unwrap();
        let second = gate.prepare_shared_cli("run:second").unwrap();
        let restarted = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
        restarted.recover_path(path.clone());
        assert_eq!(restarted.snapshot()["holders"].as_array().unwrap().len(), 2);
        assert!(restarted.enter(Duration::from_millis(10)).await);
        assert!(restarted.prepare_shared_cli("run:third").is_err());
        assert!(
            restarted
                .update(&first.owner, &first.generation, "heartbeat")
                .is_err()
        );
        restarted
            .update(&first.owner, &first.generation, "release")
            .unwrap();
        assert!(path.exists());
        assert!(restarted.prepare_shared_cli("run:third").is_err());
        restarted
            .update(&second.owner, &second.generation, "release")
            .unwrap();
        assert!(!path.exists());
        assert!(restarted.prepare_cli("capture:exclusive").is_ok());
    }

    #[tokio::test]
    async fn damaged_shared_state_never_opens_maintenance_admission() {
        let path =
            std::env::temp_dir().join(format!("v3-invalid-multiple-{}.json", Uuid::new_v4()));
        let invalid = json!({"schema":2,"lease":null,"holders":[
            {"owner":"login:wrong","generation":"one","phase":"claimed",
                "expires_at":9999999999_u64,"exclusive":true,"shared_cli":true}]});
        fs::write(&path, to_vec(&invalid).unwrap()).unwrap();
        let gate = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
        gate.recover_path(path.clone());
        assert_eq!(gate.snapshot()["lease"]["exclusive"], true);
        assert!(!gate.enter(Duration::from_millis(5)).await);
        assert!(gate.prepare_shared_cli("run:new").is_err());
        fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn login_import_fences_late_calls_and_prevents_early_release() {
        let gate = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
        let lease = gate.prepare("login:abcdef123456").unwrap();
        gate.update(&lease.owner, &lease.generation, "claim")
            .unwrap();
        let permit = gate
            .begin_oauth_import(&lease.owner, &lease.generation)
            .unwrap();
        gate.update(&lease.owner, &lease.generation, "fence")
            .unwrap();
        assert!(
            gate.begin_oauth_import(&lease.owner, &lease.generation)
                .is_err()
        );
        assert!(
            gate.update(&lease.owner, &lease.generation, "release")
                .is_err()
        );
        assert!(!gate.enter(Duration::from_millis(5)).await);
        drop(permit);
        gate.update(&lease.owner, &lease.generation, "release")
            .unwrap();
        assert!(gate.enter(Duration::from_millis(5)).await);
    }

    #[tokio::test]
    async fn shared_cli_admits_api_and_keeps_cli_serial_even_after_expiry_and_restart() {
        let path = std::env::temp_dir().join(format!("v3-shared-{}.json", Uuid::new_v4()));
        let active = Arc::new(AtomicI64::new(0));
        let gate = RuntimeGate::new(active.clone());
        gate.recover_path(path.clone());
        assert!(gate.enter(Duration::from_millis(10)).await);
        let lease = gate.prepare_cli("run:shared").unwrap();
        assert!(!lease.exclusive);
        gate.update(&lease.owner, &lease.generation, "claim")
            .unwrap();
        assert!(gate.enter(Duration::from_millis(10)).await);
        assert_eq!(active.load(Ordering::SeqCst), 2);
        assert!(gate.prepare_cli("run:other").is_err());
        assert!(gate.prepare(&lease.owner).is_err());
        gate.state
            .lock()
            .unwrap()
            .lease
            .as_mut()
            .unwrap()
            .expires_at = 0;
        assert!(gate.enter(Duration::from_millis(10)).await);
        assert!(gate.prepare_cli(&lease.owner).is_err());
        assert!(
            gate.update(&lease.owner, &lease.generation, "heartbeat")
                .is_err()
        );
        let restarted = RuntimeGate::new(active);
        restarted.recover_path(path.clone());
        assert_eq!(restarted.snapshot()["lease"]["phase"], "reconcile");
        assert!(restarted.enter(Duration::from_millis(10)).await);
        assert!(restarted.prepare_cli("run:other").is_err());
        assert!(
            restarted
                .update(&lease.owner, "old-generation", "release")
                .is_err()
        );
        restarted
            .update(&lease.owner, &lease.generation, "release")
            .unwrap();
        let maintenance = restarted.prepare("maintenance:test").unwrap();
        assert!(maintenance.exclusive);
        assert!(!restarted.enter(Duration::from_millis(10)).await);
        restarted
            .update(&maintenance.owner, &maintenance.generation, "release")
            .unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn legacy_and_unreadable_leases_still_block_api_after_restart() {
        let path = std::env::temp_dir().join(format!("v3-legacy-{}.json", Uuid::new_v4()));
        for record in [
            r#"{"owner":"legacy","generation":"old","expires_at":9999999999,"phase":"claimed"}"#,
            "broken-json",
        ] {
            std::fs::write(&path, record).unwrap();
            let gate = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
            gate.recover_path(path.clone());
            assert_eq!(gate.snapshot()["lease"]["exclusive"], true);
            assert!(!gate.enter(Duration::from_millis(10)).await);
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn expired_lease_and_restart_require_explicit_reconciliation() {
        let path = std::env::temp_dir().join(format!("v3-lease-{}.json", Uuid::new_v4()));
        let gate = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
        gate.recover_path(path.clone());
        let lease = gate.prepare("run:crashed").unwrap();
        gate.state
            .lock()
            .unwrap()
            .lease
            .as_mut()
            .unwrap()
            .expires_at = 0;
        assert!(!gate.enter(Duration::from_millis(10)).await);
        assert!(
            gate.update(&lease.owner, &lease.generation, "heartbeat")
                .is_err()
        );
        let restarted = RuntimeGate::new(Arc::new(AtomicI64::new(0)));
        restarted.recover_path(path.clone());
        assert_eq!(restarted.snapshot()["lease"]["phase"], "reconcile");
        assert!(!restarted.enter(Duration::from_millis(10)).await);
        assert!(restarted.update(&lease.owner, "old", "release").is_err());
        restarted
            .update(&lease.owner, &lease.generation, "release")
            .unwrap();
        assert!(!path.exists());
        assert!(restarted.enter(Duration::from_millis(10)).await);
    }

    #[tokio::test]
    async fn draining_blocks_new_admission_until_explicit_release() {
        let active = Arc::new(AtomicI64::new(0));
        let gate = RuntimeGate::new(active.clone());
        assert!(gate.enter(Duration::from_millis(20)).await);
        let lease = gate.prepare("run:a").unwrap();
        assert!(
            gate.update(&lease.owner, &lease.generation, "claim")
                .is_err()
        );
        active.fetch_sub(1, Ordering::SeqCst);
        gate.update(&lease.owner, &lease.generation, "claim")
            .unwrap();
        assert!(!gate.enter(Duration::from_millis(20)).await);
        assert!(
            gate.update(&lease.owner, "old-generation", "release")
                .is_err()
        );
        gate.update(&lease.owner, &lease.generation, "release")
            .unwrap();
        assert!(gate.enter(Duration::from_millis(20)).await);
    }
}
