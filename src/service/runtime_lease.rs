//! CLI 串行租约与环境维护门禁；共享 CLI 不阻止独立 API 入场。

use std::fs::{self, File, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::error::AppError;

/// 持久执行租约；排他维护失效时仍阻止新的推理入场。
#[derive(Clone, Deserialize, Serialize)]
pub struct RuntimeLease {
    pub owner: String,
    pub generation: String,
    pub expires_at: u64,
    pub phase: String,
    #[serde(default = "exclusive_by_default")]
    pub exclusive: bool,
}

/// 共享账号入场锁，同时保护占用状态和活跃请求计数的增加。
pub struct RuntimeGate {
    state: Mutex<Option<RuntimeLease>>,
    active: Arc<AtomicI64>,
    importing: Arc<AtomicI64>,
    changed: Notify,
    path: Mutex<Option<PathBuf>>,
}

/// 凭据提交持有期间禁止释放维护门禁，即使调用方已经断网。
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
    // 旧记录没有模式，不能把未收口的重授权或维护误认为普通 CLI。
    true
}

impl RuntimeGate {
    /// 创建初始未占用门禁。
    /// @param active 账号请求的共享活动计数。
    /// @return 新门禁。
    pub fn new(active: Arc<AtomicI64>) -> Self {
        Self {
            state: Mutex::new(None),
            active,
            importing: Arc::new(AtomicI64::new(0)),
            changed: Notify::new(),
            path: Mutex::new(None),
        }
    }

    /// 关联账号持久文件；残留或不可读租约一律需要对账。
    /// @param account_id 当前账号 ID。
    /// @return 无返回值。
    pub fn recover(&self, account_id: i64) {
        let Ok(directory) = std::env::var("V3_LEASE_DIR") else {
            return;
        };
        let path = PathBuf::from(directory).join(format!("{account_id}.json"));
        self.recover_path(path);
    }

    fn recover_path(&self, path: PathBuf) {
        if path.exists() {
            let lease = std::fs::read(&path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<RuntimeLease>(&bytes).ok());
            let mut lease = lease.unwrap_or(RuntimeLease {
                owner: "reconcile".into(),
                generation: Uuid::new_v4().to_string(),
                expires_at: 0,
                phase: "reconcile".into(),
                exclusive: true,
            });
            lease.phase = "reconcile".into();
            *self.state.lock().unwrap() = Some(lease);
        }
        *self.path.lock().unwrap() = Some(path);
    }

    fn persist(&self, lease: Option<&RuntimeLease>) -> Result<(), AppError> {
        let path_guard = self.path.lock().unwrap();
        let Some(path) = path_guard.as_ref() else {
            return Ok(());
        };
        if let Some(lease) = lease {
            let parent = path.parent().unwrap();
            std::fs::create_dir_all(parent)
                .map_err(|_| AppError::Internal("persist runtime lease".into()))?;
            let temporary = path.with_extension("tmp");
            let bytes = serde_json::to_vec(lease)
                .map_err(|_| AppError::Internal("encode runtime lease".into()))?;
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
            std::fs::remove_file(path)
                .map_err(|_| AppError::Internal("release runtime lease".into()))?;
        }
        #[cfg(unix)]
        File::open(path.parent().unwrap())
            .and_then(|directory| directory.sync_all())
            .map_err(|_| AppError::Internal("sync runtime lease directory".into()))?;
        Ok(())
    }

    /// 等待门禁并原子进入活动区，不允许 prepare 在检查和增加计数之间插入。
    /// @param timeout 最长等待时长。
    /// @return 超时返回 false，成功返回 true 并增加活动计数。
    pub async fn enter(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                {
                    let state = self.state.lock().unwrap();
                    if state.as_ref().is_none_or(|lease| !lease.exclusive) {
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

    /// 设置 draining 后等待旧活动请求结束；调用方负责后续 claim。
    /// @param owner 当前环境维护来源。
    /// @return 新建或同来源既有租约；不同来源返回占用冲突。
    pub fn prepare(&self, owner: &str) -> Result<RuntimeLease, AppError> {
        self.prepare_mode(owner, true)
    }

    /// 领取只约束 CLI 串行的共享租约，不排空或阻止 API。
    /// @param owner 当前 CLI 执行来源。
    /// @return 新建或同来源同模式的既有租约。
    pub fn prepare_cli(&self, owner: &str) -> Result<RuntimeLease, AppError> {
        self.prepare_mode(owner, false)
    }

    fn prepare_mode(&self, owner: &str, exclusive: bool) -> Result<RuntimeLease, AppError> {
        if owner.is_empty() || owner.len() > 160 {
            return Err(AppError::BadRequest("invalid runtime owner".into()));
        }
        let mut state = self.state.lock().unwrap();
        if let Some(lease) = state.as_ref() {
            if lease.owner == owner
                && lease.exclusive == exclusive
                && lease.expires_at > now()
                && lease.phase != "reconcile"
            {
                return Ok(lease.clone());
            }
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
        };
        self.persist(Some(&lease))?;
        *state = Some(lease.clone());
        Ok(lease)
    }

    /// 推进、续期或释放同代租约；release 只在执行已收口后由受信任调度器调用。
    /// @param owner 租约来源。
    /// @param generation 租约代次。
    /// @param operation claim、heartbeat、fence 或 release。
    /// @return 当前租约；释放成功返回 None。
    pub fn update(
        &self,
        owner: &str,
        generation: &str,
        operation: &str,
    ) -> Result<Option<RuntimeLease>, AppError> {
        let mut state = self.state.lock().unwrap();
        let lease = state
            .as_ref()
            .ok_or_else(|| AppError::BadRequest("runtime lease missing".into()))?;
        if lease.owner != owner || lease.generation != generation {
            return Err(AppError::BadRequest("stale runtime lease".into()));
        }
        if operation == "release" {
            if self.importing.load(Ordering::SeqCst) != 0 {
                return Err(AppError::ServiceUnavailable("凭据提交尚未收口".into()));
            }
            self.persist(None)?;
            *state = None;
            self.changed.notify_waiters();
            return Ok(None);
        }
        if operation == "fence" && lease.exclusive && lease.owner.starts_with("login:") {
            // 先撤销新导入权限再查询回执，防止迟到的 HTTP 请求在取消后写入。
            let mut lease = lease.clone();
            lease.phase = "reconcile".into();
            lease.expires_at = 0;
            self.persist(Some(&lease))?;
            *state = Some(lease.clone());
            return Ok(Some(lease));
        }
        if lease.expires_at <= now() || lease.phase == "reconcile" {
            return Err(AppError::ServiceUnavailable(
                "runtime lease needs reconciliation".into(),
            ));
        }
        let mut lease = lease.clone();
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
        self.persist(Some(&lease))?;
        *state = Some(lease.clone());
        Ok(Some(lease))
    }

    /// 校验登录租约并固定提交占用，直到事务结束。
    /// @param owner 原登录来源。
    /// @param generation 原登录代次。
    /// @return 保证提交期间维护门禁不能释放的句柄。
    pub fn begin_oauth_import(
        &self,
        owner: &str,
        generation: &str,
    ) -> Result<OAuthImportPermit, AppError> {
        let state = self.state.lock().unwrap();
        let lease = state
            .as_ref()
            .ok_or_else(|| AppError::BadRequest("登录租约不存在".into()))?;
        if lease.owner != owner
            || lease.generation != generation
            || !lease.exclusive
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

    /// 查询租约与活动数，供收口和重启对账使用。
    /// @return 不含凭据的运行状态。
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"lease": self.state.lock().unwrap().clone(),
            "active": self.active.load(Ordering::SeqCst), "importing": self.importing.load(Ordering::SeqCst)})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        gate.state.lock().unwrap().as_mut().unwrap().expires_at = 0;
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
        gate.state.lock().unwrap().as_mut().unwrap().expires_at = 0;
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
