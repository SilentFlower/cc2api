//! 长期归属与上游活动许可；交接排空不持有同步锁跨网络等待。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use super::{AccountService, oauth_import_restores_auth, oauth_import_snapshot};
use crate::error::AppError;
use crate::model::account::{Account, AccountAuthType, OAuthCredentialImport};
use crate::model::credential_ownership::{
    CredentialHandoffReceipt, CredentialMaterial, CredentialOwnership, CredentialOwnershipChange,
    CredentialOwnershipInput, CredentialOwnershipSummary,
};

/// 账号短期活动计数，关闭后只允许既有活动收口。
pub(super) struct CredentialGate {
    state: Mutex<(bool, usize)>,
    changed: Notify,
    operation: AsyncMutex<()>,
}

impl CredentialGate {
    fn new() -> Self {
        Self {
            state: Mutex::new((false, 0)),
            changed: Notify::new(),
            operation: AsyncMutex::new(()),
        }
    }

    fn close(&self, closed: bool) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .0 = closed;
        self.changed.notify_waiters();
    }

    fn enter(self: &Arc<Self>) -> Result<CredentialActivity, AppError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.0 {
            return Err(unavailable());
        }
        state.1 += 1;
        Ok(CredentialActivity { gate: self.clone() })
    }

    async fn drained(&self) -> Result<(), AppError> {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .1
                    == 0
                {
                    break;
                }
                notified.await;
            }
        })
        .await
        .map_err(|_| AppError::ServiceUnavailable("账号上游活动仍在收口，请重试原操作".into()))
    }
}

/// 覆盖到实际发送结束的活动许可；丢弃时自动归还排空计数。
pub struct CredentialActivity {
    gate: Arc<CredentialGate>,
}

impl CredentialActivity {
    /// 等待凭据进入排空，供零模型准备优先收口。
    /// @return 门禁关闭后结束等待。
    pub(crate) async fn closed(&self) {
        loop {
            let changed = self.gate.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .gate
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .0
            {
                return;
            }
            changed.await;
        }
    }
}

impl Drop for CredentialActivity {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.1 = state.1.saturating_sub(1);
        self.gate.changed.notify_waiters();
    }
}

fn unavailable() -> AppError {
    AppError::ServiceUnavailable("账号凭据由本地管理或正在交接/重授权，请先完成核对".into())
}

fn hex_id(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
}

fn request_hash(input: &CredentialOwnershipInput) -> Result<String, AppError> {
    let encoded = serde_json::to_vec(&json!([
        input.protocol,
        input.operation_id,
        input.direction,
        input.identity,
        input.expected_generation,
        input.source_refresh_sha256,
        input.lease_owner
    ]))
    .map_err(|_| AppError::BadRequest("凭据交接证明无效".into()))?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn validate_input(input: &CredentialOwnershipInput, account: &Account) -> Result<(), AppError> {
    let identity = &input.identity;
    let same = |left: &str, right: &str| left.trim().eq_ignore_ascii_case(right.trim());
    if input.protocol != 1
        || !hex_id(&input.operation_id, 32)
        || !matches!(
            input.action.as_str(),
            "prepare" | "commit" | "finish" | "cancel" | "status"
        )
        || !matches!(
            input.direction.as_str(),
            "create" | "attach" | "return" | "authorize" | "detach" | "reauth" | "retire"
        )
        || identity.deployment_id.is_empty()
        || identity.deployment_id.len() > 36
        || !identity
            .deployment_id
            .bytes()
            .all(|value| value.is_ascii_lowercase() || value.is_ascii_digit() || value == b'-')
        || !hex_id(&identity.environment_id, 32)
        || identity.bench_account_id <= 0
        || identity.device_id != account.device_id
        || !hex_id(&identity.device_id, 64)
        || account.auth_type != AccountAuthType::Oauth
        || identity.email.is_empty()
        || identity.email.len() > 320
        || !same(&identity.email, &account.email)
        || identity.account_uuid.is_empty()
        || !account
            .account_uuid
            .as_deref()
            .is_some_and(|value| same(value, &identity.account_uuid))
        || account
            .organization_uuid
            .as_deref()
            .filter(|value| !value.is_empty())
            .is_some_and(|value| {
                !identity
                    .organization_uuid
                    .as_deref()
                    .is_some_and(|actual| same(value, actual))
            })
        || input
            .expected_generation
            .as_ref()
            .is_some_and(|value| !hex_id(value, 32))
        || input
            .source_refresh_sha256
            .as_ref()
            .is_some_and(|value| !hex_id(value, 64))
        || (input.direction == "reauth"
            && !input
                .lease_owner
                .as_deref()
                .is_some_and(|value| value.starts_with("login:") && value.len() <= 160))
    {
        return Err(AppError::BadRequest(
            "凭据交接的协议、账号或环境身份不匹配".into(),
        ));
    }
    Ok(())
}

fn validate_material(material: &CredentialMaterial) -> Result<(), AppError> {
    if material.expires_at <= Utc::now().timestamp_millis() + 60_000
        || [&material.access_token, &material.refresh_token]
            .iter()
            .any(|token| {
                token.is_empty()
                    || token.len() > 16384
                    || token.bytes().any(|value| value <= 32 || value >= 127)
            })
    {
        return Err(AppError::BadRequest(
            "本地回交凭据已过期或格式不合法".into(),
        ));
    }
    Ok(())
}

/// 生成创建事务的初始关闭状态。@param account 新账号。@param input 归属证明。
/// @return 尚未开放网关的新建交接记录。
pub(super) fn creation_change(
    account: &Account,
    input: &CredentialOwnershipInput,
) -> Result<CredentialOwnershipChange, AppError> {
    validate_input(input, account)?;
    if input.action != "prepare"
        || input.direction != "create"
        || input.expected_generation.is_some()
        || input.source_refresh_sha256.as_deref()
            != Some(&hex::encode(Sha256::digest(
                account.refresh_token.as_bytes(),
            )))
    {
        return Err(AppError::BadRequest(
            "新账号缺少完整的本地凭据交接证明".into(),
        ));
    }
    let next = CredentialOwnership {
        protocol: 1,
        account_id: account.id,
        identity: input.identity.clone(),
        owner: "local".into(),
        phase: "handoff".into(),
        generation: input.operation_id.clone(),
        operation_id: input.operation_id.clone(),
        direction: input.direction.clone(),
        lease_owner: None,
    };
    let receipt = CredentialHandoffReceipt {
        operation_id: input.operation_id.clone(),
        account_id: account.id,
        identity: input.identity.clone(),
        direction: input.direction.clone(),
        request_hash: request_hash(input)?,
        source_owner: "local".into(),
        source_phase: "ready".into(),
        generation: input.operation_id.clone(),
        stage: "prepared".into(),
        material_sha256: None,
        import_operation_id: None,
    };
    Ok(CredentialOwnershipChange {
        expected: None,
        next,
        receipt,
    })
}

impl AccountService {
    /// 刷新发送前记录待确认阶段，进程中断后不能自行重用旧 RT。
    /// @param id 原账号。@return 有受管归属时的恢复变更，无记录账号维持旧行为。
    pub(super) async fn begin_owned_refresh(
        &self,
        id: i64,
    ) -> Result<Option<CredentialOwnershipChange>, AppError> {
        let Some(current) = self.store.credential_ownership(id).await? else {
            return Ok(None);
        };
        if !current.gateway_ready() {
            return Err(unavailable());
        }
        let receipt = self
            .store
            .credential_handoff_receipt(&current.operation_id)
            .await?
            .ok_or_else(unavailable)?;
        let mut pending = current.clone();
        pending.phase = "reconcile".into();
        self.store
            .change_credential_ownership(&CredentialOwnershipChange {
                expected: Some(current.clone()),
                next: pending.clone(),
                receipt: receipt.clone(),
            })
            .await?;
        self.credential_gate(id).close(true);
        Ok(Some(CredentialOwnershipChange {
            expected: Some(pending),
            next: current,
            receipt,
        }))
    }

    /// 恢复已经取得确定结果的原刷新，后续交接的 CAS 冲突不能放开门禁。
    /// @param change 原刷新待确认状态。@return 持久恢复结果。
    pub(super) async fn finish_owned_refresh(
        &self,
        change: &CredentialOwnershipChange,
    ) -> Result<(), AppError> {
        self.store.change_credential_ownership(change).await?;
        self.credential_gate(change.next.account_id).close(false);
        self.notify_native_change(change.next.account_id, true);
        Ok(())
    }
    fn credential_gate(&self, id: i64) -> Arc<CredentialGate> {
        self.credential_gates
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(id)
            .or_insert_with(|| Arc::new(CredentialGate::new()))
            .clone()
    }

    /// 核对长期归属及维护门禁并登记上游活动。
    /// @param id 账号。@return 必须持有到发送结束的许可。
    pub async fn upstream_activity(&self, id: i64) -> Result<CredentialActivity, AppError> {
        let permit = self.credential_gate(id).enter()?;
        if self
            .store
            .credential_ownership(id)
            .await?
            .is_some_and(|value| !value.gateway_ready())
        {
            return Err(unavailable());
        }
        let account = self.store.get_by_id(id).await?;
        let queue = self.get_or_create_queue(id, account.concurrency).await;
        let snapshot = queue.runtime_gate.snapshot();
        if snapshot["lease"].as_object().is_some_and(|lease| {
            lease
                .get("exclusive")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        }) {
            return Err(unavailable());
        }
        Ok(permit)
    }

    /// 保护零模型切换预热期间的凭据归属；调用方另行核对内核维护租约。
    /// @param id 当前账号。
    /// @return 凭据归属许可，不进入上游请求或刷新路径。
    pub(crate) async fn native_kernel_credentials(
        &self,
        id: i64,
    ) -> Result<CredentialActivity, AppError> {
        let permit = self.credential_gate(id).enter()?;
        if self
            .store
            .credential_ownership(id)
            .await?
            .is_some_and(|value| !value.gateway_ready())
        {
            return Err(unavailable());
        }
        Ok(permit)
    }

    /// 批量提供不含令牌的凭据归属。@return 管理列表及调度使用的记录。
    pub async fn credential_ownerships(
        &self,
    ) -> Result<HashMap<i64, CredentialOwnershipSummary>, AppError> {
        self.store.credential_ownerships().await
    }

    /// 查询受管状态且不解析令牌。@param id 账号。@return 归属或 None。
    pub async fn credential_ownership(
        &self,
        id: i64,
    ) -> Result<Option<CredentialOwnership>, AppError> {
        self.store.get_by_id(id).await?;
        self.store.credential_ownership(id).await
    }

    /// 受信任交接入口；同操作重试查询已提交事实，不重复导入旧凭据。
    /// @param id 原账号。@param input 同代操作证明。@return 回执及受限凭据快照。
    pub async fn credential_handoff(
        &self,
        id: i64,
        input: &CredentialOwnershipInput,
    ) -> Result<Value, AppError> {
        validate_input(input, &self.store.get_by_id(id).await?)?;
        let hash = request_hash(input)?;
        let gate = self.credential_gate(id);
        let _operation = gate.operation.lock().await;
        let mut ownership = self.store.credential_ownership(id).await?;
        let mut receipt = self
            .store
            .credential_handoff_receipt(&input.operation_id)
            .await?;
        if let Some(record) = &receipt {
            if record.account_id != id
                || record.identity != input.identity
                || record.request_hash != hash
            {
                return Err(AppError::BadRequest("凭据交接操作 ID 冲突".into()));
            }
            if input.action == "commit"
                && matches!(input.direction.as_str(), "return" | "authorize")
            {
                if let (Some(material), Some(expected)) =
                    (&input.credentials, &record.material_sha256)
                {
                    let actual = hex::encode(Sha256::digest(
                        serde_json::to_vec(material)
                            .map_err(|_| AppError::BadRequest("回交凭据格式无效".into()))?,
                    ));
                    if !bool::from(actual.as_bytes().ct_eq(expected.as_bytes())) {
                        return Err(AppError::BadRequest("回交操作的凭据摘要冲突".into()));
                    }
                }
            }
        }
        if input.action == "prepare" && receipt.is_none() {
            let change = self
                .prepare_credential_handoff(id, input, ownership, hash)
                .await?;
            gate.close(true);
            self.store.change_credential_ownership(&change).await?;
            ownership = Some(change.next);
            receipt = Some(change.receipt);
        }
        let Some(mut record) = receipt else {
            return Ok(
                json!({"protocol":1,"ownership":ownership,"receipt":null,"credentials":null}),
            );
        };
        let current = ownership.as_ref().ok_or_else(unavailable)?;
        if current.identity != input.identity {
            return Err(unavailable());
        }
        // 历史回执不能重新开放后续代次；重试只恢复当前操作已提交的最终状态。
        if current.operation_id != input.operation_id
            || matches!(record.stage.as_str(), "committed" | "cancelled")
        {
            if current.operation_id == input.operation_id {
                gate.close(!current.gateway_ready());
                if current.gateway_ready() {
                    self.notify_native_change(id, true);
                }
            }
            let snapshot = if current.operation_id == input.operation_id
                && current.gateway_ready()
                && record.stage == "committed"
            {
                let value = oauth_import_snapshot(self.store.get_by_id(id).await?)?;
                Some(
                    json!({"account_id":value.account_id,"access_token":value.access_token,
                    "refresh_token":value.refresh_token,"expires_at":value.expires_at}),
                )
            } else {
                None
            };
            return Ok(
                json!({"protocol":1,"ownership":ownership,"receipt":record,"credentials":snapshot}),
            );
        }
        gate.close(true);
        gate.drained().await?;
        let mut next = current.clone();
        if input.action == "commit" && record.stage == "prepared" {
            if input.direction == "reauth" {
                return Err(AppError::BadRequest(
                    "重新授权必须使用原登录提交入口".into(),
                ));
            }
            if matches!(input.direction.as_str(), "detach" | "retire") {
                next.owner = "local".into();
                next.phase = "ready".into();
                record.stage = "committed".into();
            } else {
                record.stage = "imported".into();
            }
            let mut change = CredentialOwnershipChange {
                expected: ownership.clone(),
                next: next.clone(),
                receipt: record.clone(),
            };
            if matches!(input.direction.as_str(), "return" | "authorize") {
                self.import_returned_credentials(id, input, &mut change)
                    .await?;
                record = change.receipt;
            } else {
                self.store.change_credential_ownership(&change).await?;
            }
            ownership = Some(next.clone());
        } else if input.action == "finish" && record.stage == "imported" {
            next.owner = "gateway".into();
            next.phase = "ready".into();
            record.stage = "committed".into();
            self.store
                .change_credential_ownership(&CredentialOwnershipChange {
                    expected: ownership.clone(),
                    next: next.clone(),
                    receipt: record.clone(),
                })
                .await?;
            ownership = Some(next.clone());
            gate.close(false);
            self.notify_native_change(id, true);
        } else if input.action == "cancel"
            && record.stage == "prepared"
            && matches!(input.direction.as_str(), "return" | "reauth")
        {
            next.owner = record.source_owner.clone();
            next.phase = record.source_phase.clone();
            record.stage = "cancelled".into();
            self.store
                .change_credential_ownership(&CredentialOwnershipChange {
                    expected: ownership.clone(),
                    next: next.clone(),
                    receipt: record.clone(),
                })
                .await?;
            ownership = Some(next.clone());
            gate.close(!next.gateway_ready());
            if next.gateway_ready() {
                self.notify_native_change(id, true);
            }
        }
        let snapshot = if matches!(record.stage.as_str(), "prepared" | "imported")
            && input.direction != "retire"
            && (input.direction != "reauth" || record.stage == "imported")
            && (!matches!(input.direction.as_str(), "return" | "authorize")
                || record.stage == "imported")
        {
            Some(oauth_import_snapshot(self.store.get_by_id(id).await?)?)
        } else {
            None
        };
        Ok(
            json!({"protocol":1,"ownership":ownership,"receipt":record,"credentials":snapshot.map(|value|
            json!({"account_id":value.account_id,"access_token":value.access_token,
                "refresh_token":value.refresh_token,"expires_at":value.expires_at}))}),
        )
    }

    async fn prepare_credential_handoff(
        &self,
        id: i64,
        input: &CredentialOwnershipInput,
        ownership: Option<CredentialOwnership>,
        hash: String,
    ) -> Result<CredentialOwnershipChange, AppError> {
        if let Some(current) = &ownership {
            if current.identity != input.identity
                || (current.phase != "ready"
                    && !(current.phase == "reconcile" && input.direction == "reauth"))
                || input.expected_generation.as_deref() != Some(current.generation.as_str())
            {
                return Err(AppError::ServiceUnavailable(
                    "原凭据归属或代次不匹配，需要核对".into(),
                ));
            }
        } else if input.expected_generation.is_some()
            || matches!(input.direction.as_str(), "return" | "create")
        {
            return Err(AppError::BadRequest(
                "缺少已确认的原凭据归属，请先核对或重新授权".into(),
            ));
        }
        let source = ownership
            .as_ref()
            .map(|value| value.owner.clone())
            .unwrap_or_else(|| "gateway".into());
        if (input.direction == "return" && source != "local")
            || (input.direction != "return" && source != "gateway")
        {
            return Err(unavailable());
        }
        if input.direction == "attach" {
            let account = self.store.get_by_id(id).await?;
            let expected = hex::encode(Sha256::digest(account.refresh_token.as_bytes()));
            if !input
                .source_refresh_sha256
                .as_deref()
                .is_some_and(|value| bool::from(value.as_bytes().ct_eq(expected.as_bytes())))
            {
                return Err(AppError::BadRequest(
                    "两端刷新凭据来源不能确认，请先核对或重新授权".into(),
                ));
            }
        }
        let next = CredentialOwnership {
            protocol: 1,
            account_id: id,
            identity: input.identity.clone(),
            owner: source.clone(),
            phase: if input.direction == "reauth" {
                "reauth"
            } else {
                "handoff"
            }
            .into(),
            generation: input.operation_id.clone(),
            operation_id: input.operation_id.clone(),
            direction: input.direction.clone(),
            lease_owner: input.lease_owner.clone(),
        };
        let receipt = CredentialHandoffReceipt {
            operation_id: input.operation_id.clone(),
            account_id: id,
            identity: input.identity.clone(),
            direction: input.direction.clone(),
            request_hash: hash,
            source_owner: source,
            source_phase: ownership
                .as_ref()
                .map(|value| value.phase.clone())
                .unwrap_or_else(|| "ready".into()),
            generation: input.operation_id.clone(),
            stage: "prepared".into(),
            material_sha256: None,
            import_operation_id: None,
        };
        Ok(CredentialOwnershipChange {
            expected: ownership,
            next,
            receipt,
        })
    }

    async fn import_returned_credentials(
        &self,
        id: i64,
        input: &CredentialOwnershipInput,
        change: &mut CredentialOwnershipChange,
    ) -> Result<(), AppError> {
        let material = input
            .credentials
            .as_ref()
            .ok_or_else(|| AppError::BadRequest("回交缺少本地当前凭据".into()))?;
        validate_material(material)?;
        if input.direction == "authorize" {
            let hash = hex::encode(Sha256::digest(material.refresh_token.as_bytes()));
            if !input
                .source_refresh_sha256
                .as_deref()
                .is_some_and(|expected| bool::from(expected.as_bytes().ct_eq(hash.as_bytes())))
            {
                return Err(AppError::BadRequest(
                    "新授权凭据与受信任来源证明不匹配".into(),
                ));
            }
        }
        let material_hash = hex::encode(Sha256::digest(
            serde_json::to_vec(material)
                .map_err(|_| AppError::BadRequest("回交凭据格式无效".into()))?,
        ));
        change.receipt.material_sha256 = Some(material_hash.clone());
        let payload = OAuthCredentialImport {
            operation_id: input.operation_id.clone(),
            owner: format!("handoff:{}", input.operation_id),
            generation: change.receipt.generation.clone(),
            email: input.identity.email.clone(),
            account_uuid: Some(input.identity.account_uuid.clone()),
            organization_uuid: input.identity.organization_uuid.clone(),
            access_token: material.access_token.clone(),
            refresh_token: material.refresh_token.clone(),
            expires_at: material.expires_at,
            subscription_type: None,
            rate_limit_tier: None,
        };
        let (key, owner) = self.acquire_oauth_credential_lock(id).await?;
        let result = async {
            let latest = self.store.get_by_id(id).await?;
            let restore_auth = oauth_import_restores_auth(&latest);
            self.store
                .import_oauth_credentials_owned(
                    &latest,
                    &payload,
                    &material_hash,
                    restore_auth,
                    Some(change),
                )
                .await
        }
        .await;
        self.cache.release_lock(&key, &owner).await;
        result
    }

    /// 将原登录维护与凭据导入放入同一事务，未知归属不得走旧导入旁路。
    /// @param id 原账号。@param input 原登录提交。@return 需要原子保存的归属回执。
    pub(super) async fn reauth_ownership_change(
        &self,
        id: i64,
        input: &OAuthCredentialImport,
    ) -> Result<Option<CredentialOwnershipChange>, AppError> {
        let Some(current) = self.store.credential_ownership(id).await? else {
            return Ok(None);
        };
        if current.owner != "gateway"
            || current.phase != "reauth"
            || current.lease_owner.as_deref() != Some(input.owner.as_str())
        {
            return Err(unavailable());
        }
        let mut receipt = self
            .store
            .credential_handoff_receipt(&current.operation_id)
            .await?
            .ok_or_else(unavailable)?;
        if receipt.stage != "prepared" {
            return Err(unavailable());
        }
        receipt.stage = "imported".into();
        receipt.import_operation_id = Some(input.operation_id.clone());
        Ok(Some(CredentialOwnershipChange {
            expected: Some(current.clone()),
            next: current,
            receipt,
        }))
    }
}
