//! 归属记录和交接回执的 SQLite/PostgreSQL 条件事务。

use std::collections::HashMap;

use sqlx::{AnyConnection, Row};

use super::AccountStore;
use crate::error::AppError;
use crate::model::credential_ownership::{
    CredentialHandoffReceipt, CredentialOwnership, CredentialOwnershipChange,
    CredentialOwnershipSummary,
};

fn decode_ownership(record: &str, id: i64) -> Result<CredentialOwnership, AppError> {
    let value: CredentialOwnership = serde_json::from_str(record)
        .map_err(|_| AppError::ServiceUnavailable("凭据归属记录损坏，需要核对".into()))?;
    let hexadecimal = |text: &str, length: usize| {
        text.len() == length
            && text
                .bytes()
                .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
    };
    if value.protocol != 1
        || value.account_id != id
        || value.identity.bench_account_id <= 0
        || value.identity.deployment_id.is_empty()
        || value.identity.email.is_empty()
        || value.identity.account_uuid.is_empty()
        || !hexadecimal(&value.identity.environment_id, 32)
        || !hexadecimal(&value.identity.device_id, 64)
        || !hexadecimal(&value.operation_id, 32)
        || value.generation != value.operation_id
        || !matches!(value.owner.as_str(), "gateway" | "local")
        || !matches!(
            value.phase.as_str(),
            "ready" | "handoff" | "reauth" | "reconcile"
        )
        || !matches!(
            value.direction.as_str(),
            "create" | "attach" | "return" | "authorize" | "detach" | "retire" | "reauth"
        )
    {
        return Err(AppError::ServiceUnavailable(
            "凭据归属记录损坏，需要核对".into(),
        ));
    }
    Ok(value)
}

impl AccountStore {
    /// 读取账号持久归属。@param id 账号。@return 无记录时沿用非受管语义。
    pub async fn credential_ownership(
        &self,
        id: i64,
    ) -> Result<Option<CredentialOwnership>, AppError> {
        let row =
            sqlx::query("SELECT record_json FROM oauth_account_ownerships WHERE account_id=$1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        row.map(|row| {
            let record: String = row.try_get("record_json")?;
            decode_ownership(&record, id)
        })
        .transpose()
    }

    /// 一次读取受管账号摘要，避免列表和选号逐账号查询。@return 按账号索引的归属。
    pub async fn credential_ownerships(
        &self,
    ) -> Result<HashMap<i64, CredentialOwnershipSummary>, AppError> {
        let rows = sqlx::query("SELECT account_id,record_json FROM oauth_account_ownerships")
            .fetch_all(&self.pool)
            .await?;
        let mut result = HashMap::new();
        for row in rows {
            let id: i64 = row.try_get("account_id")?;
            let record: String = row.try_get("record_json")?;
            let value = match decode_ownership(&record, id) {
                Ok(value) => CredentialOwnershipSummary {
                    protocol: value.protocol,
                    owner: value.owner,
                    phase: value.phase,
                },
                Err(_) => CredentialOwnershipSummary {
                    protocol: 0,
                    owner: "unknown".into(),
                    phase: "reconcile".into(),
                },
            };
            result.insert(id, value);
        }
        Ok(result)
    }

    /// 查询单次操作的无令牌回执。@param operation_id 操作。@return 已知阶段或 None。
    pub async fn credential_handoff_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<CredentialHandoffReceipt>, AppError> {
        let row = sqlx::query(
            "SELECT account_id,record_json FROM oauth_handoff_operations WHERE operation_id=$1",
        )
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let record: String = row.try_get("record_json")?;
            let value: CredentialHandoffReceipt = serde_json::from_str(&record)
                .map_err(|_| AppError::ServiceUnavailable("凭据交接回执损坏，需要核对".into()))?;
            if value.account_id != row.try_get::<i64, _>("account_id")?
                || value.operation_id != operation_id
                || value.generation != operation_id
                || !matches!(
                    value.stage.as_str(),
                    "prepared" | "imported" | "committed" | "cancelled"
                )
            {
                return Err(AppError::ServiceUnavailable(
                    "凭据交接回执损坏，需要核对".into(),
                ));
            }
            Ok(value)
        })
        .transpose()
    }

    /// 原子切换归属并保存回执。@param change 原状态及目标。@return 条件失配时拒绝。
    pub async fn change_credential_ownership(
        &self,
        change: &CredentialOwnershipChange,
    ) -> Result<(), AppError> {
        let mut transaction = self.pool.begin().await?;
        apply_ownership_change(&mut transaction, change).await?;
        transaction.commit().await?;
        Ok(())
    }
}

/// 在调用方事务内保存归属/回执，支持与凭据导入或账号创建同事务提交。
/// @param connection 当前数据库事务。@param change 完整条件变更。@return 提交前校验结果。
pub(super) async fn apply_ownership_change(
    connection: &mut AnyConnection,
    change: &CredentialOwnershipChange,
) -> Result<(), AppError> {
    let encoded = serde_json::to_string(&change.next)
        .map_err(|_| AppError::Internal("编码凭据归属失败".into()))?;
    let updated = if let Some(expected) = &change.expected {
        let original = serde_json::to_string(expected)
            .map_err(|_| AppError::Internal("编码原凭据归属失败".into()))?;
        sqlx::query("UPDATE oauth_account_ownerships SET record_json=$1 WHERE account_id=$2 AND record_json=$3")
            .bind(&encoded).bind(change.next.account_id).bind(original)
            .execute(&mut *connection).await?.rows_affected()
    } else {
        sqlx::query("INSERT INTO oauth_account_ownerships(account_id,record_json) VALUES($1,$2) ON CONFLICT(account_id) DO NOTHING")
            .bind(change.next.account_id).bind(&encoded)
            .execute(&mut *connection).await?.rows_affected()
    };
    if updated != 1 {
        return Err(AppError::ServiceUnavailable(
            "凭据归属已变化，请核对当前操作".into(),
        ));
    }
    let previous =
        sqlx::query("SELECT record_json FROM oauth_handoff_operations WHERE operation_id=$1")
            .bind(&change.receipt.operation_id)
            .fetch_optional(&mut *connection)
            .await?;
    if let Some(previous) = previous {
        let original: String = previous.try_get("record_json")?;
        let receipt: CredentialHandoffReceipt = serde_json::from_str(&original)
            .map_err(|_| AppError::ServiceUnavailable("凭据交接回执损坏".into()))?;
        if receipt.account_id != change.receipt.account_id
            || receipt.identity != change.receipt.identity
            || receipt.request_hash != change.receipt.request_hash
            || receipt.generation != change.receipt.generation
        {
            return Err(AppError::BadRequest("凭据交接操作 ID 冲突".into()));
        }
    }
    let receipt = serde_json::to_string(&change.receipt)
        .map_err(|_| AppError::Internal("编码凭据回执失败".into()))?;
    sqlx::query("INSERT INTO oauth_handoff_operations(operation_id,account_id,record_json) VALUES($1,$2,$3) ON CONFLICT(operation_id) DO UPDATE SET record_json=excluded.record_json")
        .bind(&change.receipt.operation_id).bind(change.next.account_id).bind(receipt)
        .execute(&mut *connection).await?;
    Ok(())
}
