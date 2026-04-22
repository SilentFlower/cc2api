use serde::Serialize;
use sqlx::AnyPool;
use sqlx::Row;

use crate::error::AppError;

/// 单条峰值预热调用记录,用于在设置页展示最近几次预热的结果。
#[derive(Debug, Clone, Serialize)]
pub struct PrimeLogEntry {
    pub id: i64,
    pub account_id: i64,
    pub account_name: String,
    /// 本地时间 ISO8601 字符串,与账号表的 created_at 风格保持一致。
    pub triggered_at: String,
    /// 触发预热的小时(0-23),便于前端分组展示。
    pub hour: i32,
    pub model: String,
    pub success: bool,
    pub error_message: String,
    pub duration_ms: i64,
}

/// 峰值预热日志存储,仅支持追加写入和读取最近 N 条。
pub struct PrimeLogStore {
    pool: AnyPool,
}

/// 保留在 DB 中的最大日志条数,避免历史记录无限膨胀。
const MAX_RETAINED_ROWS: i64 = 200;

impl PrimeLogStore {
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }

    /// 追加一条预热记录,并顺带裁剪,保留最近 MAX_RETAINED_ROWS 行。
    /// 裁剪失败仅记日志,不影响主写入流程。
    pub async fn insert(&self, entry: &PrimeLogEntry) -> Result<(), AppError> {
        sqlx::query(
            "INSERT INTO prime_logs \
             (account_id, account_name, triggered_at, hour, model, success, error_message, duration_ms) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(entry.account_id)
        .bind(&entry.account_name)
        .bind(&entry.triggered_at)
        .bind(entry.hour)
        .bind(&entry.model)
        .bind(if entry.success { 1_i64 } else { 0_i64 })
        .bind(&entry.error_message)
        .bind(entry.duration_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| AppError::Internal(format!("insert prime_log: {}", e)))?;

        // 裁剪旧记录:只保留最近 MAX_RETAINED_ROWS 条
        let _ = sqlx::query(
            "DELETE FROM prime_logs WHERE id NOT IN \
             (SELECT id FROM prime_logs ORDER BY id DESC LIMIT $1)",
        )
        .bind(MAX_RETAINED_ROWS)
        .execute(&self.pool)
        .await;

        Ok(())
    }

    /// 读取最近 N 条记录,按 id 倒序(等价于插入时间倒序)。
    pub async fn list_recent(&self, limit: i64) -> Result<Vec<PrimeLogEntry>, AppError> {
        let rows = sqlx::query(
            "SELECT id, account_id, account_name, triggered_at, hour, model, success, error_message, duration_ms \
             FROM prime_logs ORDER BY id DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| AppError::Internal(format!("list prime_logs: {}", e)))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let success_int: i64 = row.try_get("success").unwrap_or(0);
            out.push(PrimeLogEntry {
                id: row.try_get("id").unwrap_or_default(),
                account_id: row.try_get("account_id").unwrap_or_default(),
                account_name: row.try_get("account_name").unwrap_or_default(),
                triggered_at: row.try_get("triggered_at").unwrap_or_default(),
                hour: row.try_get("hour").unwrap_or_default(),
                model: row.try_get("model").unwrap_or_default(),
                success: success_int != 0,
                error_message: row.try_get("error_message").unwrap_or_default(),
                duration_ms: row.try_get("duration_ms").unwrap_or_default(),
            });
        }
        Ok(out)
    }
}
