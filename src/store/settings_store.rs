use std::collections::HashMap;
use sqlx::AnyPool;
use sqlx::Row;

use crate::error::AppError;

/// 全局设置存储，key-value 结构。
pub struct SettingsStore {
    pool: AnyPool,
}

impl SettingsStore {
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }

    /// 读取所有设置项。
    pub async fn get_all(&self) -> Result<HashMap<String, String>, AppError> {
        let rows = sqlx::query("SELECT key, value FROM settings")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| AppError::Internal(format!("query settings: {}", e)))?;

        let mut map = HashMap::new();
        for row in rows {
            let key: String = row.try_get("key").unwrap_or_default();
            let value: String = row.try_get("value").unwrap_or_default();
            map.insert(key, value);
        }
        Ok(map)
    }

    /// 批量更新设置项（upsert）。
    pub async fn upsert_many(&self, items: &HashMap<String, String>) -> Result<(), AppError> {
        for (key, value) in items {
            sqlx::query(
                "INSERT INTO settings (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            )
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await
            .map_err(|e| AppError::Internal(format!("upsert setting '{}': {}", key, e)))?;
        }
        Ok(())
    }
}
