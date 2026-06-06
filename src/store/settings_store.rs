use sqlx::AnyPool;
use sqlx::Row;
use std::collections::HashMap;

use crate::error::AppError;
use crate::service::access_policy::{
    DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS, DEFAULT_ALLOWED_USER_AGENTS,
};

/// 允许 `messages[].role=system` 的默认模型列表。
pub const DEFAULT_ALLOW_SYSTEM_ROLE_MODELS: &str = "claude-opus-4-8";
/// 默认允许的 Claude Code / Claude CLI 版本范围。
pub const DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING: &str =
    DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS;
/// 默认允许的非 Claude Code 客户端 User-Agent。
pub const DEFAULT_ALLOWED_USER_AGENTS_SETTING: &str = DEFAULT_ALLOWED_USER_AGENTS;

/// 系统提示词 `Shell:` 行是否默认真值透传。默认关闭(仍改写为账号预设)。
pub const DEFAULT_PASSTHROUGH_SHELL: &str = "false";
/// 系统提示词 `OS Version:` 行是否默认真值透传。默认关闭(仍改写为账号预设)。
pub const DEFAULT_PASSTHROUGH_OS_VERSION: &str = "false";
/// 系统提示词 `Working directory:` 行是否默认真值透传。默认开启:
/// 工作目录改写会直接误导模型对真实 cwd 的判断,故默认透传真实路径。
pub const DEFAULT_PASSTHROUGH_WORKING_DIR: &str = "true";
/// Anthropic ephemeral cache_control TTL 改写默认关闭,保持旧请求体行为。
pub const DEFAULT_CACHE_CONTROL_TTL_REWRITE: &str = "off";
/// Claude Code messages 缓存断点稳定化默认关闭,保持旧请求体行为。
pub const DEFAULT_MESSAGE_CACHE_CONTROL_REWRITE: &str = "off";

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

    /// 读取单个设置项。
    ///
    /// @param key 设置项 key。
    /// @param default_value key 不存在时返回的默认值。
    /// @return 设置项字符串值。
    pub async fn get_value(&self, key: &str, default_value: &str) -> Result<String, AppError> {
        let row = sqlx::query("SELECT value FROM settings WHERE key=$1")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| AppError::Internal(format!("query setting '{}': {}", key, e)))?;

        Ok(row
            .and_then(|r| r.try_get::<String, _>("value").ok())
            .unwrap_or_else(|| default_value.to_string()))
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

#[cfg(test)]
mod tests {
    use super::{DEFAULT_ALLOW_SYSTEM_ROLE_MODELS, SettingsStore};
    use sqlx::AnyPool;

    async fn make_store() -> SettingsStore {
        sqlx::any::install_default_drivers();
        let tmp = std::env::temp_dir().join(format!("ccgw_settings_{}.db", rand::random::<u64>()));
        let dsn = format!("sqlite:{}?mode=rwc", tmp.display());
        let pool = AnyPool::connect(&dsn).await.expect("pool");
        crate::store::db::migrate(&pool, "sqlite").await.expect("migrate");
        SettingsStore::new(pool)
    }

    #[tokio::test]
    async fn get_value_keeps_empty_string_as_configured_value() {
        let store = make_store().await;
        let mut items = std::collections::HashMap::new();
        items.insert("allow_system_role_models".to_string(), String::new());

        store.upsert_many(&items).await.expect("upsert");

        let value = store
            .get_value("allow_system_role_models", DEFAULT_ALLOW_SYSTEM_ROLE_MODELS)
            .await
            .expect("get value");
        assert_eq!(value, "");
    }

    #[tokio::test]
    async fn get_value_returns_default_for_missing_key() {
        let store = make_store().await;

        let value = store
            .get_value("missing_key", DEFAULT_ALLOW_SYSTEM_ROLE_MODELS)
            .await
            .expect("get value");
        assert_eq!(value, DEFAULT_ALLOW_SYSTEM_ROLE_MODELS);
    }
}
