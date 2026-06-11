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
pub const DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING: &str = DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS;
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
/// 代理 HTTP 客户端连接池默认开启,用于复用同一代理下的底层连接。
pub const DEFAULT_PROXY_CLIENT_POOL_ENABLED: &str = "true";
/// 标题/`Warmup` 预热请求本地拦截默认关闭,避免升级后改变请求行为。
pub const DEFAULT_INTERCEPT_WARMUP_TITLE_ENABLED: &str = "false";
/// Suggestion Mode 请求本地拦截默认关闭,由管理员显式开启。
pub const DEFAULT_INTERCEPT_WARMUP_SUGGESTION_ENABLED: &str = "false";
/// Claude Code Haiku `max_tokens=1` 探测请求本地拦截默认关闭,由管理员显式开启。
pub const DEFAULT_INTERCEPT_WARMUP_HAIKU_PROBE_ENABLED: &str = "false";
/// Auto Mode classifier Stage 1 默认转发上游,避免升级后改变安全判定行为。
pub const DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE1_MODE: &str = "passthrough";
/// Auto Mode classifier Stage 2 默认转发上游,避免升级后改变安全判定行为。
pub const DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE2_MODE: &str = "passthrough";
/// `thinking.type=disabled` 自动改写默认关闭,避免升级后改变请求体语义。
pub const DEFAULT_REWRITE_DISABLED_THINKING_ENABLED: &str = "false";
/// 默认只匹配线上已确认报错的 Fable 5,避免影响 Opus 4.8 / 4.7 的正常请求。
pub const DEFAULT_REWRITE_DISABLED_THINKING_MODELS: &str = "claude-fable-5";
/// assistant prefill 本地拦截默认关闭,避免升级后改变请求行为。
pub const DEFAULT_INTERCEPT_ASSISTANT_PREFILL_ENABLED: &str = "false";
/// 默认只覆盖当前事故相关模型,管理员可在设置页追加。
pub const DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS: &str =
    "claude-fable-5,claude-opus-4-8,claude-opus-4-7";
/// 429 请求观测日志默认关闭,避免默认记录用户请求内容。
pub const DEFAULT_LOG_429_REQUEST_ENABLED: &str = "false";
/// 非流式 `/v1/messages` 请求观测日志默认关闭,避免默认记录用户请求内容。
pub const DEFAULT_LOG_NON_STREAM_REQUEST_ENABLED: &str = "false";
/// 429 请求观测日志默认请求体字符上限。
pub const DEFAULT_LOG_429_REQUEST_BODY_LIMIT: &str = "8192";
/// 流式 SSE keep-alive 默认关闭,避免升级后改变下游流字节。
pub const DEFAULT_STREAM_KEEPALIVE_ENABLED: &str = "false";
/// 流式 SSE keep-alive 默认间隔秒数,需小于 Claude Code watchdog。
pub const DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECS: &str = "45";
/// 上游流静默超时默认保持历史 120 秒行为。
pub const DEFAULT_STREAM_UPSTREAM_IDLE_TIMEOUT_SECS: &str = "120";
/// bootstrap 模型选项默认透传上游响应,避免未配置时改变上游能力列表。
pub const DEFAULT_BOOTSTRAP_MODEL_OPTIONS_MODE: &str = "passthrough";
/// bootstrap 自定义模型选项默认保留 2.1.172 抓包中出现的 Fable 入口。
pub const DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS: &str = r#"[{"model":"claude-fable-5[1m]","name":"Fable","description":"Most capable for your hardest and longest-running tasks","disabled_reason":null}]"#;

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
        crate::store::db::migrate(&pool, "sqlite")
            .await
            .expect("migrate");
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
