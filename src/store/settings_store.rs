use sqlx::AnyPool;
use sqlx::Row;
use std::collections::HashMap;

use crate::error::AppError;
use crate::service::access_policy::{
    DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS, DEFAULT_ALLOWED_USER_AGENTS,
    DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS,
};
use crate::service::version_profile::{ClaudeCodeProfile, DEFAULT_CLAUDE_CODE_VERSION_PROFILE};

/// 允许 `messages[].role=system` 的默认模型列表。
pub const DEFAULT_ALLOW_SYSTEM_ROLE_MODELS: &str = "claude-opus-4-8";
/// 默认允许的 Claude Code / Claude CLI 版本范围。
pub const DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING: &str = DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS;
/// 默认禁止的 Claude Code / Claude CLI 版本范围。
pub const DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING: &str = DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS;
/// 默认 Claude Code 版本画像 key。
pub const DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING: &str = DEFAULT_CLAUDE_CODE_VERSION_PROFILE;
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
/// `/v1/messages` 顶层字段顺序指纹对齐默认开启,用于贴近 2.1.195 抓包。
pub const DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED: &str = "true";
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
/// 非流式单消息探针缓存默认关闭,避免升级后改变上游请求行为。
pub const DEFAULT_NON_STREAM_PROBE_CACHE_ENABLED: &str = "false";
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
/// bootstrap 自定义模型选项默认保留当前抓包中出现的 Fable 入口。
pub const DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS: &str = r#"[{"model":"claude-fable-5[1m]","name":"Fable","description":"Most capable for your hardest and longest-running tasks","disabled_reason":null}]"#;

/// 全局设置存储，key-value 结构。
pub struct SettingsStore {
    pool: AnyPool,
    driver: String,
}

impl SettingsStore {
    /// 构造默认 SQLite 方言的设置存储。
    ///
    /// @param pool SQLx 连接池。
    /// @return 设置存储实例。
    pub fn new(pool: AnyPool) -> Self {
        Self::new_with_driver(pool, "sqlite".into())
    }

    /// 构造指定数据库方言的设置存储。
    ///
    /// @param pool SQLx 连接池。
    /// @param driver 数据库驱动名，支持 `sqlite` 和 `postgres`。
    /// @return 设置存储实例。
    pub fn new_with_driver(pool: AnyPool, driver: String) -> Self {
        Self { pool, driver }
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

    /// 以事务方式应用 Claude Code 版本画像设置。
    ///
    /// 该操作会把所有账号的 canonical env 版本字段同步到目标画像，并强制覆盖
    /// `allowed_claude_code_versions`。账号 env 与 settings 在同一事务中提交，避免热路径
    /// 看到部分切换后的画像组合。
    ///
    /// @param profile 目标版本画像。
    /// @return 应用成功返回 `Ok(())`，数据库失败时返回业务错误。
    pub async fn apply_claude_code_profile(
        &self,
        profile: &'static ClaudeCodeProfile,
    ) -> Result<(), AppError> {
        let identity = &profile.identity;
        let account_sql = if self.driver == "postgres" {
            r#"
            UPDATE accounts
            SET canonical_env = jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            jsonb_set(canonical_env, '{version}', to_jsonb($1::text), true),
                            '{version_base}', to_jsonb($2::text), true
                        ),
                        '{build_time}', to_jsonb($3::text), true
                    ),
                    '{node_version}', to_jsonb($4::text), true
                ),
                updated_at=NOW()
            "#
        } else {
            r#"
            UPDATE accounts
            SET canonical_env = json_set(
                CASE
                    WHEN json_valid(canonical_env) THEN canonical_env
                    ELSE '{}'
                END,
                '$.version', $1,
                '$.version_base', $2,
                '$.build_time', $3,
                '$.node_version', $4
            ),
            updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
            "#
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| AppError::Internal(format!("begin profile transaction: {}", e)))?;
        sqlx::query(account_sql)
            .bind(identity.version)
            .bind(identity.version_base)
            .bind(identity.build_time)
            .bind(identity.stainless_runtime_version)
            .execute(&mut *tx)
            .await
            .map_err(|e| AppError::Internal(format!("update account profile: {}", e)))?;

        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        )
        .bind("claude_code_version_profile")
        .bind(profile.key)
        .execute(&mut *tx)
        .await
        .map_err(|e| AppError::Internal(format!("upsert profile setting: {}", e)))?;

        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, $2) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        )
        .bind("allowed_claude_code_versions")
        .bind(profile.access_policy.allowed_claude_code_versions)
        .execute(&mut *tx)
        .await
        .map_err(|e| AppError::Internal(format!("upsert allowed versions setting: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| AppError::Internal(format!("commit profile transaction: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_ALLOW_SYSTEM_ROLE_MODELS, DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING,
        DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING,
        DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED, SettingsStore,
    };
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

    async fn make_store_with_pool() -> (SettingsStore, AnyPool) {
        sqlx::any::install_default_drivers();
        let tmp = std::env::temp_dir().join(format!("ccgw_settings_{}.db", rand::random::<u64>()));
        let dsn = format!("sqlite:{}?mode=rwc", tmp.display());
        let pool = AnyPool::connect(&dsn).await.expect("pool");
        crate::store::db::migrate(&pool, "sqlite")
            .await
            .expect("migrate");
        (SettingsStore::new(pool.clone()), pool)
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

    #[tokio::test]
    async fn default_profile_setting_is_current_profile_key() {
        let store = make_store().await;

        let value = store
            .get_value(
                "claude_code_version_profile",
                DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING,
            )
            .await
            .expect("get value");

        assert_eq!(value, DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING);
    }

    #[tokio::test]
    async fn apply_profile_updates_accounts_and_allowed_versions() {
        let (settings_store, pool) = make_store_with_pool().await;
        sqlx::query(
            r#"INSERT INTO accounts (
                email, token, device_id, canonical_env, canonical_prompt_env, canonical_process
            ) VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind("user@example.com")
        .bind("token")
        .bind("device-1")
        .bind(r#"{"version":"2.1.185","custom":"keep"}"#)
        .bind("{}")
        .bind("{}")
        .execute(&pool)
        .await
        .expect("insert account");

        let mut custom = std::collections::HashMap::new();
        custom.insert(
            "allowed_user_agents".to_string(),
            "custom-agent".to_string(),
        );
        custom.insert(
            "blocked_claude_code_versions".to_string(),
            "2.1.187".to_string(),
        );
        settings_store
            .upsert_many(&custom)
            .await
            .expect("upsert custom");

        let profile = crate::service::version_profile::profile_for_key("2.1.173").unwrap();
        settings_store
            .apply_claude_code_profile(profile)
            .await
            .expect("apply profile");

        let settings = settings_store.get_all().await.expect("settings");
        assert_eq!(
            settings.get("claude_code_version_profile").unwrap(),
            "2.1.173"
        );
        assert_eq!(
            settings.get("allowed_claude_code_versions").unwrap(),
            "2.1.89-2.1.173"
        );
        assert_eq!(settings.get("allowed_user_agents").unwrap(), "custom-agent");
        assert_eq!(
            settings.get("blocked_claude_code_versions").unwrap(),
            "2.1.187"
        );

        let raw: String = sqlx::query_scalar("SELECT canonical_env FROM accounts WHERE email=$1")
            .bind("user@example.com")
            .fetch_one(&pool)
            .await
            .expect("canonical_env");
        let env: serde_json::Value = serde_json::from_str(&raw).expect("env json");
        assert_eq!(env["version"], "2.1.173");
        assert_eq!(env["version_base"], "2.1.173");
        assert_eq!(env["build_time"], "2026-06-11T01:23:13Z");
        assert_eq!(
            env["node_version"],
            crate::service::version_profile::profile_for_key("2.1.173")
                .unwrap()
                .identity
                .stainless_runtime_version
        );
        assert_eq!(env["custom"], "keep");
    }

    #[tokio::test]
    async fn default_blocked_versions_setting_is_empty() {
        let store = make_store().await;

        let value = store
            .get_value(
                "blocked_claude_code_versions",
                DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING,
            )
            .await
            .expect("get value");

        assert_eq!(value, DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING);
    }

    #[tokio::test]
    async fn default_message_body_order_fingerprint_setting_is_enabled() {
        let store = make_store().await;

        let value = store
            .get_value(
                "message_body_order_fingerprint_enabled",
                DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED,
            )
            .await
            .expect("get value");

        assert_eq!(value, DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED);
    }
}
