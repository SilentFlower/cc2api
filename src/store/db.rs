use sqlx::AnyPool;
use std::path::Path;

const PREVIOUS_ALLOWED_CLAUDE_CODE_VERSIONS_SETTINGS: &[&str] = &[
    "2.1.89-2.1.156",
    "2.1.89-2.1.169",
    "2.1.89-2.1.172",
    "2.1.89-2.1.173",
    "2.1.89-2.1.185",
    "2.1.89-2.1.187",
];
const PREVIOUS_DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTINGS: &[&str] = &["2.1.187"];
const OBSOLETE_SETTINGS_KEYS: &[&str] = &[
    "intercept_warmup_non_stream_aux_enabled",
    "intercept_warmup_non_stream_aux_mode",
];

pub async fn init_db(driver: &str, dsn: &str) -> Result<AnyPool, sqlx::Error> {
    if driver == "sqlite" {
        if let Some(parent) = Path::new(dsn).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let pool = AnyPool::connect(&format!("sqlite:{}?mode=rwc", dsn)).await?;
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&pool)
            .await
            .ok();
        Ok(pool)
    } else {
        let pool = AnyPool::connect(dsn).await?;
        Ok(pool)
    }
}

pub async fn migrate(pool: &AnyPool, driver: &str) -> Result<(), sqlx::Error> {
    let schema = if driver == "sqlite" {
        SQLITE_SCHEMA
    } else {
        PG_SCHEMA
    };
    for stmt in schema.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt).execute(pool).await?;
    }
    // 增量迁移
    sqlx::query("ALTER TABLE accounts ADD COLUMN billing_mode TEXT NOT NULL DEFAULT 'strip'")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN usage_data TEXT NOT NULL DEFAULT '{}'")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN usage_fetched_at TEXT")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN auth_type TEXT NOT NULL DEFAULT 'setup_token'")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN access_token TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN refresh_token TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN oauth_expires_at TEXT")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN oauth_refreshed_at TEXT")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN auth_error TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN account_uuid TEXT")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN organization_uuid TEXT")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN subscription_type TEXT")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN disable_reason TEXT NOT NULL DEFAULT ''")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN auto_telemetry INTEGER NOT NULL DEFAULT 0")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN telemetry_count INTEGER NOT NULL DEFAULT 0")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN auto_poll_usage INTEGER NOT NULL DEFAULT 0")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN allow_1m_models TEXT NOT NULL DEFAULT 'opus'")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN rpm_limit INTEGER NOT NULL DEFAULT 0")
        .execute(pool)
        .await
        .ok();

    // api_tokens 表
    let token_schema = if driver == "sqlite" {
        SQLITE_TOKENS_SCHEMA
    } else {
        PG_TOKENS_SCHEMA
    };
    for stmt in token_schema.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt).execute(pool).await?;
    }

    // settings 表（全局配置项）
    let settings_schema = if driver == "sqlite" {
        SQLITE_SETTINGS_SCHEMA
    } else {
        PG_SETTINGS_SCHEMA
    };
    for stmt in settings_schema.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt).execute(pool).await?;
    }
    // 插入默认评分权重与峰值预热配置（仅当 key 不存在时）
    for (key, val) in &[
        ("score_weight_7d", "0.5"),
        ("score_weight_5h", "0.3"),
        ("score_weight_concurrency", "0.2"),
        // 峰值预热相关默认值
        ("peak_prime_enabled", "true"),
        ("peak_prime_hours", "4,5,6"),
        ("peak_prime_model", "claude-haiku-4-5-20251001"),
        // Claude Code Opus 4.8 会在 messages 中携带 role=system。
        (
            "allow_system_role_models",
            crate::store::settings_store::DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
        ),
        (
            "allowed_claude_code_versions",
            crate::store::settings_store::DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING,
        ),
        (
            "blocked_claude_code_versions",
            crate::store::settings_store::DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING,
        ),
        (
            "claude_code_version_profile",
            crate::store::settings_store::DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING,
        ),
        (
            "allowed_user_agents",
            crate::store::settings_store::DEFAULT_ALLOWED_USER_AGENTS_SETTING,
        ),
        // 系统提示词环境字段「真值透传」开关:工作目录默认透传,shell/os_version 默认改写。
        (
            "passthrough_shell",
            crate::store::settings_store::DEFAULT_PASSTHROUGH_SHELL,
        ),
        (
            "passthrough_os_version",
            crate::store::settings_store::DEFAULT_PASSTHROUGH_OS_VERSION,
        ),
        (
            "passthrough_working_dir",
            crate::store::settings_store::DEFAULT_PASSTHROUGH_WORKING_DIR,
        ),
        // Anthropic cache_control TTL 改写默认关闭,避免升级后改变缓存断点语义。
        (
            "cache_control_ttl_rewrite",
            crate::store::settings_store::DEFAULT_CACHE_CONTROL_TTL_REWRITE,
        ),
        // Claude Code messages 缓存断点稳定化默认关闭,避免升级后改变缓存断点布局。
        (
            "message_cache_control_rewrite",
            crate::store::settings_store::DEFAULT_MESSAGE_CACHE_CONTROL_REWRITE,
        ),
        // `/v1/messages` 顶层字段顺序默认对齐真实 Claude Code 抓包;可在设置页关闭回滚。
        (
            "message_body_order_fingerprint_enabled",
            crate::store::settings_store::DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED,
        ),
        // 代理 reqwest Client 连接池默认开启,可通过设置页关闭用于排查连接复用问题。
        (
            "proxy_client_pool_enabled",
            crate::store::settings_store::DEFAULT_PROXY_CLIENT_POOL_ENABLED,
        ),
        // 预热与 Auto Mode classifier 本地处理默认不改变转发行为。
        (
            "intercept_warmup_title_enabled",
            crate::store::settings_store::DEFAULT_INTERCEPT_WARMUP_TITLE_ENABLED,
        ),
        (
            "intercept_warmup_suggestion_enabled",
            crate::store::settings_store::DEFAULT_INTERCEPT_WARMUP_SUGGESTION_ENABLED,
        ),
        (
            "intercept_warmup_haiku_probe_enabled",
            crate::store::settings_store::DEFAULT_INTERCEPT_WARMUP_HAIKU_PROBE_ENABLED,
        ),
        (
            "intercept_auto_mode_classifier_stage1_mode",
            crate::store::settings_store::DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE1_MODE,
        ),
        (
            "intercept_auto_mode_classifier_stage2_mode",
            crate::store::settings_store::DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE2_MODE,
        ),
        // thinking.type=disabled 兼容改写默认关闭,管理员确认模型后再开启。
        (
            "rewrite_disabled_thinking_enabled",
            crate::store::settings_store::DEFAULT_REWRITE_DISABLED_THINKING_ENABLED,
        ),
        (
            "rewrite_disabled_thinking_models",
            crate::store::settings_store::DEFAULT_REWRITE_DISABLED_THINKING_MODELS,
        ),
        // assistant prefill 本地拦截默认关闭,避免升级后改变转发行为。
        (
            "intercept_assistant_prefill_enabled",
            crate::store::settings_store::DEFAULT_INTERCEPT_ASSISTANT_PREFILL_ENABLED,
        ),
        (
            "intercept_assistant_prefill_models",
            crate::store::settings_store::DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
        ),
        // 429 请求观测默认关闭,开启后只输出脱敏和截断后的请求信息。
        (
            "log_429_request_enabled",
            crate::store::settings_store::DEFAULT_LOG_429_REQUEST_ENABLED,
        ),
        (
            "log_non_stream_request_enabled",
            crate::store::settings_store::DEFAULT_LOG_NON_STREAM_REQUEST_ENABLED,
        ),
        (
            "non_stream_probe_cache_enabled",
            crate::store::settings_store::DEFAULT_NON_STREAM_PROBE_CACHE_ENABLED,
        ),
        (
            "log_429_request_body_limit",
            crate::store::settings_store::DEFAULT_LOG_429_REQUEST_BODY_LIMIT,
        ),
        // 流式稳定性默认关闭,管理员确认 watchdog fallback 后再开启。
        (
            "stream_keepalive_enabled",
            crate::store::settings_store::DEFAULT_STREAM_KEEPALIVE_ENABLED,
        ),
        (
            "stream_keepalive_interval_secs",
            crate::store::settings_store::DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECS,
        ),
        (
            "stream_upstream_idle_timeout_secs",
            crate::store::settings_store::DEFAULT_STREAM_UPSTREAM_IDLE_TIMEOUT_SECS,
        ),
        // Claude Code bootstrap 模型选项默认透传上游;管理员可切换为配置列表或隐藏 Fable。
        (
            "bootstrap_model_options_mode",
            crate::store::settings_store::DEFAULT_BOOTSTRAP_MODEL_OPTIONS_MODE,
        ),
        (
            "bootstrap_additional_model_options",
            crate::store::settings_store::DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS,
        ),
    ] {
        let insert_sql = if driver == "sqlite" {
            "INSERT OR IGNORE INTO settings (key, value) VALUES ($1, $2)"
        } else {
            "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING"
        };
        sqlx::query(insert_sql)
            .bind(key)
            .bind(val)
            .execute(pool)
            .await
            .ok();
    }
    upgrade_default_profile_setting(pool).await?;
    let claude_code_profile = selected_claude_code_profile(pool).await?;
    upgrade_default_settings(pool, claude_code_profile).await?;
    remove_obsolete_settings(pool).await?;
    upgrade_account_claude_code_profile(pool, driver, claude_code_profile).await?;

    // prime_logs 表（峰值预热调用日志）
    let prime_logs_schema = if driver == "sqlite" {
        SQLITE_PRIME_LOGS_SCHEMA
    } else {
        PG_PRIME_LOGS_SCHEMA
    };
    for stmt in prime_logs_schema.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt).execute(pool).await?;
    }

    Ok(())
}

async fn selected_claude_code_profile(
    pool: &AnyPool,
) -> Result<&'static crate::service::version_profile::ClaudeCodeProfile, sqlx::Error> {
    let configured: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
        .bind("claude_code_version_profile")
        .fetch_optional(pool)
        .await?;
    Ok(configured
        .as_deref()
        .and_then(|key| crate::service::version_profile::profile_for_key(key).ok())
        .unwrap_or_else(crate::service::version_profile::default_profile))
}

async fn upgrade_default_settings(
    pool: &AnyPool,
    profile: &crate::service::version_profile::ClaudeCodeProfile,
) -> Result<(), sqlx::Error> {
    if profile.key != crate::store::settings_store::DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING {
        return Ok(());
    }
    for previous in PREVIOUS_ALLOWED_CLAUDE_CODE_VERSIONS_SETTINGS {
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2 AND value=$3")
            .bind(crate::store::settings_store::DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING)
            .bind("allowed_claude_code_versions")
            .bind(previous)
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn upgrade_default_profile_setting(pool: &AnyPool) -> Result<(), sqlx::Error> {
    for previous_profile in PREVIOUS_DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTINGS {
        for previous_allowed in PREVIOUS_ALLOWED_CLAUDE_CODE_VERSIONS_SETTINGS {
            sqlx::query(
                r#"
                UPDATE settings
                SET value=$1
                WHERE key=$2
                  AND value=$3
                  AND EXISTS (
                      SELECT 1
                      FROM settings allowed_versions
                      WHERE allowed_versions.key=$4
                        AND allowed_versions.value=$5
                  )
                "#,
            )
            .bind(crate::store::settings_store::DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING)
            .bind("claude_code_version_profile")
            .bind(previous_profile)
            .bind("allowed_claude_code_versions")
            .bind(previous_allowed)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

async fn remove_obsolete_settings(pool: &AnyPool) -> Result<(), sqlx::Error> {
    for key in OBSOLETE_SETTINGS_KEYS {
        sqlx::query("DELETE FROM settings WHERE key=$1")
            .bind(key)
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn upgrade_account_claude_code_profile(
    pool: &AnyPool,
    driver: &str,
    profile: &crate::service::version_profile::ClaudeCodeProfile,
) -> Result<(), sqlx::Error> {
    let identity = &profile.identity;
    let sql = if driver == "sqlite" {
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
        )
        "#
    } else {
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
        )
        "#
    };
    sqlx::query(sql)
        .bind(identity.version)
        .bind(identity.version_base)
        .bind(identity.build_time)
        .bind(identity.stainless_runtime_version)
        .execute(pool)
        .await?;
    Ok(())
}

const SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL DEFAULT '',
    email           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    token           TEXT NOT NULL,
    auth_type       TEXT NOT NULL DEFAULT 'setup_token',
    access_token    TEXT NOT NULL DEFAULT '',
    refresh_token   TEXT NOT NULL DEFAULT '',
    oauth_expires_at    TEXT,
    oauth_refreshed_at  TEXT,
    auth_error      TEXT NOT NULL DEFAULT '',
    proxy_url       TEXT NOT NULL DEFAULT '',
    device_id       TEXT NOT NULL,
    canonical_env   TEXT NOT NULL DEFAULT '{}',
    canonical_prompt_env TEXT NOT NULL DEFAULT '{}',
    canonical_process    TEXT NOT NULL DEFAULT '{}',
    billing_mode    TEXT NOT NULL DEFAULT 'strip',
    concurrency     INTEGER NOT NULL DEFAULT 3,
    priority        INTEGER NOT NULL DEFAULT 50,
    rpm_limit       INTEGER NOT NULL DEFAULT 0,
    rate_limited_at      TEXT,
    rate_limit_reset_at  TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

"#;

const PG_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS accounts (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL DEFAULT '',
    email           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    token           TEXT NOT NULL,
    auth_type       TEXT NOT NULL DEFAULT 'setup_token',
    access_token    TEXT NOT NULL DEFAULT '',
    refresh_token   TEXT NOT NULL DEFAULT '',
    oauth_expires_at    TIMESTAMPTZ,
    oauth_refreshed_at  TIMESTAMPTZ,
    auth_error      TEXT NOT NULL DEFAULT '',
    proxy_url       TEXT NOT NULL DEFAULT '',
    device_id       TEXT NOT NULL,
    canonical_env   JSONB NOT NULL DEFAULT '{}',
    canonical_prompt_env JSONB NOT NULL DEFAULT '{}',
    canonical_process    JSONB NOT NULL DEFAULT '{}',
    billing_mode    TEXT NOT NULL DEFAULT 'strip',
    concurrency     INT NOT NULL DEFAULT 3,
    priority        INT NOT NULL DEFAULT 50,
    rpm_limit       INT NOT NULL DEFAULT 0,
    rate_limited_at      TIMESTAMPTZ,
    rate_limit_reset_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

"#;

const SQLITE_TOKENS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS api_tokens (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL DEFAULT '',
    token               TEXT NOT NULL UNIQUE,
    allowed_accounts    TEXT NOT NULL DEFAULT '',
    blocked_accounts    TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'active',
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
)
"#;

const PG_TOKENS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS api_tokens (
    id                  BIGSERIAL PRIMARY KEY,
    name                TEXT NOT NULL DEFAULT '',
    token               TEXT NOT NULL UNIQUE,
    allowed_accounts    TEXT NOT NULL DEFAULT '',
    blocked_accounts    TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'active',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"#;

const SQLITE_SETTINGS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS settings (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
)
"#;

const PG_SETTINGS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS settings (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
)
"#;

/// 峰值预热日志表（SQLite）。
/// triggered_at 存 ISO8601 本地时间字符串（与既有表一致的字符串风格），避免跨平台时区差异。
const SQLITE_PRIME_LOGS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS prime_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER NOT NULL,
    account_name    TEXT NOT NULL,
    triggered_at    TEXT NOT NULL,
    hour            INTEGER NOT NULL,
    model           TEXT NOT NULL,
    success         INTEGER NOT NULL,
    error_message   TEXT NOT NULL DEFAULT '',
    duration_ms     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_prime_logs_triggered_at ON prime_logs(triggered_at DESC);
"#;

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_sqlite_pool() -> AnyPool {
        sqlx::any::install_default_drivers();
        let tmp = std::env::temp_dir().join(format!("ccgw_db_{}.db", rand::random::<u64>()));
        let dsn = format!("sqlite:{}?mode=rwc", tmp.display());
        AnyPool::connect(&dsn).await.expect("pool")
    }

    #[tokio::test]
    async fn migrate_upgrades_existing_account_claude_code_profile() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        sqlx::query(
            r#"INSERT INTO accounts (
                email, token, device_id, canonical_env, canonical_prompt_env, canonical_process
            ) VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind("user@example.com")
        .bind("token")
        .bind("device-1")
        .bind(
            r#"{"version":"2.1.156","version_base":"2.1.156","build_time":"2026-05-28T18:30:33Z","platform":"linux","custom":"keep"}"#,
        )
        .bind("{}")
        .bind("{}")
        .execute(&pool)
        .await
        .expect("insert account");

        migrate(&pool, "sqlite").await.expect("second migrate");

        let raw: String = sqlx::query_scalar("SELECT canonical_env FROM accounts WHERE email=$1")
            .bind("user@example.com")
            .fetch_one(&pool)
            .await
            .expect("canonical_env");
        let env: serde_json::Value = serde_json::from_str(&raw).expect("json");
        assert_eq!(
            env["version"],
            crate::service::version_profile::DEFAULT_CLAUDE_CODE_VERSION
        );
        assert_eq!(
            env["version_base"],
            crate::service::version_profile::DEFAULT_CLAUDE_CODE_VERSION_BASE
        );
        assert_eq!(
            env["build_time"],
            crate::service::version_profile::DEFAULT_CLAUDE_CODE_BUILD_TIME
        );
        assert_eq!(
            env["node_version"],
            crate::service::version_profile::STAINLESS_RUNTIME_VERSION
        );
        assert_eq!(env["platform"], "linux");
        assert_eq!(env["custom"], "keep");
    }

    #[tokio::test]
    async fn migrate_upgrades_only_old_default_allowed_versions_setting() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        for previous in PREVIOUS_ALLOWED_CLAUDE_CODE_VERSIONS_SETTINGS {
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind("2.1.187")
                .bind("claude_code_version_profile")
                .execute(&pool)
                .await
                .expect("set old profile default");
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(previous)
                .bind("allowed_claude_code_versions")
                .execute(&pool)
                .await
                .expect("set old default");

            migrate(&pool, "sqlite").await.expect("second migrate");
            let upgraded: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind("allowed_claude_code_versions")
                .fetch_one(&pool)
                .await
                .expect("upgraded setting");
            assert_eq!(
                upgraded,
                crate::store::settings_store::DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING
            );
            let upgraded_profile: String =
                sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                    .bind("claude_code_version_profile")
                    .fetch_one(&pool)
                    .await
                    .expect("upgraded profile setting");
            assert_eq!(
                upgraded_profile,
                crate::store::settings_store::DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING
            );
        }

        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("2.1.*")
            .bind("allowed_claude_code_versions")
            .execute(&pool)
            .await
            .expect("set custom");
        migrate(&pool, "sqlite").await.expect("third migrate");
        let custom: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("allowed_claude_code_versions")
            .fetch_one(&pool)
            .await
            .expect("custom setting");
        assert_eq!(custom, "2.1.*");
    }

    #[tokio::test]
    async fn migrate_preserves_explicit_old_claude_code_profile_with_custom_allowed_versions() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        sqlx::query(
            r#"INSERT INTO accounts (
                email, token, device_id, canonical_env, canonical_prompt_env, canonical_process
            ) VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind("user@example.com")
        .bind("token")
        .bind("device-1")
        .bind(
            r#"{"version":"2.1.187","version_base":"2.1.187","build_time":"new","custom":"keep"}"#,
        )
        .bind("{}")
        .bind("{}")
        .execute(&pool)
        .await
        .expect("insert account");

        for (profile_key, allowed_versions, build_time) in [
            ("2.1.173", "2.1.173", "2026-06-11T01:23:13Z"),
            ("2.1.185", "2.1.185", "2026-06-20T06:38:30Z"),
            ("2.1.187", "2.1.187", "2026-06-23T16:59:46Z"),
        ] {
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(profile_key)
                .bind("claude_code_version_profile")
                .execute(&pool)
                .await
                .expect("set profile");
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(allowed_versions)
                .bind("allowed_claude_code_versions")
                .execute(&pool)
                .await
                .expect("set allowed versions");

            migrate(&pool, "sqlite").await.expect("rerun migrate");

            let stored_allowed: String =
                sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                    .bind("allowed_claude_code_versions")
                    .fetch_one(&pool)
                    .await
                    .expect("allowed versions");
            assert_eq!(stored_allowed, allowed_versions);

            let raw: String =
                sqlx::query_scalar("SELECT canonical_env FROM accounts WHERE email=$1")
                    .bind("user@example.com")
                    .fetch_one(&pool)
                    .await
                    .expect("canonical_env");
            let env: serde_json::Value = serde_json::from_str(&raw).expect("json");
            assert_eq!(env["version"], profile_key);
            assert_eq!(env["version_base"], profile_key);
            assert_eq!(env["build_time"], build_time);
            assert_eq!(
                env["node_version"],
                crate::service::version_profile::profile_for_key(profile_key)
                    .unwrap()
                    .identity
                    .stainless_runtime_version
            );
            assert_eq!(env["custom"], "keep");
        }
    }

    #[tokio::test]
    async fn migrate_inserts_default_claude_code_version_profile_setting() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let profile: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("claude_code_version_profile")
            .fetch_one(&pool)
            .await
            .expect("profile setting");

        assert_eq!(
            profile,
            crate::store::settings_store::DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING
        );
    }

    #[tokio::test]
    async fn migrate_inserts_default_blocked_claude_code_versions_setting() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let blocked_versions: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind("blocked_claude_code_versions")
                .fetch_one(&pool)
                .await
                .expect("blocked versions setting");

        assert_eq!(
            blocked_versions,
            crate::store::settings_store::DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING
        );
    }

    #[tokio::test]
    async fn migrate_inserts_default_message_body_order_fingerprint_setting() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let enabled: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("message_body_order_fingerprint_enabled")
            .fetch_one(&pool)
            .await
            .expect("message body order setting");

        assert_eq!(
            enabled,
            crate::store::settings_store::DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED
        );
    }

    #[tokio::test]
    async fn migrate_removes_obsolete_non_stream_aux_settings() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        for key in OBSOLETE_SETTINGS_KEYS {
            sqlx::query("INSERT OR REPLACE INTO settings (key, value) VALUES ($1, $2)")
                .bind(key)
                .bind("legacy")
                .execute(&pool)
                .await
                .expect("insert obsolete setting");
        }

        migrate(&pool, "sqlite").await.expect("second migrate");

        for key in OBSOLETE_SETTINGS_KEYS {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM settings WHERE key=$1")
                .bind(key)
                .fetch_one(&pool)
                .await
                .expect("count obsolete setting");
            assert_eq!(count, 0, "{} should be removed", key);
        }
    }
}

/// 峰值预热日志表（PostgreSQL）。
const PG_PRIME_LOGS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS prime_logs (
    id              BIGSERIAL PRIMARY KEY,
    account_id      BIGINT NOT NULL,
    account_name    TEXT NOT NULL,
    triggered_at    TEXT NOT NULL,
    hour            INT NOT NULL,
    model           TEXT NOT NULL,
    success         INT NOT NULL,
    error_message   TEXT NOT NULL DEFAULT '',
    duration_ms     BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_prime_logs_triggered_at ON prime_logs(triggered_at DESC);
"#;
