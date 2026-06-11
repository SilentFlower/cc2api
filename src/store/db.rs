use sqlx::AnyPool;
use std::path::Path;

const PREVIOUS_ALLOWED_CLAUDE_CODE_VERSIONS_SETTINGS: &[&str] =
    &["2.1.89-2.1.156", "2.1.89-2.1.169"];

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
        // 代理 reqwest Client 连接池默认开启,可通过设置页关闭用于排查连接复用问题。
        (
            "proxy_client_pool_enabled",
            crate::store::settings_store::DEFAULT_PROXY_CLIENT_POOL_ENABLED,
        ),
        // 预热/辅助请求本地拦截默认关闭,避免升级后改变转发行为。
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
            "intercept_warmup_non_stream_aux_enabled",
            crate::store::settings_store::DEFAULT_INTERCEPT_WARMUP_NON_STREAM_AUX_ENABLED,
        ),
        (
            "intercept_warmup_non_stream_aux_mode",
            crate::store::settings_store::DEFAULT_INTERCEPT_WARMUP_NON_STREAM_AUX_MODE,
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
            "log_429_request_body_limit",
            crate::store::settings_store::DEFAULT_LOG_429_REQUEST_BODY_LIMIT,
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
    upgrade_default_settings(pool).await?;
    upgrade_account_claude_code_profile(pool, driver).await?;

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

async fn upgrade_default_settings(pool: &AnyPool) -> Result<(), sqlx::Error> {
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

async fn upgrade_account_claude_code_profile(
    pool: &AnyPool,
    driver: &str,
) -> Result<(), sqlx::Error> {
    let version = crate::service::version_profile::DEFAULT_CLAUDE_CODE_VERSION;
    let version_base = crate::service::version_profile::DEFAULT_CLAUDE_CODE_VERSION_BASE;
    let build_time = crate::service::version_profile::DEFAULT_CLAUDE_CODE_BUILD_TIME;
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
            '$.build_time', $3
        )
        "#
    } else {
        r#"
        UPDATE accounts
        SET canonical_env = jsonb_set(
            jsonb_set(
                jsonb_set(canonical_env, '{version}', to_jsonb($1::text), true),
                '{version_base}', to_jsonb($2::text), true
            ),
            '{build_time}', to_jsonb($3::text), true
        )
        "#
    };
    sqlx::query(sql)
        .bind(version)
        .bind(version_base)
        .bind(build_time)
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
        assert_eq!(env["platform"], "linux");
        assert_eq!(env["custom"], "keep");
    }

    #[tokio::test]
    async fn migrate_upgrades_only_old_default_allowed_versions_setting() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        for previous in PREVIOUS_ALLOWED_CLAUDE_CODE_VERSIONS_SETTINGS {
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
