//! 网关初始数据库迁移定义；与业务查询和维护台账独立。
use chrono::{SecondsFormat, Utc};
use sqlx::{AnyConnection, AnyPool, Error as SqlxError};

// 初始迁移冻结默认值和画像；后来的业务默认变更通过新迁移版本表达。
const DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING: &str = "2.1.89-2.1.293";
const DEFAULT_ALLOWED_USER_AGENTS_SETTING: &str = "AI-Hub-Monitor*\npython-httpx*";
const DEFAULT_ALLOW_1M_MODELS: &str = "opus,claude-sonnet-5";
const DEFAULT_ALLOW_SYSTEM_ROLE_MODELS: &str =
    "claude-opus-5-5,claude-opus-5,claude-fable-5,claude-fable-5-1,claude-opus-4-8";
const DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING: &str = "";
const DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS: &str = r#"[{"model":"claude-fable-5-1[1m]","name":"Fable","description":"Most capable for your hardest and longest-running tasks","disabled_reason":null}]"#;
const DEFAULT_BOOTSTRAP_MODEL_OPTIONS_MODE: &str = "passthrough";
const DEFAULT_CACHE_CONTROL_TTL_REWRITE: &str = "off";
const DEFAULT_CLAUDE_CODE_CONTEXT_SANITIZER_MODE: &str = "report_only";
const DEFAULT_CLAUDE_CODE_PROFILE_SELECTION_MODE_SETTING: &str = "client_version";
const DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING: &str = "2.1.293";
const DEFAULT_FABLE_STICKY_QUOTA_FALLBACK_ENABLED: &str = "true";
const DEFAULT_FABLE_WEEKLY_USAGE_LIMIT_PERCENT: &str = "50";
const DEFAULT_INTERCEPT_ASSISTANT_PREFILL_ENABLED: &str = "false";
const DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS: &str =
    "claude-fable-5,claude-opus-5,claude-opus-4-8,claude-opus-4-7";
const DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE1_MODE: &str = "passthrough";
const DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE2_MODE: &str = "passthrough";
const DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_IDENTITY_INJECTION_ENABLED: &str = "false";
const DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_MODE: &str = "passthrough";
const DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_MODELS: &str = "claude-fable-5,claude-fable-5-1,claude-opus-5,claude-opus-5-5,claude-opus-4-8,claude-opus-4-7,claude-sonnet-5";
const DEFAULT_INTERCEPT_WARMUP_HAIKU_PROBE_ENABLED: &str = "false";
const DEFAULT_INTERCEPT_WARMUP_SUGGESTION_ENABLED: &str = "false";
const DEFAULT_INTERCEPT_WARMUP_TITLE_ENABLED: &str = "false";
const DEFAULT_LOG_429_REQUEST_BODY_LIMIT: &str = "8192";
const DEFAULT_LOG_429_REQUEST_ENABLED: &str = "false";
const DEFAULT_LOG_NON_STREAM_REQUEST_ENABLED: &str = "false";
const DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED: &str = "true";
const DEFAULT_MESSAGE_CACHE_CONTROL_REWRITE: &str = "off";
const DEFAULT_MESSAGE_TOP_FIELD_FILTER_ENABLED: &str = "true";
const DEFAULT_NON_STREAM_PROBE_CACHE_ENABLED: &str = "false";
const DEFAULT_PASSTHROUGH_OS_VERSION: &str = "false";
const DEFAULT_PASSTHROUGH_SHELL: &str = "false";
const DEFAULT_PASSTHROUGH_WORKING_DIR: &str = "true";
const DEFAULT_PROXY_CLIENT_POOL_ENABLED: &str = "true";
const DEFAULT_REQUEST_DIAGNOSTICS_LOG_MODE: &str = "info";
const DEFAULT_REWRITE_DISABLED_THINKING_ENABLED: &str = "false";
const DEFAULT_REWRITE_DISABLED_THINKING_MODELS: &str = "claude-fable-5";
const DEFAULT_SESSION_HELLO_PROBE_ENABLED: &str = "false";
const DEFAULT_SESSION_HELLO_PROBE_FAILURE_COOLDOWN_SECS: &str = "300";
const DEFAULT_SESSION_HELLO_PROBE_STRICT: &str = "false";
const DEFAULT_SESSION_HELLO_PROBE_SUCCESS_TTL_SECS: &str = "3600";
const DEFAULT_SESSION_HELLO_PROBE_TIMEOUT_SECS: &str = "5";
const DEFAULT_STREAM_KEEPALIVE_ENABLED: &str = "false";
const DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECS: &str = "45";
const DEFAULT_STREAM_UPSTREAM_IDLE_TIMEOUT_SECS: &str = "120";
const DEFAULT_UPSTREAM_SESSION_POOL_SIZE: i32 = 3;
const DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY: &str = "mapped_request";
const DEFAULT_UPSTREAM_SESSION_TTL_MINUTES: i32 = 60;
const V3_SCHEDULING_DEFAULTS: &[(&str, &str)] = &[
    ("v3_scheduling_mode", "smart"),
    ("v3_admission_wait_seconds", "120"),
    ("v3_smart_weight_load", "1"),
    ("v3_smart_weight_quota", "0.8"),
    ("v3_smart_weight_health", "1"),
    ("v3_smart_weight_latency", "0.5"),
];

/// 初始迁移使用的历史身份字段，避免依赖后来运行时画像。
struct BaselineProfile {
    key: &'static str,
    version: &'static str,
    version_base: &'static str,
    build_time: &'static str,
    stainless_runtime_version: &'static str,
}
const BASELINE_PROFILES: &[BaselineProfile] = &[
    BaselineProfile {
        key: "2.1.293",
        version: "2.1.293",
        version_base: "2.1.293",
        build_time: "2026-10-07T06:36:42Z",
        stainless_runtime_version: "v26.3.0",
    },
    BaselineProfile {
        key: "2.1.280",
        version: "2.1.280",
        version_base: "2.1.280",
        build_time: "2026-09-21T20:40:17Z",
        stainless_runtime_version: "v26.3.0",
    },
    BaselineProfile {
        key: "2.1.260",
        version: "2.1.260",
        version_base: "2.1.260",
        build_time: "2026-09-03T19:41:35Z",
        stainless_runtime_version: "v26.3.0",
    },
    BaselineProfile {
        key: "2.1.257",
        version: "2.1.257",
        version_base: "2.1.257",
        build_time: "2026-09-01T05:28:54Z",
        stainless_runtime_version: "v26.3.0",
    },
    BaselineProfile {
        key: "2.1.220",
        version: "2.1.220",
        version_base: "2.1.220",
        build_time: "2026-07-24T22:17:45Z",
        stainless_runtime_version: "v26.3.0",
    },
    BaselineProfile {
        key: "2.1.185",
        version: "2.1.185",
        version_base: "2.1.185",
        build_time: "2026-06-20T06:38:30Z",
        stainless_runtime_version: "v24.3.0",
    },
    BaselineProfile {
        key: "2.1.195",
        version: "2.1.195",
        version_base: "2.1.195",
        build_time: "2026-06-26T01:00:56Z",
        stainless_runtime_version: "v26.3.0",
    },
    BaselineProfile {
        key: "2.1.187",
        version: "2.1.187",
        version_base: "2.1.187",
        build_time: "2026-06-23T16:59:46Z",
        stainless_runtime_version: "v24.3.0",
    },
    BaselineProfile {
        key: "2.1.173",
        version: "2.1.173",
        version_base: "2.1.173",
        build_time: "2026-06-11T01:23:13Z",
        stainless_runtime_version: "v24.3.0",
    },
    BaselineProfile {
        key: "2.1.197",
        version: "2.1.197",
        version_base: "2.1.197",
        build_time: "2026-06-29T19:08:42Z",
        stainless_runtime_version: "v26.3.0",
    },
];

const PREVIOUS_DEFAULT_CLAUDE_CODE_PROFILE_SETTINGS: &[(&str, &str)] = &[
    ("2.1.187", "2.1.89-2.1.187"),
    ("2.1.195", "2.1.89-2.1.195"),
    ("2.1.197", "2.1.89-2.1.197"),
    ("2.1.220", "2.1.89-2.1.220"),
    ("2.1.257", "2.1.89-2.1.257"),
    ("2.1.260", "2.1.89-2.1.260"),
    ("2.1.280", "2.1.89-2.1.280"),
];
const PREVIOUS_DEFAULT_ALLOW_SYSTEM_ROLE_MODELS: &str = "claude-opus-4-8";
const PREVIOUS_DEFAULT_ALLOW_SYSTEM_ROLE_MODELS_2_1_260: &str =
    "claude-opus-5,claude-fable-5,claude-fable-5-1,claude-opus-4-8";
const PREVIOUS_DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS: &str = r#"[{"model":"claude-fable-5[1m]","name":"Fable","description":"Most capable for your hardest and longest-running tasks","disabled_reason":null}]"#;
const PREVIOUS_DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS: &str =
    "claude-fable-5,claude-opus-4-8,claude-opus-4-7";
const MIGRATION_ALLOW_1M_MODELS_2_1_197_KEY: &str = "migration_allow_1m_models_2_1_197_done";
const MIGRATION_ALLOW_SYSTEM_ROLE_FABLE_5_1_KEY: &str =
    "migration_allow_system_role_fable_5_1_done";
const OBSOLETE_SETTINGS_KEYS: &[&str] = &[
    "intercept_warmup_non_stream_aux_enabled",
    "intercept_warmup_non_stream_aux_mode",
];

/// 执行既有数据库基线，保持原默认配置和条件回填。
/// @param pool 已建立的数据库连接池。
/// @param driver SQLite 或 PostgreSQL 驱动。
/// @return 完整初始化结果或数据库错误。
pub async fn migrate_legacy(pool: &AnyPool, driver: &str) -> Result<(), sqlx::Error> {
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
    // 回执和凭据必须同事务提交；仅保存摘要，避免形成第二份令牌存储。
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS oauth_credential_imports (
        operation_id TEXT PRIMARY KEY,
        account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
        request_hash TEXT NOT NULL,
        created_at TEXT NOT NULL
    )",
    )
    .execute(pool)
    .await?;
    for statement in [
        "CREATE TABLE IF NOT EXISTS oauth_account_ownerships (
            account_id BIGINT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
            record_json TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS oauth_handoff_operations (
            operation_id TEXT PRIMARY KEY,
            account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            record_json TEXT NOT NULL)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    migrate_usage(pool).await?;
    migrate_diagnostics(pool).await?;
    migrate_scheduling(pool).await?;
    // 增量迁移
    sqlx::query("ALTER TABLE accounts ADD COLUMN request_backend TEXT NOT NULL DEFAULT 'legacy'")
        .execute(pool)
        .await
        .ok();
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
    sqlx::query("ALTER TABLE accounts ADD COLUMN rate_limit_tier TEXT")
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
    sqlx::query(&format!(
        "ALTER TABLE accounts ADD COLUMN allow_1m_models TEXT NOT NULL DEFAULT '{}'",
        DEFAULT_ALLOW_1M_MODELS
    ))
    .execute(pool)
    .await
    .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN allow_fast_mode INTEGER NOT NULL DEFAULT 0")
        .execute(pool)
        .await
        .ok();
    sqlx::query("ALTER TABLE accounts ADD COLUMN rpm_limit INTEGER NOT NULL DEFAULT 0")
        .execute(pool)
        .await
        .ok();
    for column in [
        "five_hour_usage_limit_percent",
        "seven_day_usage_limit_percent",
    ] {
        sqlx::query(&format!(
            "ALTER TABLE accounts ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
        ))
        .execute(pool)
        .await
        .ok();
    }
    sqlx::query(
        "ALTER TABLE accounts ADD COLUMN upstream_session_pool_enabled INTEGER NOT NULL DEFAULT 0",
    )
    .execute(pool)
    .await
    .ok();
    sqlx::query(&format!(
        "ALTER TABLE accounts ADD COLUMN upstream_session_pool_size INTEGER NOT NULL DEFAULT {}",
        DEFAULT_UPSTREAM_SESSION_POOL_SIZE
    ))
    .execute(pool)
    .await
    .ok();
    sqlx::query(&format!(
        "ALTER TABLE accounts ADD COLUMN upstream_session_ttl_minutes INTEGER NOT NULL DEFAULT {}",
        DEFAULT_UPSTREAM_SESSION_TTL_MINUTES
    ))
    .execute(pool)
    .await
    .ok();
    sqlx::query(&format!(
        "ALTER TABLE accounts ADD COLUMN upstream_session_refresh_policy TEXT NOT NULL DEFAULT '{}'",
        DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY
    ))
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
    for (key, val) in [
        ("score_weight_7d", "0.5"),
        ("score_weight_5h", "0.3"),
        ("score_weight_concurrency", "0.2"),
        // 峰值预热相关默认值
        ("peak_prime_enabled", "true"),
        ("peak_prime_hours", "4,5,6"),
        ("peak_prime_model", "claude-haiku-4-5-20251001"),
        // Claude Code Opus 5、Fable 5 与 Opus 4.8 会在 messages 中携带 role=system。
        ("allow_system_role_models", DEFAULT_ALLOW_SYSTEM_ROLE_MODELS),
        (
            "allowed_claude_code_versions",
            DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING,
        ),
        (
            "blocked_claude_code_versions",
            DEFAULT_BLOCKED_CLAUDE_CODE_VERSIONS_SETTING,
        ),
        (
            "claude_code_version_profile",
            DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING,
        ),
        (
            "claude_code_profile_selection_mode",
            DEFAULT_CLAUDE_CODE_PROFILE_SELECTION_MODE_SETTING,
        ),
        ("allowed_user_agents", DEFAULT_ALLOWED_USER_AGENTS_SETTING),
        // Claude Code base URL 风险治理默认只观测并输出脱敏摘要,不改写请求体。
        (
            "claude_code_context_sanitizer_mode",
            DEFAULT_CLAUDE_CODE_CONTEXT_SANITIZER_MODE,
        ),
        // 系统提示词环境字段「真值透传」开关:工作目录默认透传,shell/os_version 默认改写。
        ("passthrough_shell", DEFAULT_PASSTHROUGH_SHELL),
        ("passthrough_os_version", DEFAULT_PASSTHROUGH_OS_VERSION),
        ("passthrough_working_dir", DEFAULT_PASSTHROUGH_WORKING_DIR),
        // Anthropic cache_control TTL 改写默认关闭,避免升级后改变缓存断点语义。
        (
            "cache_control_ttl_rewrite",
            DEFAULT_CACHE_CONTROL_TTL_REWRITE,
        ),
        // Claude Code messages 缓存断点稳定化默认关闭,避免升级后改变缓存断点布局。
        (
            "message_cache_control_rewrite",
            DEFAULT_MESSAGE_CACHE_CONTROL_REWRITE,
        ),
        // API mimicry `/v1/messages` 顶层字段顺序默认对齐真实 Claude Code 抓包;可在设置页关闭回滚。
        (
            "message_body_order_fingerprint_enabled",
            DEFAULT_MESSAGE_BODY_ORDER_FINGERPRINT_ENABLED,
        ),
        (
            "message_top_field_filter_enabled",
            DEFAULT_MESSAGE_TOP_FIELD_FILTER_ENABLED,
        ),
        // Fable 周用量明确耗尽时默认允许打破 sticky,避免有可用账号却持续命中满额账号。
        (
            "fable_sticky_quota_fallback_enabled",
            DEFAULT_FABLE_STICKY_QUOTA_FALLBACK_ENABLED,
        ),
        // Fable 周用量达到全局控制线后停止继续分配新请求,默认保留一半额度。
        (
            "fable_weekly_usage_limit_percent",
            DEFAULT_FABLE_WEEKLY_USAGE_LIMIT_PERCENT,
        ),
        // 代理 reqwest Client 连接池默认开启,可通过设置页关闭用于排查连接复用问题。
        (
            "proxy_client_pool_enabled",
            DEFAULT_PROXY_CLIENT_POOL_ENABLED,
        ),
        // 预热与 Auto Mode classifier 本地处理默认不改变转发行为。
        (
            "intercept_warmup_title_enabled",
            DEFAULT_INTERCEPT_WARMUP_TITLE_ENABLED,
        ),
        (
            "intercept_warmup_suggestion_enabled",
            DEFAULT_INTERCEPT_WARMUP_SUGGESTION_ENABLED,
        ),
        (
            "intercept_warmup_haiku_probe_enabled",
            DEFAULT_INTERCEPT_WARMUP_HAIKU_PROBE_ENABLED,
        ),
        (
            "intercept_auto_mode_classifier_stage1_mode",
            DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE1_MODE,
        ),
        (
            "intercept_auto_mode_classifier_stage2_mode",
            DEFAULT_INTERCEPT_AUTO_MODE_CLASSIFIER_STAGE2_MODE,
        ),
        (
            "intercept_cli_bg_status_classifier_mode",
            DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_MODE,
        ),
        (
            "intercept_cli_bg_status_classifier_models",
            DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_MODELS,
        ),
        (
            "intercept_cli_bg_status_classifier_identity_injection_enabled",
            DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_IDENTITY_INJECTION_ENABLED,
        ),
        // thinking.type=disabled 兼容改写默认关闭,管理员确认模型后再开启。
        (
            "rewrite_disabled_thinking_enabled",
            DEFAULT_REWRITE_DISABLED_THINKING_ENABLED,
        ),
        (
            "rewrite_disabled_thinking_models",
            DEFAULT_REWRITE_DISABLED_THINKING_MODELS,
        ),
        // assistant prefill 本地拦截默认关闭,避免升级后改变转发行为。
        (
            "intercept_assistant_prefill_enabled",
            DEFAULT_INTERCEPT_ASSISTANT_PREFILL_ENABLED,
        ),
        (
            "intercept_assistant_prefill_models",
            DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
        ),
        // 429 请求观测默认关闭,开启后只输出脱敏和截断后的请求信息。
        (
            "request_diagnostics_log_mode",
            DEFAULT_REQUEST_DIAGNOSTICS_LOG_MODE,
        ),
        ("log_429_request_enabled", DEFAULT_LOG_429_REQUEST_ENABLED),
        (
            "log_non_stream_request_enabled",
            DEFAULT_LOG_NON_STREAM_REQUEST_ENABLED,
        ),
        (
            "non_stream_probe_cache_enabled",
            DEFAULT_NON_STREAM_PROBE_CACHE_ENABLED,
        ),
        (
            "log_429_request_body_limit",
            DEFAULT_LOG_429_REQUEST_BODY_LIMIT,
        ),
        // 流式稳定性默认关闭,管理员确认 watchdog fallback 后再开启。
        ("stream_keepalive_enabled", DEFAULT_STREAM_KEEPALIVE_ENABLED),
        (
            "stream_keepalive_interval_secs",
            DEFAULT_STREAM_KEEPALIVE_INTERVAL_SECS,
        ),
        (
            "stream_upstream_idle_timeout_secs",
            DEFAULT_STREAM_UPSTREAM_IDLE_TIMEOUT_SECS,
        ),
        // Claude Code bootstrap 模型选项默认透传上游;管理员可切换为配置列表或隐藏 Fable。
        (
            "bootstrap_model_options_mode",
            DEFAULT_BOOTSTRAP_MODEL_OPTIONS_MODE,
        ),
        (
            "bootstrap_additional_model_options",
            DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS,
        ),
        // 有效上游 Session 首次 Hello 代理探测默认关闭；旧库只补缺失键，不覆盖管理员配置。
        (
            "session_hello_probe_enabled",
            DEFAULT_SESSION_HELLO_PROBE_ENABLED,
        ),
        (
            "session_hello_probe_strict",
            DEFAULT_SESSION_HELLO_PROBE_STRICT,
        ),
        (
            "session_hello_probe_timeout_secs",
            DEFAULT_SESSION_HELLO_PROBE_TIMEOUT_SECS,
        ),
        (
            "session_hello_probe_success_ttl_secs",
            DEFAULT_SESSION_HELLO_PROBE_SUCCESS_TTL_SECS,
        ),
        (
            "session_hello_probe_failure_cooldown_secs",
            DEFAULT_SESSION_HELLO_PROBE_FAILURE_COOLDOWN_SECS,
        ),
    ]
    .into_iter()
    .chain(V3_SCHEDULING_DEFAULTS.iter().copied())
    {
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
    upgrade_default_profile_setting(pool, driver).await?;
    upgrade_default_model_settings(pool).await?;
    upgrade_system_role_models_for_fable_5_1(pool).await?;
    upgrade_default_bootstrap_model_options(pool).await?;
    upgrade_default_allow_1m_models(pool).await?;
    remove_obsolete_settings(pool).await?;

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
    connection: &mut AnyConnection,
) -> Result<&'static BaselineProfile, sqlx::Error> {
    let configured: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
        .bind("claude_code_version_profile")
        .fetch_optional(connection)
        .await?;
    Ok(configured
        .as_deref()
        .and_then(|key| {
            BASELINE_PROFILES
                .iter()
                .find(|profile| profile.key == key.trim())
        })
        .unwrap_or(&BASELINE_PROFILES[0]))
}

async fn upgrade_default_allow_1m_models(pool: &AnyPool) -> Result<(), sqlx::Error> {
    let already_done: Option<String> =
        sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind(MIGRATION_ALLOW_1M_MODELS_2_1_197_KEY)
            .fetch_optional(pool)
            .await?;
    if already_done.is_some() {
        return Ok(());
    }

    sqlx::query("UPDATE accounts SET allow_1m_models=$1 WHERE allow_1m_models=$2")
        .bind(DEFAULT_ALLOW_1M_MODELS)
        .bind("opus")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ($1, $2) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(MIGRATION_ALLOW_1M_MODELS_2_1_197_KEY)
    .bind("true")
    .execute(pool)
    .await?;
    Ok(())
}

// 只有精确旧出厂 profile/range 配对会升级，管理员自定义禁止列表和 UA 不参与改写。
async fn upgrade_default_profile_setting(pool: &AnyPool, driver: &str) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for (previous_profile, previous_allowed) in PREVIOUS_DEFAULT_CLAUDE_CODE_PROFILE_SETTINGS {
        let changed = sqlx::query(
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
              AND EXISTS (
                  SELECT 1
                  FROM settings selection_mode
                  WHERE selection_mode.key=$6
                    AND selection_mode.value=$7
              )
            "#,
        )
        .bind(DEFAULT_CLAUDE_CODE_VERSION_PROFILE_SETTING)
        .bind("claude_code_version_profile")
        .bind(previous_profile)
        .bind("allowed_claude_code_versions")
        .bind(previous_allowed)
        .bind("claude_code_profile_selection_mode")
        .bind("client_version")
        .execute(&mut *tx)
        .await?;
        if changed.rows_affected() > 0 {
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2 AND value=$3")
                .bind(DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS_SETTING)
                .bind("allowed_claude_code_versions")
                .bind(previous_allowed)
                .execute(&mut *tx)
                .await?;
        }
    }
    let profile = selected_claude_code_profile(&mut tx).await?;
    upgrade_account_claude_code_profile(&mut tx, driver, profile).await?;
    tx.commit().await
}

async fn upgrade_default_model_settings(pool: &AnyPool) -> Result<(), sqlx::Error> {
    for (key, previous, current) in [
        (
            "allow_system_role_models",
            PREVIOUS_DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
            DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
        ),
        (
            "allow_system_role_models",
            PREVIOUS_DEFAULT_ALLOW_SYSTEM_ROLE_MODELS_2_1_260,
            DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
        ),
        (
            "intercept_assistant_prefill_models",
            PREVIOUS_DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
            DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
        ),
    ] {
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2 AND value=$3")
            .bind(current)
            .bind(key)
            .bind(previous)
            .execute(pool)
            .await?;
    }
    Ok(())
}

async fn upgrade_system_role_models_for_fable_5_1(pool: &AnyPool) -> Result<(), sqlx::Error> {
    let already_done: Option<String> =
        sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind(MIGRATION_ALLOW_SYSTEM_ROLE_FABLE_5_1_KEY)
            .fetch_optional(pool)
            .await?;
    if already_done.is_some() {
        return Ok(());
    }

    let configured: Option<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
        .bind("allow_system_role_models")
        .fetch_optional(pool)
        .await?;
    if let Some(configured) = configured.filter(|value| !value.trim().is_empty()) {
        let mut models = Vec::new();
        for model in configured
            .split(',')
            .map(str::trim)
            .filter(|model| !model.is_empty())
        {
            if !models.iter().any(|existing| existing == model) {
                models.push(model.to_string());
            }
        }
        if !models.iter().any(|model| model == "claude-fable-5-1") {
            models.push("claude-fable-5-1".into());
        }
        let upgraded = models.join(",");
        if upgraded != configured {
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(upgraded)
                .bind("allow_system_role_models")
                .execute(pool)
                .await?;
        }
    }

    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ($1, $2) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
    )
    .bind(MIGRATION_ALLOW_SYSTEM_ROLE_FABLE_5_1_KEY)
    .bind("true")
    .execute(pool)
    .await?;
    Ok(())
}

async fn upgrade_default_bootstrap_model_options(pool: &AnyPool) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE settings SET value=$1 WHERE key=$2 AND value=$3")
        .bind(DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS)
        .bind("bootstrap_additional_model_options")
        .bind(PREVIOUS_DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS)
        .execute(pool)
        .await?;
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
    connection: &mut AnyConnection,
    driver: &str,
    profile: &BaselineProfile,
) -> Result<(), sqlx::Error> {
    let identity = profile;
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
        .execute(connection)
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
    request_backend TEXT NOT NULL DEFAULT 'legacy',
    concurrency     INTEGER NOT NULL DEFAULT 3,
    priority        INTEGER NOT NULL DEFAULT 50,
    rpm_limit       INTEGER NOT NULL DEFAULT 0,
    five_hour_usage_limit_percent INTEGER NOT NULL DEFAULT 0,
    seven_day_usage_limit_percent INTEGER NOT NULL DEFAULT 0,
    allow_fast_mode INTEGER NOT NULL DEFAULT 0,
    upstream_session_pool_enabled INTEGER NOT NULL DEFAULT 0,
    upstream_session_pool_size INTEGER NOT NULL DEFAULT 3,
    upstream_session_ttl_minutes INTEGER NOT NULL DEFAULT 60,
    upstream_session_refresh_policy TEXT NOT NULL DEFAULT 'mapped_request',
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
    request_backend TEXT NOT NULL DEFAULT 'legacy',
    concurrency     INT NOT NULL DEFAULT 3,
    priority        INT NOT NULL DEFAULT 50,
    rpm_limit       INT NOT NULL DEFAULT 0,
    five_hour_usage_limit_percent INT NOT NULL DEFAULT 0,
    seven_day_usage_limit_percent INT NOT NULL DEFAULT 0,
    allow_fast_mode INT NOT NULL DEFAULT 0,
    upstream_session_pool_enabled INT NOT NULL DEFAULT 0,
    upstream_session_pool_size INT NOT NULL DEFAULT 3,
    upstream_session_ttl_minutes INT NOT NULL DEFAULT 60,
    upstream_session_refresh_policy TEXT NOT NULL DEFAULT 'mapped_request',
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

/// 建立原有usage持久结构。
/// @param pool 已建立的数据库池。
/// @return 初始化结果。
pub(crate) async fn migrate_usage(pool: &AnyPool) -> Result<(), sqlx::Error> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS usage_records (
            record_id TEXT PRIMARY KEY,
            account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            model TEXT NOT NULL,
            input_tokens BIGINT NOT NULL,
            output_tokens BIGINT NOT NULL,
            cache_read_tokens BIGINT NOT NULL,
            cache_write_tokens BIGINT NOT NULL,
            cache_write_5m_tokens BIGINT NOT NULL,
            cache_write_1h_tokens BIGINT NOT NULL,
            cost_usd DOUBLE PRECISION,
            partial BIGINT NOT NULL,
            price_version TEXT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS usage_records_account_time ON usage_records(account_id, started_at)",
        "CREATE INDEX IF NOT EXISTS usage_records_time ON usage_records(started_at)",
        "CREATE TABLE IF NOT EXISTS usage_stats_meta (id BIGINT PRIMARY KEY, recorded_since TEXT NOT NULL)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    sqlx::query(
        "INSERT INTO usage_stats_meta(id, recorded_since) VALUES(1, $1) ON CONFLICT(id) DO NOTHING",
    )
    .bind(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
    .execute(pool)
    .await?;
    Ok(())
}

/// 建立原有diagnostics持久结构。
/// @param pool 已建立的数据库池。
/// @return 初始化结果。
pub(crate) async fn migrate_diagnostics(pool: &AnyPool) -> Result<(), sqlx::Error> {
    for sql in [
        "CREATE TABLE IF NOT EXISTS request_diagnostics (
            request_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
            response_request_id TEXT, account_id BIGINT, outcome TEXT NOT NULL, summary_json TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS request_diagnostic_attempts (
            attempt_id TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES request_diagnostics(request_id) ON DELETE CASCADE,
            ordinal BIGINT NOT NULL, account_id BIGINT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
            upstream_request_id TEXT, summary_json TEXT NOT NULL)",
        "CREATE INDEX IF NOT EXISTS request_diagnostics_time ON request_diagnostics(started_at,request_id)",
        "CREATE INDEX IF NOT EXISTS request_diagnostics_response_id ON request_diagnostics(response_request_id)",
        "CREATE INDEX IF NOT EXISTS request_diagnostic_attempts_request ON request_diagnostic_attempts(request_id,ordinal)",
        "CREATE INDEX IF NOT EXISTS request_diagnostic_attempts_account ON request_diagnostic_attempts(account_id,request_id)",
        "CREATE INDEX IF NOT EXISTS request_diagnostic_attempts_upstream ON request_diagnostic_attempts(upstream_request_id)",
    ] { sqlx::query(sql).execute(pool).await?; }
    Ok(())
}

/// 建立原有scheduling持久结构。
/// @param pool 已建立的数据库池。
/// @return 初始化结果。
pub(crate) async fn migrate_scheduling(pool: &AnyPool) -> Result<(), SqlxError> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS gateway_session_bindings (session_key TEXT PRIMARY KEY, account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE, revision TEXT NOT NULL, created_at_ms BIGINT NOT NULL, last_seen_at_ms BIGINT NOT NULL, expires_at_ms BIGINT NOT NULL)",
        "CREATE INDEX IF NOT EXISTS gateway_bindings_account_expiry ON gateway_session_bindings(account_id, expires_at_ms)",
        "CREATE INDEX IF NOT EXISTS gateway_bindings_expiry ON gateway_session_bindings(expires_at_ms)",
        "CREATE TABLE IF NOT EXISTS gateway_account_send_state (account_id BIGINT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE, last_admitted_at_ms BIGINT NOT NULL)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::migrate_legacy as migrate;
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
        for (previous_profile, previous_allowed) in PREVIOUS_DEFAULT_CLAUDE_CODE_PROFILE_SETTINGS {
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(previous_profile)
                .bind("claude_code_version_profile")
                .execute(&pool)
                .await
                .expect("set old profile default");
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(previous_allowed)
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
    async fn migrate_upgrades_only_exact_old_default_model_lists() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");

        for (key, previous, expected) in [
            (
                "allow_system_role_models",
                PREVIOUS_DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
                crate::store::settings_store::DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
            ),
            (
                "allow_system_role_models",
                PREVIOUS_DEFAULT_ALLOW_SYSTEM_ROLE_MODELS_2_1_260,
                crate::store::settings_store::DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
            ),
            (
                "intercept_assistant_prefill_models",
                PREVIOUS_DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
                crate::store::settings_store::DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
            ),
        ] {
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(previous)
                .bind(key)
                .execute(&pool)
                .await
                .expect("set old default");
            migrate(&pool, "sqlite").await.expect("upgrade old default");
            let upgraded: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind(key)
                .fetch_one(&pool)
                .await
                .expect("upgraded setting");
            assert_eq!(upgraded, expected);

            let custom = format!("{},custom-model", previous);
            sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
                .bind(&custom)
                .bind(key)
                .execute(&pool)
                .await
                .expect("set custom value");
            migrate(&pool, "sqlite")
                .await
                .expect("preserve custom value");
            let retained: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind(key)
                .fetch_one(&pool)
                .await
                .expect("retained setting");
            assert_eq!(retained, custom);
        }
    }

    #[tokio::test]
    async fn migrate_appends_fable_5_1_and_preserves_unrelated_model_settings() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        sqlx::query("DELETE FROM settings WHERE key=$1")
            .bind(MIGRATION_ALLOW_SYSTEM_ROLE_FABLE_5_1_KEY)
            .execute(&pool)
            .await
            .expect("reset migration marker");
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("claude-opus-5,claude-fable-5,claude-opus-4-8,claude-sonnet-5")
            .bind("allow_system_role_models")
            .execute(&pool)
            .await
            .expect("set online system role models");
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("claude-fable-5,custom-prefill")
            .bind("intercept_assistant_prefill_models")
            .execute(&pool)
            .await
            .expect("set custom prefill models");
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("claude-fable-5")
            .bind("rewrite_disabled_thinking_models")
            .execute(&pool)
            .await
            .expect("set disabled thinking models");

        migrate(&pool, "sqlite").await.expect("upgrade fable 5.1");

        let system_role_models: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind("allow_system_role_models")
                .fetch_one(&pool)
                .await
                .expect("system role models");
        assert_eq!(
            system_role_models,
            "claude-opus-5,claude-fable-5,claude-opus-4-8,claude-sonnet-5,claude-fable-5-1"
        );
        let prefill_models: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("intercept_assistant_prefill_models")
            .fetch_one(&pool)
            .await
            .expect("prefill models");
        assert_eq!(prefill_models, "claude-fable-5,custom-prefill");
        let disabled_thinking_models: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind("rewrite_disabled_thinking_models")
                .fetch_one(&pool)
                .await
                .expect("disabled thinking models");
        assert_eq!(disabled_thinking_models, "claude-fable-5");
    }

    #[tokio::test]
    async fn migrate_upgrades_only_old_default_bootstrap_model_option() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind(PREVIOUS_DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS)
            .bind("bootstrap_additional_model_options")
            .execute(&pool)
            .await
            .expect("set previous bootstrap default");

        migrate(&pool, "sqlite")
            .await
            .expect("upgrade bootstrap default");
        let upgraded: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("bootstrap_additional_model_options")
            .fetch_one(&pool)
            .await
            .expect("upgraded bootstrap option");
        assert_eq!(
            upgraded,
            crate::store::settings_store::DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS
        );

        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind(r#"[{"model":"custom-fable"}]"#)
            .bind("bootstrap_additional_model_options")
            .execute(&pool)
            .await
            .expect("set custom bootstrap option");
        migrate(&pool, "sqlite")
            .await
            .expect("preserve custom bootstrap");
        let custom: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("bootstrap_additional_model_options")
            .fetch_one(&pool)
            .await
            .expect("custom bootstrap option");
        assert_eq!(custom, r#"[{"model":"custom-fable"}]"#);
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
            ("2.1.195", "2.1.195", "2026-06-26T01:00:56Z"),
            ("2.1.197", "2.1.197", "2026-06-29T19:08:42Z"),
            ("2.1.220", "2.1.220", "2026-07-24T22:17:45Z"),
            ("2.1.257", "2.1.257", "2026-09-01T05:28:54Z"),
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
    async fn migrate_backfills_selection_mode_without_overwriting_explicit_value() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.unwrap();
        sqlx::query("DELETE FROM settings WHERE key=$1")
            .bind("claude_code_profile_selection_mode")
            .execute(&pool)
            .await
            .unwrap();
        migrate(&pool, "sqlite").await.unwrap();
        let value: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("claude_code_profile_selection_mode")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(value, "client_version");
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("account")
            .bind("claude_code_profile_selection_mode")
            .execute(&pool)
            .await
            .unwrap();
        migrate(&pool, "sqlite").await.unwrap();
        let value: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("claude_code_profile_selection_mode")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(value, "account");
    }

    #[tokio::test]
    async fn migrate_creates_accounts_with_fast_mode_disabled_by_default() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");
        sqlx::query(
            r#"INSERT INTO accounts (
                email, token, device_id, canonical_env, canonical_prompt_env, canonical_process
            ) VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind("fast-default@example.com")
        .bind("token")
        .bind("device-1")
        .bind("{}")
        .bind("{}")
        .bind("{}")
        .execute(&pool)
        .await
        .expect("insert account");

        let allow_fast_mode: i32 =
            sqlx::query_scalar("SELECT allow_fast_mode FROM accounts WHERE email=$1")
                .bind("fast-default@example.com")
                .fetch_one(&pool)
                .await
                .expect("allow_fast_mode");

        assert_eq!(allow_fast_mode, 0);
    }

    #[tokio::test]
    async fn migrate_upgrades_only_old_default_allow_1m_models() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        sqlx::query("DELETE FROM settings WHERE key=$1")
            .bind(MIGRATION_ALLOW_1M_MODELS_2_1_197_KEY)
            .execute(&pool)
            .await
            .expect("remove migration marker");
        sqlx::query(
            r#"INSERT INTO accounts (
                email, token, device_id, canonical_env, canonical_prompt_env, canonical_process, allow_1m_models
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
        )
        .bind("default@example.com")
        .bind("token")
        .bind("device-1")
        .bind("{}")
        .bind("{}")
        .bind("{}")
        .bind("opus")
        .execute(&pool)
        .await
        .expect("insert default account");
        sqlx::query(
            r#"INSERT INTO accounts (
                email, token, device_id, canonical_env, canonical_prompt_env, canonical_process, allow_1m_models
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)"#,
        )
        .bind("custom@example.com")
        .bind("token")
        .bind("device-2")
        .bind("{}")
        .bind("{}")
        .bind("{}")
        .bind("opus,custom")
        .execute(&pool)
        .await
        .expect("insert custom account");

        migrate(&pool, "sqlite").await.expect("second migrate");

        let upgraded: String =
            sqlx::query_scalar("SELECT allow_1m_models FROM accounts WHERE email=$1")
                .bind("default@example.com")
                .fetch_one(&pool)
                .await
                .expect("upgraded allow_1m_models");
        assert_eq!(upgraded, crate::model::account::DEFAULT_ALLOW_1M_MODELS);

        let custom: String =
            sqlx::query_scalar("SELECT allow_1m_models FROM accounts WHERE email=$1")
                .bind("custom@example.com")
                .fetch_one(&pool)
                .await
                .expect("custom allow_1m_models");
        assert_eq!(custom, "opus,custom");

        sqlx::query("UPDATE accounts SET allow_1m_models=$1 WHERE email=$2")
            .bind("opus")
            .bind("default@example.com")
            .execute(&pool)
            .await
            .expect("admin rollback to opus");
        migrate(&pool, "sqlite")
            .await
            .expect("third migrate should not rerun migration");
        let retained: String =
            sqlx::query_scalar("SELECT allow_1m_models FROM accounts WHERE email=$1")
                .bind("default@example.com")
                .fetch_one(&pool)
                .await
                .expect("retained allow_1m_models");
        assert_eq!(retained, "opus");
    }

    #[tokio::test]
    async fn migrate_inserts_default_claude_code_context_sanitizer_mode() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let mode: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("claude_code_context_sanitizer_mode")
            .fetch_one(&pool)
            .await
            .expect("context sanitizer setting");

        assert_eq!(
            mode,
            crate::store::settings_store::DEFAULT_CLAUDE_CODE_CONTEXT_SANITIZER_MODE
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

    /// 新库与旧库缺键默认开启，重复迁移保留显式关闭。
    /// @return 无；缺省补齐或幂等保留失效时失败。
    #[tokio::test]
    async fn migrate_message_top_field_filter_defaults_and_preserves_false() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.unwrap();
        for stored in [None, Some("false"), Some("false")] {
            sqlx::query("DELETE FROM settings WHERE key=$1")
                .bind("message_top_field_filter_enabled")
                .execute(&pool)
                .await
                .unwrap();
            if let Some(stored) = stored {
                sqlx::query("INSERT INTO settings (key,value) VALUES ($1,$2)")
                    .bind("message_top_field_filter_enabled")
                    .bind(stored)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            migrate(&pool, "sqlite").await.unwrap();
            let value: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind("message_top_field_filter_enabled")
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(value, stored.unwrap_or("true"));
        }
    }

    /// 新旧库仅补缺失模式，重复迁移保留管理员显式 debug。
    #[tokio::test]
    async fn migrate_request_log_mode_preserves_explicit_debug() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.unwrap();
        let read = || {
            sqlx::query_scalar::<_, String>(
                "SELECT value FROM settings WHERE key='request_diagnostics_log_mode'",
            )
        };
        assert_eq!(read().fetch_one(&pool).await.unwrap(), "info");
        sqlx::query("UPDATE settings SET value='debug' WHERE key='request_diagnostics_log_mode'")
            .execute(&pool)
            .await
            .unwrap();
        migrate(&pool, "sqlite").await.unwrap();
        assert_eq!(read().fetch_one(&pool).await.unwrap(), "debug");
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
    async fn migrate_inserts_cli_bg_status_classifier_default_without_overwriting_custom_value() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let mode: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("intercept_cli_bg_status_classifier_mode")
            .fetch_one(&pool)
            .await
            .expect("cli-bg status classifier setting");
        assert_eq!(
            mode,
            crate::store::settings_store::DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_MODE
        );
        let models: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("intercept_cli_bg_status_classifier_models")
            .fetch_one(&pool)
            .await
            .expect("cli-bg status classifier model list");
        assert_eq!(
            models,
            crate::store::settings_store::DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_MODELS
        );

        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("mock")
            .bind("intercept_cli_bg_status_classifier_mode")
            .execute(&pool)
            .await
            .expect("update cli-bg status classifier setting");
        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("claude-opus-5-5")
            .bind("intercept_cli_bg_status_classifier_models")
            .execute(&pool)
            .await
            .expect("update cli-bg status classifier model list");
        migrate(&pool, "sqlite").await.expect("second migrate");

        let retained: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("intercept_cli_bg_status_classifier_mode")
            .fetch_one(&pool)
            .await
            .expect("retained cli-bg status classifier setting");
        assert_eq!(retained, "mock");
        let retained_models: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("intercept_cli_bg_status_classifier_models")
            .fetch_one(&pool)
            .await
            .expect("retained cli-bg status classifier model list");
        assert_eq!(retained_models, "claude-opus-5-5");
    }

    #[tokio::test]
    async fn migrate_inserts_cli_bg_identity_injection_default_without_overwriting_custom_value() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let enabled: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("intercept_cli_bg_status_classifier_identity_injection_enabled")
            .fetch_one(&pool)
            .await
            .expect("cli-bg identity injection setting");
        assert_eq!(
            enabled,
            crate::store::settings_store::DEFAULT_INTERCEPT_CLI_BG_STATUS_CLASSIFIER_IDENTITY_INJECTION_ENABLED
        );

        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("true")
            .bind("intercept_cli_bg_status_classifier_identity_injection_enabled")
            .execute(&pool)
            .await
            .expect("update cli-bg identity injection setting");
        migrate(&pool, "sqlite").await.expect("second migrate");

        let retained: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("intercept_cli_bg_status_classifier_identity_injection_enabled")
            .fetch_one(&pool)
            .await
            .expect("retained cli-bg identity injection setting");
        assert_eq!(retained, "true");
    }

    #[tokio::test]
    async fn migrate_inserts_fable_sticky_quota_fallback_default() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let enabled: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("fable_sticky_quota_fallback_enabled")
            .fetch_one(&pool)
            .await
            .expect("fable sticky quota fallback setting");

        assert_eq!(
            enabled,
            crate::store::settings_store::DEFAULT_FABLE_STICKY_QUOTA_FALLBACK_ENABLED
        );
    }

    #[tokio::test]
    async fn migrate_inserts_fable_weekly_usage_limit_percent_default() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("migrate");

        let percent: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("fable_weekly_usage_limit_percent")
            .fetch_one(&pool)
            .await
            .expect("fable weekly usage limit percent setting");

        assert_eq!(
            percent,
            crate::store::settings_store::DEFAULT_FABLE_WEEKLY_USAGE_LIMIT_PERCENT
        );
    }

    #[tokio::test]
    async fn migrate_inserts_session_hello_probe_defaults_without_overwriting_custom_value() {
        let pool = make_sqlite_pool().await;
        migrate(&pool, "sqlite").await.expect("initial migrate");
        for (key, expected) in [
            (
                "session_hello_probe_enabled",
                crate::store::settings_store::DEFAULT_SESSION_HELLO_PROBE_ENABLED,
            ),
            (
                "session_hello_probe_strict",
                crate::store::settings_store::DEFAULT_SESSION_HELLO_PROBE_STRICT,
            ),
            (
                "session_hello_probe_timeout_secs",
                crate::store::settings_store::DEFAULT_SESSION_HELLO_PROBE_TIMEOUT_SECS,
            ),
            (
                "session_hello_probe_success_ttl_secs",
                crate::store::settings_store::DEFAULT_SESSION_HELLO_PROBE_SUCCESS_TTL_SECS,
            ),
            (
                "session_hello_probe_failure_cooldown_secs",
                crate::store::settings_store::DEFAULT_SESSION_HELLO_PROBE_FAILURE_COOLDOWN_SECS,
            ),
        ] {
            let value: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
                .bind(key)
                .fetch_one(&pool)
                .await
                .expect("session hello probe default");
            assert_eq!(value, expected, "key={key}");
        }

        sqlx::query("UPDATE settings SET value=$1 WHERE key=$2")
            .bind("true")
            .bind("session_hello_probe_enabled")
            .execute(&pool)
            .await
            .expect("设置管理员自定义值");
        migrate(&pool, "sqlite").await.expect("second migrate");
        let retained: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=$1")
            .bind("session_hello_probe_enabled")
            .fetch_one(&pool)
            .await
            .expect("读取管理员自定义值");
        assert_eq!(retained, "true");
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
