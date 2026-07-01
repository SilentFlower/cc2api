use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use sqlx::AnyPool;
use sqlx::Row;
use sqlx::any::AnyRow;

use crate::error::AppError;
use crate::model::account::{Account, AccountStatus, DEFAULT_ALLOW_1M_MODELS};
use crate::service::version_profile::IdentityProfile;

pub struct AccountStore {
    pool: AnyPool,
    driver: String,
}

impl AccountStore {
    pub fn new(pool: AnyPool, driver: String) -> Self {
        Self { pool, driver }
    }

    fn now_expr(&self) -> &str {
        if self.driver == "sqlite" {
            "strftime('%Y-%m-%dT%H:%M:%SZ','now')"
        } else {
            "NOW()"
        }
    }

    fn is_pg(&self) -> bool {
        self.driver == "postgres"
    }

    fn fmt_time(&self, t: DateTime<Utc>) -> String {
        t.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    }

    /// Returns `$N` for SQLite or `$N::TIMESTAMPTZ` for Postgres
    fn ts(&self, n: u32) -> String {
        if self.is_pg() {
            format!("${}::TIMESTAMPTZ", n)
        } else {
            format!("${}", n)
        }
    }

    fn parse_time(row: &AnyRow, col: &str) -> DateTime<Utc> {
        // SQLite returns string, Postgres returns native
        if let Ok(s) = row.try_get::<String, _>(col) {
            NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%SZ")
                .map(|n| n.and_utc())
                .unwrap_or_default()
        } else {
            Utc::now()
        }
    }

    fn parse_optional_time(row: &AnyRow, col: &str) -> Option<DateTime<Utc>> {
        if let Ok(s) = row.try_get::<Option<String>, _>(col) {
            s.and_then(|s| {
                NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%SZ")
                    .map(|n| n.and_utc())
                    .ok()
            })
        } else {
            None
        }
    }

    fn parse_json(row: &AnyRow, col: &str) -> Value {
        if let Ok(s) = row.try_get::<String, _>(col) {
            serde_json::from_str(&s).unwrap_or_else(|_| Value::Object(Default::default()))
        } else {
            Value::Object(Default::default())
        }
    }

    fn row_to_account(row: &AnyRow) -> Account {
        Account {
            id: row.try_get::<i64, _>("id").unwrap_or_default(),
            name: row.try_get::<String, _>("name").unwrap_or_default(),
            email: row.try_get::<String, _>("email").unwrap_or_default(),
            status: row
                .try_get::<String, _>("status")
                .unwrap_or_else(|_| "active".into())
                .into(),
            auth_type: row
                .try_get::<String, _>("auth_type")
                .unwrap_or_else(|_| "setup_token".into())
                .into(),
            setup_token: row.try_get::<String, _>("token").unwrap_or_default(),
            access_token: row.try_get::<String, _>("access_token").unwrap_or_default(),
            refresh_token: row
                .try_get::<String, _>("refresh_token")
                .unwrap_or_default(),
            expires_at: Self::parse_optional_time(row, "oauth_expires_at"),
            oauth_refreshed_at: Self::parse_optional_time(row, "oauth_refreshed_at"),
            auth_error: row.try_get::<String, _>("auth_error").unwrap_or_default(),
            proxy_url: row.try_get::<String, _>("proxy_url").unwrap_or_default(),
            device_id: row.try_get::<String, _>("device_id").unwrap_or_default(),
            canonical_env: Self::parse_json(row, "canonical_env"),
            canonical_prompt: Self::parse_json(row, "canonical_prompt_env"),
            canonical_process: Self::parse_json(row, "canonical_process"),
            billing_mode: row
                .try_get::<String, _>("billing_mode")
                .unwrap_or_else(|_| "strip".into())
                .into(),
            account_uuid: row
                .try_get::<Option<String>, _>("account_uuid")
                .unwrap_or(None),
            organization_uuid: row
                .try_get::<Option<String>, _>("organization_uuid")
                .unwrap_or(None),
            subscription_type: row
                .try_get::<Option<String>, _>("subscription_type")
                .unwrap_or(None),
            concurrency: row.try_get::<i32, _>("concurrency").unwrap_or(3),
            priority: row.try_get::<i32, _>("priority").unwrap_or(50),
            rpm_limit: row.try_get::<i32, _>("rpm_limit").unwrap_or(0),
            rate_limited_at: Self::parse_optional_time(row, "rate_limited_at"),
            rate_limit_reset_at: Self::parse_optional_time(row, "rate_limit_reset_at"),
            disable_reason: row
                .try_get::<String, _>("disable_reason")
                .unwrap_or_default(),
            auto_telemetry: row.try_get::<i32, _>("auto_telemetry").unwrap_or(0) != 0,
            auto_poll_usage: row.try_get::<i32, _>("auto_poll_usage").unwrap_or(0) != 0,
            allow_1m_models: row
                .try_get::<String, _>("allow_1m_models")
                .unwrap_or_else(|_| DEFAULT_ALLOW_1M_MODELS.into()),
            telemetry_count: row.try_get::<i64, _>("telemetry_count").unwrap_or(0),
            usage_data: Self::parse_json(row, "usage_data"),
            usage_fetched_at: Self::parse_optional_time(row, "usage_fetched_at"),
            created_at: Self::parse_time(row, "created_at"),
            updated_at: Self::parse_time(row, "updated_at"),
        }
    }

    pub async fn create(&self, a: &mut Account) -> Result<(), AppError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) as cnt FROM accounts WHERE email=$1")
            .bind(&a.email)
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        if count > 0 {
            return Err(AppError::BadRequest(format!(
                "email {} already exists",
                a.email
            )));
        }

        let env_str = serde_json::to_string(&a.canonical_env).unwrap_or_else(|_| "{}".into());
        let prompt_str = serde_json::to_string(&a.canonical_prompt).unwrap_or_else(|_| "{}".into());
        let process_str =
            serde_json::to_string(&a.canonical_process).unwrap_or_else(|_| "{}".into());
        let expires_at = a.expires_at.map(|t| self.fmt_time(t));
        let oauth_refreshed_at = a.oauth_refreshed_at.map(|t| self.fmt_time(t));

        let auto_telemetry_int: i32 = if a.auto_telemetry { 1 } else { 0 };
        let auto_poll_usage_int: i32 = if a.auto_poll_usage { 1 } else { 0 };
        let q = format!(
            r#"INSERT INTO accounts (name, email, status, token, proxy_url,
                auth_type, access_token, refresh_token, oauth_expires_at, oauth_refreshed_at, auth_error,
                device_id, canonical_env, canonical_prompt_env, canonical_process,
                billing_mode, account_uuid, organization_uuid, subscription_type,
                concurrency, priority, rpm_limit, auto_telemetry, auto_poll_usage, allow_1m_models)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,{},{},{},$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25)
            RETURNING id, created_at, updated_at"#,
            self.ts(9),
            self.ts(10),
            "$11"
        );
        let row: AnyRow = sqlx::query(&q)
            .bind(&a.name)
            .bind(&a.email)
            .bind(a.status.to_string())
            .bind(&a.setup_token)
            .bind(&a.proxy_url)
            .bind(a.auth_type.to_string())
            .bind(&a.access_token)
            .bind(&a.refresh_token)
            .bind(expires_at)
            .bind(oauth_refreshed_at)
            .bind(&a.auth_error)
            .bind(&a.device_id)
            .bind(&env_str)
            .bind(&prompt_str)
            .bind(&process_str)
            .bind(a.billing_mode.to_string())
            .bind(&a.account_uuid)
            .bind(&a.organization_uuid)
            .bind(&a.subscription_type)
            .bind(a.concurrency)
            .bind(a.priority)
            .bind(a.rpm_limit)
            .bind(auto_telemetry_int)
            .bind(auto_poll_usage_int)
            .bind(&a.allow_1m_models)
            .fetch_one(&self.pool)
            .await?;

        a.id = row.try_get::<i64, _>("id").unwrap_or_default();
        a.created_at = Self::parse_time(&row, "created_at");
        a.updated_at = Self::parse_time(&row, "updated_at");
        Ok(())
    }

    pub async fn update(&self, a: &Account) -> Result<(), AppError> {
        let expires_at = a.expires_at.map(|t| self.fmt_time(t));
        let oauth_refreshed_at = a.oauth_refreshed_at.map(|t| self.fmt_time(t));
        let auto_telemetry_int: i32 = if a.auto_telemetry { 1 } else { 0 };
        let auto_poll_usage_int: i32 = if a.auto_poll_usage { 1 } else { 0 };
        let q = format!(
            r#"UPDATE accounts SET name=$1, email=$2, status=$3, token=$4,
                auth_type=$5, access_token=$6, refresh_token=$7, oauth_expires_at={}, oauth_refreshed_at={},
                auth_error=$10, proxy_url=$11, billing_mode=$12,
                account_uuid=$13, organization_uuid=$14, subscription_type=$15,
                concurrency=$16, priority=$17, rpm_limit=$18, auto_telemetry=$19, auto_poll_usage=$20, allow_1m_models=$21, updated_at={}
            WHERE id=$22"#,
            self.ts(8),
            self.ts(9),
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(&a.name)
            .bind(&a.email)
            .bind(a.status.to_string())
            .bind(&a.setup_token)
            .bind(a.auth_type.to_string())
            .bind(&a.access_token)
            .bind(&a.refresh_token)
            .bind(expires_at)
            .bind(oauth_refreshed_at)
            .bind(&a.auth_error)
            .bind(&a.proxy_url)
            .bind(a.billing_mode.to_string())
            .bind(&a.account_uuid)
            .bind(&a.organization_uuid)
            .bind(&a.subscription_type)
            .bind(a.concurrency)
            .bind(a.priority)
            .bind(a.rpm_limit)
            .bind(auto_telemetry_int)
            .bind(auto_poll_usage_int)
            .bind(&a.allow_1m_models)
            .bind(a.id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 批量覆盖所有账号 canonical env 中的 Claude Code 版本身份字段。
    ///
    /// @param identity 目标版本身份画像。
    /// @return 更新成功返回 `Ok(())`，数据库失败时返回业务错误。
    pub async fn update_all_canonical_env_identity(
        &self,
        identity: &IdentityProfile,
    ) -> Result<(), AppError> {
        let q = if self.is_pg() {
            format!(
                r#"
                UPDATE accounts
                SET canonical_env = jsonb_set(
                        jsonb_set(
                            jsonb_set(
                                jsonb_set(canonical_env, '{{version}}', to_jsonb($1::text), true),
                                '{{version_base}}', to_jsonb($2::text), true
                            ),
                            '{{build_time}}', to_jsonb($3::text), true
                        ),
                        '{{node_version}}', to_jsonb($4::text), true
                    ),
                updated_at={}
                "#,
                self.now_expr()
            )
        } else {
            format!(
                r#"
                UPDATE accounts
                SET canonical_env = json_set(
                    CASE
                        WHEN json_valid(canonical_env) THEN canonical_env
                        ELSE '{{}}'
                    END,
                    '$.version', $1,
                    '$.version_base', $2,
                    '$.build_time', $3,
                    '$.node_version', $4
                ),
                updated_at={}
                "#,
                self.now_expr()
            )
        };
        sqlx::query(&q)
            .bind(identity.version)
            .bind(identity.version_base)
            .bind(identity.build_time)
            .bind(identity.stainless_runtime_version)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_oauth_tokens(
        &self,
        id: i64,
        access_token: &str,
        refresh_token: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let q = format!(
            r#"UPDATE accounts SET access_token=$1, refresh_token=$2, oauth_expires_at={},
                oauth_refreshed_at={}, auth_error='', updated_at={}
            WHERE id=$5"#,
            self.ts(3),
            self.ts(4),
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(access_token)
            .bind(refresh_token)
            .bind(self.fmt_time(expires_at))
            .bind(self.fmt_time(Utc::now()))
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_auth_error(&self, id: i64, auth_error: &str) -> Result<(), AppError> {
        let q = format!(
            "UPDATE accounts SET auth_error=$1, updated_at={} WHERE id=$2",
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(auth_error)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_status(&self, id: i64, status: AccountStatus) -> Result<(), AppError> {
        let q = format!(
            "UPDATE accounts SET status=$1, updated_at={} WHERE id=$2",
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(status.to_string())
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_rate_limit(&self, id: i64, reset_at: DateTime<Utc>) -> Result<(), AppError> {
        let q = format!(
            "UPDATE accounts SET rate_limited_at={}, rate_limit_reset_at={}, updated_at={} WHERE id=$3",
            self.ts(1),
            self.ts(2),
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(self.fmt_time(Utc::now()))
            .bind(self.fmt_time(reset_at))
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn disable_account(
        &self,
        id: i64,
        status: AccountStatus,
        reason: &str,
        rate_limit_reset_at: Option<DateTime<Utc>>,
    ) -> Result<(), AppError> {
        let q = format!(
            r#"UPDATE accounts SET status=$1, disable_reason=$2,
                rate_limited_at={}, rate_limit_reset_at={}, updated_at={}
            WHERE id=$5"#,
            self.ts(3),
            self.ts(4),
            self.now_expr()
        );
        let limited_str = rate_limit_reset_at.map(|_| self.fmt_time(Utc::now()));
        let reset_str = rate_limit_reset_at.map(|t| self.fmt_time(t));
        sqlx::query(&q)
            .bind(status.to_string())
            .bind(reason)
            .bind(limited_str)
            .bind(reset_str)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn enable_account(&self, id: i64) -> Result<(), AppError> {
        let q = format!(
            r#"UPDATE accounts SET status='active', disable_reason='',
                rate_limited_at=NULL, rate_limit_reset_at=NULL, updated_at={}
            WHERE id=$1"#,
            self.now_expr()
        );
        sqlx::query(&q).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn clear_rate_limit(&self, id: i64) -> Result<(), AppError> {
        let q = format!(
            "UPDATE accounts SET rate_limited_at=NULL, rate_limit_reset_at=NULL, updated_at={} WHERE id=$1",
            self.now_expr()
        );
        sqlx::query(&q).bind(id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn delete(&self, id: i64) -> Result<(), AppError> {
        sqlx::query("DELETE FROM accounts WHERE id=$1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_by_id(&self, id: i64) -> Result<Account, AppError> {
        let row: AnyRow = sqlx::query(&format!(
            "SELECT {} FROM accounts WHERE id=$1",
            ACCOUNT_COLS
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(Self::row_to_account(&row))
    }

    pub async fn list(&self) -> Result<Vec<Account>, AppError> {
        let rows: Vec<AnyRow> = sqlx::query(&format!(
            "SELECT {} FROM accounts ORDER BY priority ASC, id ASC",
            ACCOUNT_COLS
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(Self::row_to_account).collect())
    }

    pub async fn count(&self) -> Result<i64, AppError> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM accounts")
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
        Ok(count)
    }

    pub async fn list_paged(&self, page: i64, page_size: i64) -> Result<Vec<Account>, AppError> {
        let offset = (page - 1) * page_size;
        let q = format!(
            "SELECT {} FROM accounts ORDER BY priority ASC, id ASC LIMIT $1 OFFSET $2",
            ACCOUNT_COLS
        );
        let rows: Vec<AnyRow> = sqlx::query(&q)
            .bind(page_size)
            .bind(offset)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(Self::row_to_account).collect())
    }

    pub async fn update_usage(&self, id: i64, usage_data: &str) -> Result<(), AppError> {
        let q = format!(
            "UPDATE accounts SET usage_data=$1, usage_fetched_at={}, updated_at={} WHERE id=$3",
            self.ts(2),
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(usage_data)
            .bind(self.fmt_time(Utc::now()))
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn increment_telemetry_count(&self, id: i64, delta: i64) -> Result<(), AppError> {
        let q = format!(
            "UPDATE accounts SET telemetry_count = telemetry_count + $1, updated_at={} WHERE id=$2",
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(delta)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_schedulable(&self) -> Result<Vec<Account>, AppError> {
        let q = format!(
            r#"SELECT {} FROM accounts
            WHERE status='active'
              AND (rate_limit_reset_at IS NULL OR rate_limit_reset_at < {})
            ORDER BY priority ASC, id ASC"#,
            ACCOUNT_COLS,
            self.now_expr()
        );
        let rows: Vec<AnyRow> = sqlx::query(&q).fetch_all(&self.pool).await?;
        Ok(rows.iter().map(Self::row_to_account).collect())
    }
}

const ACCOUNT_COLS: &str = r#"id, name, email, status, token, auth_type, access_token, refresh_token,
    oauth_expires_at, oauth_refreshed_at, auth_error, proxy_url, device_id,
    canonical_env, canonical_prompt_env, canonical_process,
    billing_mode, account_uuid, organization_uuid, subscription_type,
    concurrency, priority, rpm_limit, rate_limited_at, rate_limit_reset_at,
    disable_reason, auto_telemetry, auto_poll_usage, allow_1m_models, telemetry_count,
    usage_data, usage_fetched_at, created_at, updated_at"#;

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store(driver: &str) -> AccountStore {
        sqlx::any::install_default_drivers();
        let tmp = std::env::temp_dir().join(format!("ccgw_unit_{}.db", rand::random::<u64>()));
        let dsn = format!("sqlite:{}?mode=rwc", tmp.display());
        let pool = AnyPool::connect(&dsn).await.expect("pool");
        AccountStore {
            pool,
            driver: driver.to_string(),
        }
    }

    #[tokio::test]
    async fn test_ts_sqlite_plain_placeholder() {
        let store = make_store("sqlite").await;
        assert_eq!(store.ts(1), "$1");
        assert_eq!(store.ts(5), "$5");
        assert_eq!(store.ts(10), "$10");
    }

    #[tokio::test]
    async fn test_ts_postgres_casts_to_timestamptz() {
        let store = make_store("postgres").await;
        assert_eq!(store.ts(1), "$1::TIMESTAMPTZ");
        assert_eq!(store.ts(5), "$5::TIMESTAMPTZ");
        assert_eq!(store.ts(10), "$10::TIMESTAMPTZ");
    }

    #[tokio::test]
    async fn test_is_pg() {
        assert!(make_store("postgres").await.is_pg());
        assert!(!make_store("sqlite").await.is_pg());
    }

    #[tokio::test]
    async fn test_now_expr_sqlite() {
        let store = make_store("sqlite").await;
        assert_eq!(store.now_expr(), "strftime('%Y-%m-%dT%H:%M:%SZ','now')");
    }

    #[tokio::test]
    async fn test_now_expr_postgres() {
        let store = make_store("postgres").await;
        assert_eq!(store.now_expr(), "NOW()");
    }

    #[tokio::test]
    async fn test_fmt_time_iso8601() {
        let store = make_store("sqlite").await;
        let t = chrono::NaiveDate::from_ymd_opt(2026, 4, 9)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap()
            .and_utc();
        assert_eq!(store.fmt_time(t), "2026-04-09T12:30:45Z");
    }

    #[tokio::test]
    async fn update_all_canonical_env_identity_preserves_other_env_fields() {
        let store = make_store("sqlite").await;
        crate::store::db::migrate(&store.pool, "sqlite")
            .await
            .expect("migrate");
        sqlx::query(
            r#"INSERT INTO accounts (
                email, token, device_id, canonical_env, canonical_prompt_env, canonical_process
            ) VALUES ($1, $2, $3, $4, $5, $6)"#,
        )
        .bind("user@example.com")
        .bind("token")
        .bind("device-1")
        .bind(r#"{"version":"2.1.185","version_base":"2.1.185","build_time":"old","platform":"linux"}"#)
        .bind("{}")
        .bind("{}")
        .execute(&store.pool)
        .await
        .expect("insert account");

        let profile = crate::service::version_profile::profile_for_key("2.1.173").unwrap();
        store
            .update_all_canonical_env_identity(&profile.identity)
            .await
            .expect("update identity");

        let raw: String = sqlx::query_scalar("SELECT canonical_env FROM accounts WHERE email=$1")
            .bind("user@example.com")
            .fetch_one(&store.pool)
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
        assert_eq!(env["platform"], "linux");
    }
}
