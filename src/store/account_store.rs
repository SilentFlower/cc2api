use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row;
use sqlx::any::{AnyArguments, AnyRow};
use sqlx::{AnyPool, Arguments};

use crate::error::AppError;
use crate::model::account::{
    Account, AccountStatus, DEFAULT_ALLOW_1M_MODELS, DEFAULT_UPSTREAM_SESSION_POOL_SIZE,
    DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY, DEFAULT_UPSTREAM_SESSION_TTL_MINUTES,
    OAuthCredentialImport,
};
use crate::model::credential_ownership::CredentialOwnershipChange;
use crate::service::version_profile::IdentityProfile;

#[path = "credential_ownership.rs"]
mod credential_ownership;

/// 统一保存和读取 SQLite/PostgreSQL 账号配置与凭据。
pub struct AccountStore {
    pool: AnyPool,
    driver: String,
}

impl AccountStore {
    /// 创建与目标数据库驱动匹配的账号存储。
    /// @param pool 数据库连接池。
    /// @param driver sqlite 或 postgres。
    /// @return 账号存储实例。
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

    /// 为 PostgreSQL 显式转换文本时间参数，SQLite 保留原占位符。
    fn ts(&self, n: u32) -> String {
        if self.is_pg() {
            // SQLx Any 将无类型 NULL 编成 i32，先转 TEXT 让 NULL 与实际时间字符串共用转换。
            format!("${}::TEXT::TIMESTAMPTZ", n)
        } else {
            format!("${}", n)
        }
    }

    /// 按可空参数的实际编码类型划分PG预编译缓存，原始绑定值保持不变。
    /// @param query 已参数化的SQL。
    /// @param present 各可空参数是否具有实际值，顺序与调用点固定。
    /// @return PG附加有限布尔缓存键，SQLite返回原SQL。
    fn nullable_query(&self, mut query: String, present: &[bool]) -> String {
        if self.is_pg() {
            // SQLx Any把NULL编码为i32，PG缓存仅以SQL为键；同一缓存项必须保持参数类型。
            query.push_str(" /* any-null:");
            for value in present {
                query.push(if *value { '1' } else { '0' });
            }
            query.push_str(" */");
        }
        query
    }

    /// 固定账号创建和全量更新中的六个可空参数。@param query 参数化SQL。@param account 待写账号。@return 类型稳定的缓存SQL。
    fn account_query(&self, query: String, account: &Account) -> String {
        self.nullable_query(
            query,
            &[
                account.expires_at.is_some(),
                account.oauth_refreshed_at.is_some(),
                account.account_uuid.is_some(),
                account.organization_uuid.is_some(),
                account.subscription_type.is_some(),
                account.rate_limit_tier.is_some(),
            ],
        )
    }

    /// SQLx Any 不直接支持 JSONB，写入 PostgreSQL 原生 JSON 列时需要显式转换。
    fn json(&self, n: u32) -> String {
        if self.is_pg() {
            format!("${n}::JSONB")
        } else {
            format!("${n}")
        }
    }

    /// 兼容 SQLite 的 RFC3339 和 PostgreSQL 文本化后的带时区时间。
    fn parse_stored_time(value: &str) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(value)
            .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z"))
            .ok()
            .map(|time| time.with_timezone(&Utc))
    }

    fn parse_time(row: &AnyRow, col: &str) -> DateTime<Utc> {
        row.try_get::<String, _>(col)
            .ok()
            .and_then(|value| Self::parse_stored_time(&value))
            .unwrap_or_default()
    }

    fn parse_optional_time(row: &AnyRow, col: &str) -> Option<DateTime<Utc>> {
        row.try_get::<Option<String>, _>(col)
            .ok()
            .flatten()
            .and_then(|value| Self::parse_stored_time(&value))
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
            request_backend: row
                .try_get::<String, _>("request_backend")
                .unwrap_or_else(|_| "legacy".into()),
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
            rate_limit_tier: row
                .try_get::<Option<String>, _>("rate_limit_tier")
                .unwrap_or(None),
            concurrency: row.try_get::<i32, _>("concurrency").unwrap_or(3),
            priority: row.try_get::<i32, _>("priority").unwrap_or(50),
            rpm_limit: row.try_get::<i32, _>("rpm_limit").unwrap_or(0),
            five_hour_usage_limit_percent: row
                .try_get::<i32, _>("five_hour_usage_limit_percent")
                .unwrap_or(0),
            seven_day_usage_limit_percent: row
                .try_get::<i32, _>("seven_day_usage_limit_percent")
                .unwrap_or(0),
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
            allow_fast_mode: row.try_get::<i32, _>("allow_fast_mode").unwrap_or(0) != 0,
            upstream_session_pool_enabled: row
                .try_get::<i32, _>("upstream_session_pool_enabled")
                .unwrap_or(0)
                != 0,
            upstream_session_pool_size: row
                .try_get::<i32, _>("upstream_session_pool_size")
                .unwrap_or(DEFAULT_UPSTREAM_SESSION_POOL_SIZE),
            upstream_session_ttl_minutes: row
                .try_get::<i32, _>("upstream_session_ttl_minutes")
                .unwrap_or(DEFAULT_UPSTREAM_SESSION_TTL_MINUTES),
            upstream_session_refresh_policy: row
                .try_get::<String, _>("upstream_session_refresh_policy")
                .unwrap_or_else(|_| DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY.into()),
            telemetry_count: row.try_get::<i64, _>("telemetry_count").unwrap_or(0),
            usage_data: Self::parse_json(row, "usage_data"),
            usage_fetched_at: Self::parse_optional_time(row, "usage_fetched_at"),
            created_at: Self::parse_time(row, "created_at"),
            updated_at: Self::parse_time(row, "updated_at"),
        }
    }

    /// 创建账号并等待写入完成，使后续独立连接可以立即解析凭据。
    /// @param a 待创建账号；成功后回填 ID 和数据库时间。
    /// @return 持久化结果；失败不报告创建成功。
    pub async fn create(&self, a: &mut Account) -> Result<(), AppError> {
        self.create_with_ownership(a, None).await
    }

    /// 同事务创建账号及初始交接状态，避免半创建账号提前刷新。
    /// @param a 已校验账号。@param ownership 初始归属。@return 创建并提交的结果。
    pub(crate) async fn create_with_ownership(
        &self,
        a: &mut Account,
        ownership: Option<CredentialOwnershipChange>,
    ) -> Result<(), AppError> {
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
        let allow_fast_mode_int: i32 = if a.allow_fast_mode { 1 } else { 0 };
        let upstream_session_pool_enabled_int: i32 = if a.upstream_session_pool_enabled {
            1
        } else {
            0
        };
        let q = format!(
            r#"INSERT INTO accounts (name, email, status, token, proxy_url,
                auth_type, access_token, refresh_token, oauth_expires_at, oauth_refreshed_at, auth_error,
                device_id, canonical_env, canonical_prompt_env, canonical_process,
                billing_mode, account_uuid, organization_uuid, subscription_type,
                concurrency, priority, rpm_limit, auto_telemetry, auto_poll_usage, allow_1m_models,
                allow_fast_mode, upstream_session_pool_enabled, upstream_session_pool_size, upstream_session_ttl_minutes,
                upstream_session_refresh_policy, five_hour_usage_limit_percent, seven_day_usage_limit_percent, rate_limit_tier, request_backend)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,{},{},{},$12,{},{},{},$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34)
            RETURNING id, CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at"#,
            self.ts(9),
            self.ts(10),
            "$11",
            self.json(13),
            self.json(14),
            self.json(15)
        );
        let q = self.account_query(q, a);
        // SQLite WAL 下只取 RETURNING 首行会在语句完成前返回，必须消费到结束才确认提交。
        let mut transaction = self.pool.begin().await?;
        let rows: Vec<AnyRow> = sqlx::query(&q)
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
            .bind(allow_fast_mode_int)
            .bind(upstream_session_pool_enabled_int)
            .bind(a.upstream_session_pool_size)
            .bind(a.upstream_session_ttl_minutes)
            .bind(&a.upstream_session_refresh_policy)
            .bind(a.five_hour_usage_limit_percent)
            .bind(a.seven_day_usage_limit_percent)
            .bind(&a.rate_limit_tier)
            .bind(&a.request_backend)
            .fetch_all(&mut *transaction)
            .await?;
        let row = rows.first().ok_or(AppError::NotFound)?;

        a.id = row.try_get::<i64, _>("id").unwrap_or_default();
        a.created_at = Self::parse_time(row, "created_at");
        a.updated_at = Self::parse_time(row, "updated_at");
        if let Some(mut change) = ownership {
            change.next.account_id = a.id;
            change.receipt.account_id = a.id;
            credential_ownership::apply_ownership_change(&mut transaction, &change).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// 保存账号的可编辑配置并保留独立更新的用量与进程指标。
    /// @param a 已校验的完整账号配置。
    /// @return 数据库写入结果。
    pub async fn update(&self, a: &Account) -> Result<(), AppError> {
        let expires_at = a.expires_at.map(|t| self.fmt_time(t));
        let oauth_refreshed_at = a.oauth_refreshed_at.map(|t| self.fmt_time(t));
        let auto_telemetry_int: i32 = if a.auto_telemetry { 1 } else { 0 };
        let auto_poll_usage_int: i32 = if a.auto_poll_usage { 1 } else { 0 };
        let allow_fast_mode_int: i32 = if a.allow_fast_mode { 1 } else { 0 };
        let upstream_session_pool_enabled_int: i32 = if a.upstream_session_pool_enabled {
            1
        } else {
            0
        };
        let q = format!(
            r#"UPDATE accounts SET name=$1, email=$2, status=$3, token=$4,
                auth_type=$5, access_token=$6, refresh_token=$7, oauth_expires_at={}, oauth_refreshed_at={},
                auth_error=$10, proxy_url=$11, billing_mode=$12,
                account_uuid=$13, organization_uuid=$14, subscription_type=$15,
                concurrency=$16, priority=$17, rpm_limit=$18, auto_telemetry=$19, auto_poll_usage=$20,
                allow_1m_models=$21, allow_fast_mode=$22, upstream_session_pool_enabled=$23,
                upstream_session_pool_size=$24, upstream_session_ttl_minutes=$25,
                upstream_session_refresh_policy=$26, five_hour_usage_limit_percent=$27,
                seven_day_usage_limit_percent=$28, rate_limit_tier=$29, request_backend=$30, updated_at={}
            WHERE id=$31"#,
            self.ts(8),
            self.ts(9),
            self.now_expr()
        );
        let q = self.account_query(q, a);
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
            .bind(allow_fast_mode_int)
            .bind(upstream_session_pool_enabled_int)
            .bind(a.upstream_session_pool_size)
            .bind(a.upstream_session_ttl_minutes)
            .bind(&a.upstream_session_refresh_policy)
            .bind(a.five_hour_usage_limit_percent)
            .bind(a.seven_day_usage_limit_percent)
            .bind(&a.rate_limit_tier)
            .bind(&a.request_backend)
            .bind(a.id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 仅写入白名单内明确提交的普通配置，避免旧快照覆盖并行变化的凭据和状态。
    /// @param account 已校验的配置快照。
    /// @param fields 本次需要更新的字段名，未知或受保护字段会被拒绝。
    /// @return 数据库写入结果；空字段列表不执行写入。
    pub async fn update_options(&self, account: &Account, fields: &[&str]) -> Result<(), AppError> {
        let mut args = AnyArguments::default();
        let mut assignments = Vec::new();
        for field in fields {
            let text = match *field {
                "name" => Some(Some(account.name.clone())),
                "billing_mode" => Some(Some(account.billing_mode.to_string())),
                "request_backend" => Some(Some(account.request_backend.clone())),
                "subscription_type" => Some(account.subscription_type.clone()),
                "rate_limit_tier" => Some(account.rate_limit_tier.clone()),
                "allow_1m_models" => Some(Some(account.allow_1m_models.clone())),
                "upstream_session_refresh_policy" => {
                    Some(Some(account.upstream_session_refresh_policy.clone()))
                }
                _ => None,
            };
            let index = assignments.len() + 1;
            if let Some(value) = text {
                // 固定绑定文本类型，避免 Any 的 NULL 在预编译缓存中先被记成整数类型。
                args.add(value.unwrap_or_default());
                let expression = if matches!(*field, "subscription_type" | "rate_limit_tier") {
                    format!("NULLIF(${index},'')")
                } else {
                    format!("${index}")
                };
                assignments.push(format!("{field}={expression}"));
                continue;
            }
            let value = match *field {
                "concurrency" => account.concurrency,
                "priority" => account.priority,
                "rpm_limit" => account.rpm_limit,
                "auto_telemetry" => i32::from(account.auto_telemetry),
                "auto_poll_usage" => i32::from(account.auto_poll_usage),
                "allow_fast_mode" => i32::from(account.allow_fast_mode),
                "upstream_session_pool_enabled" => i32::from(account.upstream_session_pool_enabled),
                "upstream_session_pool_size" => account.upstream_session_pool_size,
                "upstream_session_ttl_minutes" => account.upstream_session_ttl_minutes,
                "five_hour_usage_limit_percent" => account.five_hour_usage_limit_percent,
                "seven_day_usage_limit_percent" => account.seven_day_usage_limit_percent,
                _ => return Err(AppError::BadRequest("字段不支持运行中更新".into())),
            };
            args.add(value);
            assignments.push(format!("{field}=${index}"));
        }
        if assignments.is_empty() {
            return Ok(());
        }
        args.add(account.id);
        let query = format!(
            "UPDATE accounts SET {}, updated_at={} WHERE id=${}",
            assignments.join(", "),
            self.now_expr(),
            assignments.len() + 1
        );
        sqlx::query_with(&query, args).execute(&self.pool).await?;
        Ok(())
    }

    /// 只更新账号的两个用量控制线，避免状态快捷操作覆盖其它配置或凭据。
    /// @param account 已通过值域校验的账号控制线快照。
    /// @return 数据库写入结果。
    pub async fn update_usage_limits(&self, account: &Account) -> Result<(), AppError> {
        let query = format!(
            "UPDATE accounts SET five_hour_usage_limit_percent=$1, seven_day_usage_limit_percent=$2, updated_at={} WHERE id=$3",
            self.now_expr()
        );
        sqlx::query(&query)
            .bind(account.five_hour_usage_limit_percent)
            .bind(account.seven_day_usage_limit_percent)
            .bind(account.id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 只更新原身份的进程指标，避免用旧账号副本覆盖并发刷新的 OAuth 或策略。
    /// @param account 校验时读取的账号身份。
    /// @param process 已校验的进程 JSON。
    /// @return 条件写入结果；身份已改变时拒绝。
    pub async fn update_cli_process(
        &self,
        account: &Account,
        process: &Value,
    ) -> Result<(), AppError> {
        let query = format!(
            "UPDATE accounts SET canonical_process={}, updated_at={} \
            WHERE id=$2 AND device_id=$3 AND COALESCE(account_uuid,'')=$4 AND email=$5 AND auth_type='oauth'",
            self.json(1),
            self.now_expr()
        );
        let changed = sqlx::query(&query)
            .bind(
                serde_json::to_string(process)
                    .map_err(|_| AppError::BadRequest("进程指标格式无效".into()))?,
            )
            .bind(account.id)
            .bind(&account.device_id)
            .bind(account.account_uuid.as_deref().unwrap_or_default())
            .bind(&account.email)
            .execute(&self.pool)
            .await?;
        if changed.rows_affected() != 1 {
            return Err(AppError::BadRequest(
                "账号身份已变化，未更新进程指标".into(),
            ));
        }
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

    /// 只提交仍属于调用时凭据的刷新结果，拒绝覆盖重授权产生的新令牌。
    /// @param expected 刷新开始时的账号快照。
    /// @param access_token 新 AT。
    /// @param refresh_token 新 RT。
    /// @param expires_at 新有效期。
    /// @return 是否更新了调用时的同一份凭据。
    pub async fn update_oauth_tokens_if_current(
        &self,
        expected: &Account,
        access_token: &str,
        refresh_token: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, AppError> {
        let query = format!(
            "UPDATE accounts SET access_token=$1,refresh_token=$2,
            oauth_expires_at={},oauth_refreshed_at={},auth_error='',updated_at={}
            WHERE id=$5 AND auth_type='oauth' AND access_token=$6 AND refresh_token=$7",
            self.ts(3),
            self.ts(4),
            self.now_expr()
        );
        Ok(sqlx::query(&query)
            .bind(access_token)
            .bind(refresh_token)
            .bind(self.fmt_time(expires_at))
            .bind(self.fmt_time(Utc::now()))
            .bind(expected.id)
            .bind(&expected.access_token)
            .bind(&expected.refresh_token)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }

    /// 旧刷新失败不能污染已经重新授权的账号。
    /// @param expected 刷新开始时的账号快照。
    /// @param error 脱敏认证错误。
    /// @return 错误是否仍属于当前凭据。
    pub async fn update_auth_error_if_current(
        &self,
        expected: &Account,
        error: &str,
    ) -> Result<bool, AppError> {
        let query = format!(
            "UPDATE accounts SET auth_error=$1,updated_at={}
            WHERE id=$2 AND auth_type='oauth' AND access_token=$3 AND refresh_token=$4",
            self.now_expr()
        );
        Ok(sqlx::query(&query)
            .bind(error)
            .bind(expected.id)
            .bind(&expected.access_token)
            .bind(&expected.refresh_token)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
    }

    /// 读取不含凭据的导入回执，供超时重试与恢复确认。
    /// @param operation_id 唯一提交操作 ID。
    /// @return 原账号 ID 和请求摘要；不存在时返回 None。
    pub async fn oauth_import_receipt(
        &self,
        operation_id: &str,
    ) -> Result<Option<(i64, String)>, AppError> {
        let record = sqlx::query(
            "SELECT account_id,request_hash FROM oauth_credential_imports WHERE operation_id=$1",
        )
        .bind(operation_id)
        .fetch_optional(&self.pool)
        .await?;
        record
            .map(|row| Ok((row.try_get("account_id")?, row.try_get("request_hash")?)))
            .transpose()
    }

    /// 同事务保存重授权凭据和回执，保留其他账号策略与限流。
    /// @param expected 已完成身份核对的当前账号快照。
    /// @param input CLI 产生的新凭据。
    /// @param request_hash 请求摘要，用于识别重试与操作 ID 冲突。
    /// @param restore_auth 是否只恢复已确认的认证停用。
    /// @return 成功返回空值；冲突或失败回滚凭据与回执。
    pub async fn import_oauth_credentials(
        &self,
        expected: &Account,
        input: &OAuthCredentialImport,
        request_hash: &str,
        restore_auth: bool,
    ) -> Result<(), AppError> {
        self.import_oauth_credentials_owned(expected, input, request_hash, restore_auth, None)
            .await
    }

    /// 将登录/回交凭据与归属阶段原子提交，复用原导入回执和身份条件。
    /// @param expected 原账号。@param input 凭据。@param request_hash 摘要。
    /// @param restore_auth 是否恢复认证停用。@param ownership 同批归属变更。@return 提交结果。
    pub(crate) async fn import_oauth_credentials_owned(
        &self,
        expected: &Account,
        input: &OAuthCredentialImport,
        request_hash: &str,
        restore_auth: bool,
        ownership: Option<&CredentialOwnershipChange>,
    ) -> Result<(), AppError> {
        let expires_at = DateTime::from_timestamp_millis(input.expires_at)
            .ok_or_else(|| AppError::BadRequest("凭据有效期不合法".into()))?;
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO oauth_credential_imports(operation_id,account_id,request_hash,created_at)
            VALUES($1,$2,$3,$4) ON CONFLICT(operation_id) DO NOTHING",
        )
        .bind(&input.operation_id)
        .bind(expected.id)
        .bind(request_hash)
        .bind(self.fmt_time(Utc::now()))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        if inserted == 0 {
            let receipt = sqlx::query("SELECT account_id,request_hash FROM oauth_credential_imports WHERE operation_id=$1")
                .bind(&input.operation_id).fetch_one(&mut *transaction).await?;
            if receipt.try_get::<i64, _>("account_id")? != expected.id
                || receipt.try_get::<String, _>("request_hash")? != request_hash
            {
                return Err(AppError::BadRequest("凭据提交操作 ID 冲突".into()));
            }
            // 重试只读当前权威凭据，绝不再次写入第一次提交的旧 RT。
            transaction.commit().await?;
            return Ok(());
        }
        let query = format!("UPDATE accounts SET access_token=$1,refresh_token=$2,oauth_expires_at={},
            auth_error='',status=CASE WHEN $4=1 THEN 'active' ELSE status END,
            disable_reason=CASE WHEN $4=1 THEN '' ELSE disable_reason END,oauth_refreshed_at={},updated_at={},
            rate_limit_tier=CASE WHEN $16=1 THEN CAST($17 AS TEXT)
                WHEN $14=1 AND COALESCE(subscription_type,'')<>COALESCE(CAST($15 AS TEXT),'') THEN NULL ELSE rate_limit_tier END,
            subscription_type=CASE WHEN $14=1 THEN CAST($15 AS TEXT) ELSE subscription_type END
            WHERE id=$6 AND auth_type='oauth' AND access_token=$7 AND refresh_token=$8
                AND status=$9 AND disable_reason=$10 AND email=$11
                AND COALESCE(account_uuid,'')=$12 AND COALESCE(organization_uuid,'')=$13",
            self.ts(3), self.ts(5), self.now_expr());
        let updated = sqlx::query(&query)
            .bind(&input.access_token)
            .bind(&input.refresh_token)
            .bind(self.fmt_time(expires_at))
            .bind(i32::from(restore_auth))
            .bind(self.fmt_time(Utc::now()))
            .bind(expected.id)
            .bind(&expected.access_token)
            .bind(&expected.refresh_token)
            .bind(expected.status.to_string())
            .bind(&expected.disable_reason)
            .bind(&expected.email)
            .bind(expected.account_uuid.as_deref().unwrap_or_default())
            .bind(expected.organization_uuid.as_deref().unwrap_or_default())
            .bind(i32::from(input.subscription_type.is_some()))
            .bind(
                input
                    .subscription_type
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty()),
            )
            .bind(i32::from(input.rate_limit_tier.is_some()))
            .bind(
                input
                    .rate_limit_tier
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty()),
            )
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        if updated != 1 {
            return Err(AppError::ServiceUnavailable(
                "账号状态或凭据已改变，请重试提交".into(),
            ));
        }
        if let Some(change) = ownership {
            credential_ownership::apply_ownership_change(&mut transaction, change).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// 局部更新原始套餐元数据，避免旧账号快照覆盖轮换后的凭据。
    /// @param id 目标账号。
    /// @param subscription_type 外层为空表示未提交，内层为空表示清除。
    /// @param rate_limit_tier 外层为空表示未提交，内层为空表示清除。
    /// @return 保存结果；套餐改变而未提供真实分级时清除旧分级。
    pub async fn update_subscription_metadata(
        &self,
        id: i64,
        subscription_type: Option<Option<String>>,
        rate_limit_tier: Option<Option<String>>,
    ) -> Result<(), AppError> {
        // 始终绑定文本，清除用 NULLIF 表达，避免 NULL/文本交替时缓存的参数类型冲突。
        let query = format!("UPDATE accounts SET
            rate_limit_tier=CASE WHEN $1=1 THEN NULLIF($2,'')
                WHEN $3=1 AND COALESCE(subscription_type,'')<>$4 THEN NULL ELSE rate_limit_tier END,
            subscription_type=CASE WHEN $3=1 THEN NULLIF($4,'') ELSE subscription_type END,updated_at={}
            WHERE id=$5", self.now_expr());
        let changed = sqlx::query(&query)
            .bind(i32::from(rate_limit_tier.is_some()))
            .bind(rate_limit_tier.flatten().unwrap_or_default())
            .bind(i32::from(subscription_type.is_some()))
            .bind(subscription_type.flatten().unwrap_or_default())
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected();
        if changed != 1 {
            return Err(AppError::NotFound);
        }
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

    /// 保存账号停用状态及可选限流时间。
    /// @param id 目标账号ID。
    /// @param status 调用方决定的账号状态。
    /// @param reason 已脱敏的停用原因。
    /// @param rate_limit_reset_at 可选恢复时间，省略时清除原限流时间。
    /// @return 数据库写入结果。
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
        let q = self.nullable_query(q, &[rate_limit_reset_at.is_some()]);
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

    /// 因上游认证失败停用账号，并原子记录认证错误。
    ///
    /// @param id 账号 ID。
    /// @param reason 不包含凭据内容的认证失败原因。
    /// @return 更新成功时返回空值。
    pub async fn disable_for_auth_failure(&self, id: i64, reason: &str) -> Result<(), AppError> {
        let q = format!(
            r#"UPDATE accounts SET status='disabled', auth_error=$1, disable_reason=$1,
                rate_limited_at=NULL, rate_limit_reset_at=NULL, updated_at={}
            WHERE id=$2"#,
            self.now_expr()
        );
        sqlx::query(&q)
            .bind(reason)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 永久凭据失败的迟到回调不能停用新登录凭据或覆盖人工停用。
    /// @param expected 失败解析时的账号快照。
    /// @param reason 脱敏永久凭据错误。
    /// @return 是否仍属于同一份 active OAuth 凭据。
    pub async fn disable_for_auth_failure_if_current(
        &self,
        expected: &Account,
        reason: &str,
    ) -> Result<bool, AppError> {
        let query = format!("UPDATE accounts SET status='disabled',auth_error=$1,disable_reason=$1,
            rate_limited_at=NULL,rate_limit_reset_at=NULL,updated_at={}
            WHERE id=$2 AND status='active' AND auth_type='oauth' AND access_token=$3 AND refresh_token=$4", self.now_expr());
        Ok(sqlx::query(&query)
            .bind(reason)
            .bind(expected.id)
            .bind(&expected.access_token)
            .bind(&expected.refresh_token)
            .execute(&self.pool)
            .await?
            .rows_affected()
            == 1)
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
        let mut transaction = self.pool.begin().await?;
        sqlx::query("DELETE FROM oauth_handoff_operations WHERE account_id=$1")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM oauth_account_ownerships WHERE account_id=$1")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM accounts WHERE id=$1")
            .bind(id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
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

// 将原生 JSON/时间列统一投影为文本，避免 SQLx Any 无法解码 PostgreSQL 类型。
const ACCOUNT_COLS: &str = r#"id, name, email, status, token, auth_type, access_token, refresh_token,
    CAST(oauth_expires_at AS TEXT) AS oauth_expires_at,
    CAST(oauth_refreshed_at AS TEXT) AS oauth_refreshed_at, auth_error, proxy_url, device_id,
    CAST(canonical_env AS TEXT) AS canonical_env,
    CAST(canonical_prompt_env AS TEXT) AS canonical_prompt_env,
    CAST(canonical_process AS TEXT) AS canonical_process,
    billing_mode, request_backend, account_uuid, organization_uuid, subscription_type, rate_limit_tier,
    concurrency, priority, rpm_limit, five_hour_usage_limit_percent, seven_day_usage_limit_percent,
    CAST(rate_limited_at AS TEXT) AS rate_limited_at,
    CAST(rate_limit_reset_at AS TEXT) AS rate_limit_reset_at,
    disable_reason, auto_telemetry, auto_poll_usage, allow_1m_models, allow_fast_mode,
    upstream_session_pool_enabled, upstream_session_pool_size, upstream_session_ttl_minutes,
    upstream_session_refresh_policy, telemetry_count,
    CAST(usage_data AS TEXT) AS usage_data,
    CAST(usage_fetched_at AS TEXT) AS usage_fetched_at,
    CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::database_schema::migrate_legacy;
    use sqlx::any::AnyPoolOptions;

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
    async fn historical_backend_migration_and_account_updates_are_independent() {
        let store = make_store("sqlite").await;
        // 历史夹具模拟引入版本台账前的数据库，后续再由正式入口登记基线。
        migrate_legacy(&store.pool, "sqlite").await.unwrap();
        sqlx::query("ALTER TABLE accounts DROP COLUMN request_backend")
            .execute(&store.pool)
            .await
            .unwrap();
        for email in ["a@example.test", "b@example.test"] {
            sqlx::query("INSERT INTO accounts(email,token,device_id,canonical_env,canonical_prompt_env,canonical_process) VALUES($1,'synthetic','device','{}','{}','{}')").bind(email).execute(&store.pool).await.unwrap();
        }
        crate::store::db::migrate(&store.pool, "sqlite")
            .await
            .unwrap();
        let mut accounts = store.list().await.unwrap();
        assert_eq!(accounts.len(), 2);
        assert!(
            accounts
                .iter()
                .all(|account| account.request_backend == "legacy")
        );
        accounts[0].request_backend = "native293".into();
        store
            .update_options(&accounts[0], &["request_backend"])
            .await
            .unwrap();
        assert_eq!(
            store
                .get_by_id(accounts[0].id)
                .await
                .unwrap()
                .request_backend,
            "native293"
        );
        assert_eq!(
            store
                .get_by_id(accounts[1].id)
                .await
                .unwrap()
                .request_backend,
            "legacy"
        );
        accounts[0].request_backend = "legacy".into();
        store.update(&accounts[0]).await.unwrap();
        assert_eq!(
            store
                .get_by_id(accounts[0].id)
                .await
                .unwrap()
                .request_backend,
            "legacy"
        );
    }

    #[tokio::test]
    #[ignore = "需要独立、全新的合成 PostgreSQL 测试库 CCRB_TEST_POSTGRES_URL"]
    async fn postgres_native_backend_migration_create_update_and_rollback() {
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&std::env::var("CCRB_TEST_POSTGRES_URL").unwrap())
            .await
            .unwrap();
        crate::store::db::migrate(&pool, "postgres").await.unwrap();
        sqlx::query("ALTER TABLE accounts DROP COLUMN request_backend")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts(email,token,device_id,canonical_env,canonical_prompt_env,canonical_process) VALUES('historical@example.test','synthetic','device','{}','{}','{}')").execute(&pool).await.unwrap();
        crate::store::db::migrate(&pool, "postgres").await.unwrap();
        let store = AccountStore::new(pool, "postgres".into());
        let old = store.list().await.unwrap().remove(0);
        assert_eq!(old.request_backend, "legacy");
        let mut native = old.clone();
        native.id = 0;
        native.email = "native@example.test".into();
        native.request_backend = "native293".into();
        store.create(&mut native).await.unwrap();
        // 同一连接的同一SQL必须覆盖NULL和实际时间交替，防止参数类型缓存漂移。
        let timestamp = DateTime::parse_from_rfc3339("2030-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut dated = old.clone();
        dated.id = 0;
        dated.email = "dated@example.test".into();
        dated.expires_at = Some(timestamp);
        dated.oauth_refreshed_at = Some(timestamp);
        dated.account_uuid = Some("11111111-1111-4111-8111-111111111111".into());
        dated.organization_uuid = Some("22222222-2222-4222-8222-222222222222".into());
        dated.subscription_type = Some("pro".into());
        dated.rate_limit_tier = Some("synthetic-tier".into());
        store.create(&mut dated).await.unwrap();
        assert_eq!(
            store.get_by_id(dated.id).await.unwrap().expires_at,
            Some(timestamp)
        );
        assert_eq!(
            store.get_by_id(native.id).await.unwrap().request_backend,
            "native293"
        );
        native.request_backend = "legacy".into();
        store.update(&native).await.unwrap();
        assert_eq!(
            store.get_by_id(native.id).await.unwrap().request_backend,
            "legacy"
        );
        native.request_backend = "native293".into();
        native.expires_at = Some(timestamp);
        native.oauth_refreshed_at = Some(timestamp);
        native.account_uuid = dated.account_uuid.clone();
        native.organization_uuid = dated.organization_uuid.clone();
        native.subscription_type = dated.subscription_type.clone();
        native.rate_limit_tier = dated.rate_limit_tier.clone();
        store.update(&native).await.unwrap();
        assert_eq!(
            store.get_by_id(native.id).await.unwrap().expires_at,
            Some(timestamp)
        );
        native.expires_at = None;
        native.oauth_refreshed_at = None;
        native.account_uuid = None;
        native.organization_uuid = None;
        native.subscription_type = None;
        native.rate_limit_tier = None;
        store.update(&native).await.unwrap();
        assert!(
            store
                .get_by_id(native.id)
                .await
                .unwrap()
                .expires_at
                .is_none()
        );
        native.account_uuid = Some(String::new());
        store.update(&native).await.unwrap();
        assert_eq!(
            store.get_by_id(native.id).await.unwrap().account_uuid,
            Some(String::new())
        );
        for reset in [None, Some(timestamp), None] {
            store
                .disable_account(
                    native.id,
                    AccountStatus::Disabled,
                    "synthetic-disable",
                    reset,
                )
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_by_id(native.id)
                    .await
                    .unwrap()
                    .rate_limit_reset_at,
                reset
            );
        }
        store
            .update_options(&native, &["request_backend"])
            .await
            .unwrap();
        assert_eq!(
            store.get_by_id(native.id).await.unwrap().request_backend,
            "native293"
        );
        assert_eq!(
            store.get_by_id(old.id).await.unwrap().request_backend,
            "legacy"
        );
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
        assert_eq!(store.ts(1), "$1::TEXT::TIMESTAMPTZ");
        assert_eq!(store.ts(5), "$5::TEXT::TIMESTAMPTZ");
        assert_eq!(store.ts(10), "$10::TEXT::TIMESTAMPTZ");
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
