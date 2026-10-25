use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde_json::Value;
use sqlx::{AnyPool, Row};

use crate::error::AppError;
use crate::model::account::Account;
use crate::model::request_usage::{RequestUsage, WindowUsage};
use crate::service::telemetry::MessageTelemetryUsage;
use crate::service::usage_pricing::{cache_write_tokens, price_version};

/// 一次真实上游调用的数值摘要，不保存正文或凭据。
pub struct UsageRecord {
    pub record_id: String,
    pub account_id: i64,
    pub started_at: DateTime<Utc>,
    pub model: String,
    pub usage: MessageTelemetryUsage,
    pub cost_usd: Option<f64>,
    pub partial: bool,
}

/// 本地用量持久化与分页批量汇总。
pub struct UsageStore {
    pool: AnyPool,
    coverage_started_at: DateTime<Utc>,
    last_gap_ms: AtomicI64,
    last_prune_ms: AtomicI64,
}

impl UsageStore {
    /// 构造统计存储；保守地将本进程开始前视为可能存在采集间断。
    /// @param pool 已迁移的 SQLite／PostgreSQL 连接池。
    /// @return 共享统计存储。
    pub fn new(pool: AnyPool) -> Self {
        Self {
            pool,
            coverage_started_at: Utc::now(),
            last_gap_ms: AtomicI64::new(0),
            last_prune_ms: AtomicI64::new(0),
        }
    }

    /// 幂等创建双数据库通用的统计表和首次启用时间。
    /// @param pool 应用数据库连接池。
    /// @return 迁移成功或数据库错误。
    pub async fn migrate(pool: &AnyPool) -> Result<(), sqlx::Error> {
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
        sqlx::query("INSERT INTO usage_stats_meta(id, recorded_since) VALUES(1, $1) ON CONFLICT(id) DO NOTHING")
            .bind(timestamp(Utc::now()))
            .execute(pool).await?;
        Ok(())
    }

    /// 记录一次调用，唯一键保证重复 finalize 不会重复计数。
    /// @param record 已解析的安全摘要。
    /// @return 写入成功或数据库／数值范围错误。
    pub async fn record(&self, record: &UsageRecord) -> Result<(), AppError> {
        let result = self.insert(record).await;
        if result.is_err() {
            self.last_gap_ms
                .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
        }
        result
    }

    async fn insert(&self, record: &UsageRecord) -> Result<(), AppError> {
        let values = [
            record.usage.input_tokens,
            record.usage.output_tokens,
            record.usage.cache_read_input_tokens,
            cache_write_tokens(record.usage),
            record.usage.cache_creation_ephemeral_5m_input_tokens,
            record.usage.cache_creation_ephemeral_1h_input_tokens,
        ]
        .map(i64::try_from);
        let mut tokens = Vec::with_capacity(values.len());
        for value in values {
            tokens.push(value.map_err(|_| AppError::Internal("用量数值超出存储范围".into()))?);
        }
        if record
            .cost_usd
            .is_some_and(|cost| !cost.is_finite() || cost < 0.0)
        {
            return Err(AppError::Internal("无效的用量费用".into()));
        }
        sqlx::query("INSERT INTO usage_records
            (record_id,account_id,started_at,finished_at,model,input_tokens,output_tokens,cache_read_tokens,
             cache_write_tokens,cache_write_5m_tokens,cache_write_1h_tokens,cost_usd,partial,price_version)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT(record_id) DO NOTHING")
            .bind(&record.record_id).bind(record.account_id).bind(timestamp(record.started_at))
            .bind(timestamp(Utc::now())).bind(&record.model)
            .bind(tokens[0]).bind(tokens[1]).bind(tokens[2]).bind(tokens[3]).bind(tokens[4]).bind(tokens[5])
            .bind(record.cost_usd).bind(i64::from(record.partial)).bind(price_version())
            .execute(&self.pool).await.map_err(storage_error)?;

        // 按时间保留八天，不能按条数截断，否则高流量会破坏当前周统计。
        let now = Utc::now();
        let previous = self.last_prune_ms.load(Ordering::Relaxed);
        if now.timestamp_millis() - previous > 3_600_000
            && self
                .last_prune_ms
                .compare_exchange(
                    previous,
                    now.timestamp_millis(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            let _ = sqlx::query("DELETE FROM usage_records WHERE started_at < $1")
                .bind(timestamp(now - Duration::days(8)))
                .execute(&self.pool)
                .await;
        }
        Ok(())
    }

    /// 为当前账号页批量查询两个窗口；读取本身不清除记录。
    /// @param accounts 当前页账号及其官方额度观察。
    /// @param now 本次查询统一使用的 UTC 时间。
    /// @return 按账号 ID 索引的统计快照。
    pub async fn summaries(
        &self,
        accounts: &[Account],
        now: DateTime<Utc>,
    ) -> Result<HashMap<i64, RequestUsage>, AppError> {
        let mut result = HashMap::new();
        if accounts.is_empty() {
            return Ok(result);
        }
        let recorded_since: String =
            sqlx::query_scalar("SELECT recorded_since FROM usage_stats_meta WHERE id=1")
                .fetch_one(&self.pool)
                .await
                .map_err(storage_error)?;
        let recorded_since = DateTime::parse_from_rfc3339(&recorded_since)
            .map_err(|_| AppError::Internal("统计启用时间无效".into()))?
            .with_timezone(&Utc);
        let coverage_since = recorded_since.max(self.coverage_started_at);
        let gap_ms = self.last_gap_ms.load(Ordering::Relaxed);
        let mut bounds = Vec::with_capacity(accounts.len() * 2);
        for account in accounts {
            bounds.push((
                account.id,
                "five_hour",
                window_bounds(&account.usage_data, "five_hour", Duration::hours(5), now),
            ));
            bounds.push((
                account.id,
                "seven_day",
                window_bounds(&account.usage_data, "seven_day", Duration::days(7), now),
            ));
        }
        let placeholders: Vec<_> = (0..bounds.len())
            .map(|n| {
                let n = n * 4;
                format!(
                    "(CAST(${} AS BIGINT),${},${},${})",
                    n + 1,
                    n + 2,
                    n + 3,
                    n + 4
                )
            })
            .collect();
        // 在数据库内聚合，分页轮询不把整周原始记录拉回进程，也不逐账号查询。
        let sql = format!("WITH windows(account_id,kind,starts_at,ends_at) AS (VALUES {})
            SELECT w.account_id,w.kind,COUNT(r.record_id) AS requests,
                CAST(COALESCE(SUM(r.input_tokens+r.output_tokens+r.cache_read_tokens+r.cache_write_tokens),0) AS BIGINT) AS tokens,
                COALESCE(SUM(r.cost_usd),CAST(0 AS DOUBLE PRECISION)) AS cost,
                CAST(SUM(CASE WHEN r.record_id IS NOT NULL AND r.cost_usd IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS unpriced,
                CAST(COALESCE(SUM(r.partial),0) AS BIGINT) AS partial
            FROM windows w LEFT JOIN usage_records r ON r.account_id=w.account_id
                AND r.started_at>=w.starts_at AND r.started_at<w.ends_at
            GROUP BY w.account_id,w.kind", placeholders.join(","));
        let mut query = sqlx::query(&sql);
        for (id, kind, bound) in &bounds {
            query = query
                .bind(*id)
                .bind(*kind)
                .bind(timestamp(bound.starts_at))
                .bind(timestamp(bound.ends_at.min(now)));
        }
        let rows = query.fetch_all(&self.pool).await.map_err(storage_error)?;
        let mut windows = HashMap::new();
        for row in rows {
            let id: i64 = row.try_get("account_id").map_err(storage_error)?;
            let kind: String = row.try_get("kind").map_err(storage_error)?;
            let requests: i64 = row.try_get("requests").map_err(storage_error)?;
            let tokens: i64 = row.try_get("tokens").map_err(storage_error)?;
            let cost_usd: f64 = row.try_get("cost").map_err(storage_error)?;
            let unpriced_requests: i64 = row.try_get("unpriced").map_err(storage_error)?;
            let partial_requests: i64 = row.try_get("partial").map_err(storage_error)?;
            windows.insert(
                (id, kind),
                (
                    requests,
                    tokens,
                    cost_usd,
                    unpriced_requests,
                    partial_requests,
                ),
            );
        }
        for (id, kind, bound) in bounds {
            let (requests, tokens, cost_usd, unpriced_requests, partial_requests) = windows
                .remove(&(id, kind.into()))
                .ok_or_else(|| AppError::Internal("窗口统计结果缺失".into()))?;
            if !cost_usd.is_finite() {
                return Err(AppError::Internal("窗口费用超出范围".into()));
            }
            let coverage_complete = coverage_since <= bound.starts_at
                && gap_ms < bound.starts_at.timestamp_millis()
                && unpriced_requests == 0
                && partial_requests == 0;
            let estimated_total_cost_usd = bound
                .utilization
                .filter(|pct| coverage_complete && *pct > 0.0 && cost_usd > 0.0)
                .map(|pct| cost_usd * 100.0 / pct)
                .filter(|cost| cost.is_finite());
            let window = WindowUsage {
                requests,
                tokens,
                cost_usd,
                unpriced_requests,
                partial_requests,
                starts_at: bound.starts_at,
                ends_at: bound.ends_at,
                basis: bound.basis,
                coverage_complete,
                estimated_total_cost_usd,
            };
            let item = result.entry(id).or_insert_with(|| RequestUsage {
                recorded_since,
                five_hour: window.clone(),
                seven_day: window.clone(),
            });
            if kind == "five_hour" {
                item.five_hour = window;
            } else {
                item.seven_day = window;
            }
        }
        Ok(result)
    }
}

/// 后端选定的窗口范围及可用于外推的官方占用率。
struct WindowBounds {
    starts_at: DateTime<Utc>,
    ends_at: DateTime<Utc>,
    basis: &'static str,
    utilization: Option<f64>,
}

fn window_bounds(data: &Value, key: &str, duration: Duration, now: DateTime<Utc>) -> WindowBounds {
    let window = &data[key];
    let reset = window["resets_at"]
        .as_str()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    if let Some(reset) = reset.filter(|reset| *reset > now && *reset <= now + duration) {
        WindowBounds {
            starts_at: reset - duration,
            ends_at: reset,
            basis: "quota",
            utilization: window["utilization"]
                .as_f64()
                .filter(|pct| pct.is_finite() && *pct >= 0.0),
        }
    } else {
        WindowBounds {
            starts_at: now - duration,
            ends_at: now,
            basis: "rolling",
            utilization: None,
        }
    }
}

fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn storage_error(error: sqlx::Error) -> AppError {
    AppError::Internal(format!("用量统计数据库操作失败: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::account_store::AccountStore;
    use crate::store::db;
    use serde_json::json;
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    use uuid::Uuid;

    async fn fixture() -> (AnyPool, UsageStore, Account) {
        install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        db::migrate(&pool, "sqlite").await.unwrap();
        sqlx::query("INSERT INTO accounts(email,token,device_id) VALUES('usage@example.invalid','synthetic','synthetic')")
            .execute(&pool).await.unwrap();
        let account = AccountStore::new(pool.clone(), "sqlite".into())
            .list()
            .await
            .unwrap()
            .remove(0);
        let mut store = UsageStore::new(pool.clone());
        store.coverage_started_at = Utc::now() - Duration::days(10);
        sqlx::query("UPDATE usage_stats_meta SET recorded_since=$1 WHERE id=1")
            .bind(timestamp(store.coverage_started_at))
            .execute(&pool)
            .await
            .unwrap();
        (pool, store, account)
    }

    fn record(
        account_id: i64,
        started_at: DateTime<Utc>,
        cost_usd: Option<f64>,
        partial: bool,
    ) -> UsageRecord {
        UsageRecord {
            record_id: Uuid::new_v4().to_string(),
            account_id,
            started_at,
            model: "claude-sonnet-5".into(),
            usage: MessageTelemetryUsage {
                input_tokens: 100,
                output_tokens: 10,
                cache_read_input_tokens: 20,
                cache_creation_input_tokens: 30,
                ..Default::default()
            },
            cost_usd,
            partial,
        }
    }

    #[tokio::test]
    async fn refresh_keeps_records_and_each_reset_selects_its_own_cycle() {
        let (pool, store, mut account) = fixture().await;
        let now = Utc::now();
        account.usage_data = json!({
            "five_hour":{"utilization":10,"resets_at":timestamp(now+Duration::hours(1))},
            "seven_day":{"utilization":25,"resets_at":timestamp(now+Duration::days(1))}
        });
        for (age, cost) in [
            (Duration::days(7), 1.0),
            (Duration::hours(5), 2.0),
            (Duration::hours(4), 3.0),
            (Duration::hours(1), 4.0),
        ] {
            let item = record(account.id, now - age, Some(cost), false);
            store.record(&item).await.unwrap();
            store.record(&item).await.unwrap();
        }
        for _ in 0..2 {
            let stats = store
                .summaries(&[account.clone()], now)
                .await
                .unwrap()
                .remove(&account.id)
                .unwrap();
            assert_eq!(stats.five_hour.requests, 2);
            assert_eq!(stats.seven_day.requests, 3);
            assert_eq!(stats.five_hour.tokens, 320);
            assert_eq!(stats.five_hour.cost_usd, 7.0);
            assert_eq!(stats.five_hour.estimated_total_cost_usd, Some(70.0));
            assert_eq!(stats.seven_day.estimated_total_cost_usd, Some(36.0));
        }
        store
            .record(&record(
                account.id,
                now + Duration::minutes(70),
                Some(5.0),
                false,
            ))
            .await
            .unwrap();
        account.usage_data["five_hour"]["resets_at"] = json!(timestamp(now + Duration::hours(6)));
        let stats = store
            .summaries(&[account.clone()], now + Duration::minutes(90))
            .await
            .unwrap()
            .remove(&account.id)
            .unwrap();
        assert_eq!(stats.five_hour.requests, 1);
        assert_eq!(stats.five_hour.cost_usd, 5.0);
        assert_eq!(stats.seven_day.requests, 4);
        account.usage_data["seven_day"]["resets_at"] = json!(timestamp(now + Duration::days(8)));
        store
            .record(&record(
                account.id,
                now + Duration::days(1) + Duration::minutes(30),
                Some(6.0),
                false,
            ))
            .await
            .unwrap();
        let stats = store
            .summaries(
                &[account.clone()],
                now + Duration::days(1) + Duration::hours(1),
            )
            .await
            .unwrap()
            .remove(&account.id)
            .unwrap();
        assert_eq!(stats.seven_day.requests, 1);
        assert_eq!(stats.seven_day.cost_usd, 6.0);
        assert_eq!(stats.five_hour.basis, "rolling");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_records")
                .fetch_one(&pool)
                .await
                .unwrap(),
            6
        );
    }

    #[tokio::test]
    async fn unknown_or_incomplete_data_never_produces_total_estimate() {
        let (_, store, mut account) = fixture().await;
        let now = Utc::now();
        store
            .record(&record(account.id, now - Duration::minutes(1), None, true))
            .await
            .unwrap();
        for window in [
            json!({}),
            json!({"utilization":10,"resets_at":"bad"}),
            json!({"utilization":10,"resets_at":timestamp(now-Duration::seconds(1))}),
            json!({"utilization":0,"resets_at":timestamp(now+Duration::hours(1))}),
            json!({"utilization":10,"resets_at":timestamp(now+Duration::hours(1))}),
        ] {
            account.usage_data = json!({"five_hour":window});
            let stats = store
                .summaries(&[account.clone()], now)
                .await
                .unwrap()
                .remove(&account.id)
                .unwrap();
            assert_eq!(stats.five_hour.requests, 1);
            assert_eq!(stats.five_hour.unpriced_requests, 1);
            assert_eq!(stats.five_hour.partial_requests, 1);
            assert!(!stats.five_hour.coverage_complete);
            assert!(stats.five_hour.estimated_total_cost_usd.is_none());
        }
    }

    #[tokio::test]
    async fn committed_records_survive_independent_connection_and_reopening() {
        install_default_drivers();
        let file = std::env::temp_dir().join(format!("cc2api-usage-{}.db", Uuid::new_v4()));
        let dsn = format!("sqlite:{}?mode=rwc", file.display());
        let pool = AnyPool::connect(&dsn).await.unwrap();
        db::migrate(&pool, "sqlite").await.unwrap();
        db::migrate(&pool, "sqlite").await.unwrap();
        sqlx::query("INSERT INTO accounts(email,token,device_id) VALUES('usage@example.invalid','synthetic','synthetic')").execute(&pool).await.unwrap();
        let mut account = AccountStore::new(pool.clone(), "sqlite".into())
            .list()
            .await
            .unwrap()
            .remove(0);
        let now = Utc::now();
        account.usage_data =
            json!({"five_hour":{"utilization":10,"resets_at":timestamp(now+Duration::hours(1))}});
        let store = UsageStore::new(pool.clone());
        let item = record(account.id, now - Duration::minutes(1), Some(12.5), false);
        store.record(&item).await.unwrap();
        let reader = AnyPool::connect(&dsn).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_records")
                .fetch_one(&reader)
                .await
                .unwrap(),
            1
        );
        reader.close().await;
        drop(store);
        pool.close().await;
        let reopened = AnyPool::connect(&dsn).await.unwrap();
        let store = UsageStore::new(reopened.clone());
        let stats = store
            .summaries(&[account.clone()], now)
            .await
            .unwrap()
            .remove(&account.id)
            .unwrap();
        assert_eq!(stats.five_hour.cost_usd, 12.5);
        assert_eq!(stats.five_hour.requests, 1);
        assert!(
            stats.five_hour.estimated_total_cost_usd.is_none(),
            "重启后覆盖不足不能外推整周期"
        );
        drop(store);
        reopened.close().await;
        let _ = std::fs::remove_file(file);
    }
}
