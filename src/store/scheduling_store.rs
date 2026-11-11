use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use serde::Serialize;
use sqlx::any::AnyRow;
use sqlx::{AnyPool, Error as SqlxError, Row};
use tokio::time::Instant;
use uuid::Uuid;

use crate::error::AppError;

/// 网关绑定的闲置有效期；活动租约续期，不保存请求正文。
pub const BINDING_TTL_MS: i64 = 60 * 60 * 1000;
// SQLx Any 会把 SQLite 运行时 INTEGER 映射为 i32；文本投影保留完整 UTC 毫秒和账号 ID。
const BINDING_COLUMNS: &str = "session_key,CAST(account_id AS TEXT) AS account_id,revision,CAST(created_at_ms AS TEXT) AS created_at_ms,CAST(last_seen_at_ms AS TEXT) AS last_seen_at_ms,CAST(expires_at_ms AS TEXT) AS expires_at_ms";

/// 一个按 Token 隔离的匿名会话绑定快照。
#[derive(Clone, Debug, Serialize)]
pub struct SessionBinding {
    pub session_key: String,
    pub account_id: i64,
    pub revision: String,
    pub created_at_ms: i64,
    pub last_seen_at_ms: i64,
    pub expires_at_ms: i64,
}

/// 同一事务内裁决绑定和发送间隔的结果。
#[derive(Debug)]
pub enum SendClaim {
    /// 已取得一个当前许可，绑定与许可同时提交。
    Acquired(Option<SessionBinding>),
    /// 尚未到许可时间；没有修改绑定或提前预订名额。
    WaitUntil(i64),
    /// 另一个请求已改变绑定；本次发送预算已回滚。
    BindingConflict,
}

/// SQLite／PostgreSQL 共用的会话及发送状态存储。
#[derive(Clone)]
pub struct SchedulingStore {
    pool: AnyPool,
}

impl SchedulingStore {
    /// 使用现有数据库连接池创建调度存储。
    /// @param pool 已初始化的 SQLite 或 PostgreSQL 池。
    /// @return 调度存储。
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }

    /// 幂等增加状态表与索引，不改变原账号和缓存数据。
    /// @param pool 已初始化的数据库池。
    /// @return 迁移成功或数据库错误。
    pub async fn migrate(pool: &AnyPool) -> Result<(), SqlxError> {
        super::database_schema::migrate_scheduling(pool).await
    }

    /// 只读取得尚未过期的绑定，不延长有效期。
    /// @param key 按 Token 隔离的摘要。
    /// @param now_ms 当前 UTC 毫秒。
    /// @return 有效快照或空值；读取失败停止准入。
    pub async fn binding(
        &self,
        key: &str,
        now_ms: i64,
    ) -> Result<Option<SessionBinding>, AppError> {
        let sql = format!(
            "SELECT {BINDING_COLUMNS} FROM gateway_session_bindings WHERE session_key=$1 AND expires_at_ms>$2"
        );
        let row = sqlx::query(&sql)
            .bind(key)
            .bind(now_ms)
            .fetch_optional(&self.pool)
            .await
            .map_err(unavailable)?;
        row.map(binding_from_row).transpose()
    }

    /// 根据最近一次许可计算最早可发送时间，读取不消费许可。
    /// @param account_id 账号 ID。
    /// @param interval_ms 当前 RPM 对应的间隔。
    /// @return 最近许可加当前间隔，无历史时为零。
    pub async fn next_send(&self, account_id: i64, interval_ms: i64) -> Result<i64, AppError> {
        if interval_ms == 0 {
            return Ok(0);
        }
        let row = sqlx::query(
            "SELECT CAST(last_admitted_at_ms AS TEXT) AS last_admitted_at_ms FROM gateway_account_send_state WHERE account_id=$1",
        )
        .bind(account_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(unavailable)?;
        row.map(|r| {
            integer_from_row(&r, "last_admitted_at_ms").map(|last| last.saturating_add(interval_ms))
        })
        .transpose()
        .map(|next| next.unwrap_or(0))
    }

    /// 用显式时间和间隔原子裁决，供受控时钟验证持久状态合同。
    /// @param account_id 本次准备提交的账号。
    /// @param interval_ms 当前最小提交间隔，零表示无限制。
    /// @param now_ms 本次裁决的 UTC 毫秒。
    /// @param key 显式会话的隔离摘要；无 ID 时为空。
    /// @param expected 选号时读取的版本；允许既有硬资格规则触发替换。
    /// @return 已提交、需要等待或绑定冲突；后两者均不消耗许可。
    pub async fn claim(
        &self,
        account_id: i64,
        interval_ms: i64,
        now_ms: i64,
        key: Option<&str>,
        expected: Option<&SessionBinding>,
    ) -> Result<SendClaim, AppError> {
        self.claim_inner(account_id, key, expected, Some((interval_ms, now_ms)), None)
            .await
    }

    /// 生产发送裁决：取得写锁后读取当前 RPM，同时检查上次提交后的真实间隔。
    /// @param account_id 已验证身份的账号 ID，不重读或改写协议身份。
    /// @param key 可选的 Token 隔离摘要。
    /// @param expected 选号时读取的绑定版本。
    /// @param last_released_at 上一次许可释放／提交记点的单调时钟。
    /// @return 当前许可、最早重试时刻或绑定冲突；等待不消费名额。
    pub async fn claim_current(
        &self,
        account_id: i64,
        key: Option<&str>,
        expected: Option<&SessionBinding>,
        last_released_at: Option<Instant>,
    ) -> Result<SendClaim, AppError> {
        self.claim_inner(account_id, key, expected, None, last_released_at)
            .await
    }

    /// 在一个短事务内共享强约束，显式测试时钟不进入生产选号路径。
    /// @param account_id 当前账号。
    /// @param key 匿名会话摘要。
    /// @param expected 原绑定版本。
    /// @param fixed 测试指定的间隔和时间；生产为 None。
    /// @param last_released_at 当前进程的上次许可释放时刻。
    /// @return 不提前消费预算的原子裁决。
    async fn claim_inner(
        &self,
        account_id: i64,
        key: Option<&str>,
        expected: Option<&SessionBinding>,
        fixed: Option<(i64, i64)>,
        last_released_at: Option<Instant>,
    ) -> Result<SendClaim, AppError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        // 先取得发送行的写入裁决，避免 SQLite 先读后写造成快照升级竞争。
        sqlx::query("INSERT INTO gateway_account_send_state(account_id,last_admitted_at_ms) VALUES($1,0) ON CONFLICT(account_id) DO NOTHING")
            .bind(account_id).execute(&mut *tx).await.map_err(unavailable)?;
        // 无操作更新取得发送行的写锁；必须在锁等待结束后采样，不能让等待抵扣间隔。
        let rows = sqlx::query("UPDATE gateway_account_send_state SET last_admitted_at_ms=last_admitted_at_ms WHERE account_id=$1 RETURNING CAST(last_admitted_at_ms AS TEXT) AS last_admitted_at_ms")
            .bind(account_id).fetch_all(&mut *tx).await.map_err(unavailable)?;
        let last = integer_from_row(
            rows.first()
                .ok_or_else(|| unavailable(SqlxError::RowNotFound))?,
            "last_admitted_at_ms",
        )?;
        let (interval_ms, now_ms) = match fixed {
            Some(timing) => timing,
            None => {
                let row = sqlx::query(
                    "SELECT CAST(rpm_limit AS TEXT) AS rpm_limit FROM accounts WHERE id=$1",
                )
                .bind(account_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(unavailable)?;
                let rpm = i32::try_from(integer_from_row(&row, "rpm_limit")?)
                    .map_err(|_| AppError::ServiceUnavailable("账号 RPM 配置不可用".into()))?;
                (send_interval_ms(rpm), Utc::now().timestamp_millis())
            }
        };
        let local_wait = last_released_at
            .map(|at| Duration::from_millis(interval_ms.max(0) as u64).saturating_sub(at.elapsed()))
            .unwrap_or_default();
        let local_wait_ms = local_wait.as_nanos().div_ceil(1_000_000) as i64;
        let persisted_ready = if interval_ms == 0 || last == 0 {
            now_ms
        } else {
            last.saturating_add(interval_ms)
        };
        let ready_at = persisted_ready.max(now_ms.saturating_add(local_wait_ms));
        if ready_at > now_ms {
            tx.rollback().await.map_err(unavailable)?;
            return Ok(SendClaim::WaitUntil(ready_at));
        }
        let binding = if let Some(key) = key {
            let revision = Uuid::new_v4().to_string();
            let expected_revision = expected.map(|b| b.revision.as_str()).unwrap_or("");
            let sql = format!(
                "INSERT INTO gateway_session_bindings(session_key,account_id,revision,created_at_ms,last_seen_at_ms,expires_at_ms) VALUES($1,$2,$3,$4,$4,$5) ON CONFLICT(session_key) DO UPDATE SET account_id=excluded.account_id, revision=CASE WHEN gateway_session_bindings.account_id=excluded.account_id AND gateway_session_bindings.expires_at_ms>$4 THEN gateway_session_bindings.revision ELSE excluded.revision END, created_at_ms=CASE WHEN gateway_session_bindings.account_id=excluded.account_id AND gateway_session_bindings.expires_at_ms>$4 THEN gateway_session_bindings.created_at_ms ELSE excluded.created_at_ms END, last_seen_at_ms=CASE WHEN gateway_session_bindings.last_seen_at_ms>excluded.last_seen_at_ms THEN gateway_session_bindings.last_seen_at_ms ELSE excluded.last_seen_at_ms END, expires_at_ms=CASE WHEN gateway_session_bindings.expires_at_ms>excluded.expires_at_ms AND gateway_session_bindings.account_id=excluded.account_id THEN gateway_session_bindings.expires_at_ms ELSE excluded.expires_at_ms END WHERE gateway_session_bindings.expires_at_ms<=$4 OR gateway_session_bindings.revision=$6 OR gateway_session_bindings.account_id=excluded.account_id RETURNING {BINDING_COLUMNS}"
            );
            let rows = sqlx::query(&sql)
                .bind(key)
                .bind(account_id)
                .bind(revision)
                .bind(now_ms)
                .bind(now_ms.saturating_add(BINDING_TTL_MS))
                .bind(expected_revision)
                .fetch_all(&mut *tx)
                .await
                .map_err(unavailable)?;
            let Some(row) = rows.into_iter().next() else {
                tx.rollback().await.map_err(unavailable)?;
                return Ok(SendClaim::BindingConflict);
            };
            Some(binding_from_row(row)?)
        } else {
            None
        };
        // 绑定 CAS 也可能等待其它事务；最后才记录本次认领时刻，冲突不消耗预算。
        let admitted_at = fixed
            .map(|(_, at)| at)
            .unwrap_or_else(|| Utc::now().timestamp_millis());
        sqlx::query(
            "UPDATE gateway_account_send_state SET last_admitted_at_ms=$2 WHERE account_id=$1",
        )
        .bind(account_id)
        .bind(admitted_at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(SendClaim::Acquired(binding))
    }

    /// 仅对当前版本续租；监控查询不得调用此方法。
    /// @param binding 需要保持有效的绑定版本。
    /// @param now_ms 当前 UTC 毫秒。
    /// @param activity 是否来自真实请求活动，后台续租传 false。
    /// @return 是否仍持有该版本。
    pub async fn renew(
        &self,
        binding: &SessionBinding,
        now_ms: i64,
        activity: bool,
    ) -> Result<bool, AppError> {
        let result = sqlx::query("UPDATE gateway_session_bindings SET expires_at_ms=CASE WHEN expires_at_ms>$4 THEN expires_at_ms ELSE $4 END,last_seen_at_ms=CASE WHEN CAST($5 AS BIGINT)=1 AND last_seen_at_ms<$6 THEN $6 ELSE last_seen_at_ms END WHERE session_key=$1 AND account_id=$2 AND revision=$3")
            .bind(&binding.session_key).bind(binding.account_id).bind(&binding.revision)
            .bind(now_ms.saturating_add(BINDING_TTL_MS)).bind(if activity {1_i64} else {0_i64}).bind(now_ms)
            .execute(&self.pool).await.map_err(unavailable)?;
        Ok(result.rows_affected() == 1)
    }

    /// 批量读取指定账号的有效绑定数量。
    /// @param ids 当前管理页的账号 ID。
    /// @param now_ms 当前 UTC 毫秒。
    /// @return 账号到有效绑定数的映射。
    pub async fn counts(&self, ids: &[i64], now_ms: i64) -> Result<HashMap<i64, i64>, AppError> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = (0..ids.len())
            .map(|i| format!("${}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT CAST(account_id AS TEXT) AS account_id,CAST(COUNT(*) AS TEXT) AS count FROM gateway_session_bindings WHERE expires_at_ms>$1 AND account_id IN ({placeholders}) GROUP BY account_id"
        );
        let mut query = sqlx::query(&sql).bind(now_ms);
        for id in ids {
            query = query.bind(*id);
        }
        let rows = query.fetch_all(&self.pool).await.map_err(unavailable)?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    integer_from_row(&r, "account_id")?,
                    integer_from_row(&r, "count")?,
                ))
            })
            .collect()
    }

    /// 分页查询绑定，活跃筛选由当前租约摘要集合确定。
    /// @param account_id 目标账号。
    /// @param now_ms 当前 UTC 毫秒。
    /// @param page 从一开始的页码。
    /// @param page_size 每页数量，调用方限制不超过一百。
    /// @param prefix 可选十六进制摘要前缀。
    /// @param active_keys 当前账号正在等待／执行的会话摘要。
    /// @param active 筛选活跃或闲置；None 不筛选。
    /// @return 本页及未分页总数。
    pub async fn sessions(
        &self,
        account_id: i64,
        now_ms: i64,
        page: i64,
        page_size: i64,
        prefix: &str,
        active_keys: &[String],
        active: Option<bool>,
    ) -> Result<(Vec<SessionBinding>, i64), AppError> {
        let mut predicate =
            "account_id=$1 AND expires_at_ms>$2 AND session_key LIKE $3".to_string();
        if let Some(active) = active {
            if active_keys.is_empty() {
                if active {
                    predicate.push_str(" AND 1=0");
                }
            } else {
                let placeholders = (0..active_keys.len())
                    .map(|i| format!("${}", i + 4))
                    .collect::<Vec<_>>()
                    .join(",");
                predicate.push_str(&format!(
                    " AND session_key {}IN ({placeholders})",
                    if active { "" } else { "NOT " }
                ));
            }
        }
        let key_count = if active.is_some() {
            active_keys.len()
        } else {
            0
        };
        let pattern = format!("{prefix}%");
        let total_sql = format!(
            "SELECT CAST(COUNT(*) AS TEXT) AS count FROM gateway_session_bindings WHERE {predicate}"
        );
        let mut query = sqlx::query(&total_sql)
            .bind(account_id)
            .bind(now_ms)
            .bind(&pattern);
        if active.is_some() {
            for key in active_keys {
                query = query.bind(key);
            }
        }
        let row = query.fetch_one(&self.pool).await.map_err(unavailable)?;
        let total = integer_from_row(&row, "count")?;
        let sql = format!(
            "SELECT {BINDING_COLUMNS} FROM gateway_session_bindings WHERE {predicate} ORDER BY gateway_session_bindings.last_seen_at_ms DESC,session_key LIMIT ${} OFFSET ${}",
            key_count + 4,
            key_count + 5
        );
        let mut query = sqlx::query(&sql)
            .bind(account_id)
            .bind(now_ms)
            .bind(pattern);
        if active.is_some() {
            for key in active_keys {
                query = query.bind(key);
            }
        }
        let rows = query
            .bind(page_size)
            .bind((page - 1).saturating_mul(page_size))
            .fetch_all(&self.pool)
            .await
            .map_err(unavailable)?;
        Ok((
            rows.into_iter()
                .map(binding_from_row)
                .collect::<Result<Vec<_>, _>>()?,
            total,
        ))
    }

    /// 与账号删除一起原子清理新状态，不依赖 SQLite 单连接的外键开关。
    /// @param account_id 已由原管理门禁允许删除的账号。
    /// @return 整体提交成功或存储错误；无部分清理。
    pub async fn delete_account(&self, account_id: i64) -> Result<(), AppError> {
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        for sql in [
            "DELETE FROM gateway_session_bindings WHERE account_id=$1",
            "DELETE FROM gateway_account_send_state WHERE account_id=$1",
            "DELETE FROM oauth_handoff_operations WHERE account_id=$1",
            "DELETE FROM oauth_account_ownerships WHERE account_id=$1",
            "DELETE FROM accounts WHERE id=$1",
        ] {
            sqlx::query(sql)
                .bind(account_id)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }

    /// 批量延长真实活动绑定的当前版本，不改最近请求活动时间。
    /// @param bindings 去重后的当前版本。
    /// @param now_ms UTC 毫秒。
    /// @return 续租完成或数据库不可用。
    pub async fn renew_active(
        &self,
        bindings: &[SessionBinding],
        now_ms: i64,
    ) -> Result<(), AppError> {
        for chunk in bindings.chunks(200) {
            let predicates = (0..chunk.len())
                .map(|i| format!("(session_key=${} AND revision=${})", i * 2 + 2, i * 2 + 3))
                .collect::<Vec<_>>()
                .join(" OR ");
            let sql = format!(
                "UPDATE gateway_session_bindings SET expires_at_ms=CASE WHEN expires_at_ms>$1 THEN expires_at_ms ELSE $1 END WHERE {predicates}"
            );
            let mut query = sqlx::query(&sql).bind(now_ms.saturating_add(BINDING_TTL_MS));
            for binding in chunk {
                query = query.bind(&binding.session_key).bind(&binding.revision);
            }
            query.execute(&self.pool).await.map_err(unavailable)?;
        }
        Ok(())
    }

    /// 有界清理过期绑定，不在管理读取时删除记录。
    /// @param now_ms 当前 UTC 毫秒。
    /// @return 成功或数据库错误。
    pub async fn cleanup(&self, now_ms: i64) -> Result<(), AppError> {
        self.cleanup_protected(now_ms, &[]).await
    }

    /// 只清理没有活动租约保护的过期 key，删除时再次检查过期时间。
    /// @param now_ms 当前 UTC 毫秒。
    /// @param protected 活动租约持有的 key。
    /// @return 有界清理结果。
    pub async fn cleanup_protected(
        &self,
        now_ms: i64,
        protected: &[String],
    ) -> Result<(), AppError> {
        self.cleanup_batch(now_ms, protected).await.map(|_| ())
    }

    /// 清理一个有界批次，删除时再次核对有效期，避免误删刚续租的行。
    /// @param now_ms 当前 UTC 毫秒。
    /// @param protected 当前活动 key。
    /// @return 实际删除数量。
    pub(crate) async fn cleanup_batch(
        &self,
        now_ms: i64,
        protected: &[String],
    ) -> Result<u64, AppError> {
        let rows=sqlx::query("SELECT session_key FROM gateway_session_bindings WHERE expires_at_ms<=$1 ORDER BY expires_at_ms LIMIT 200").bind(now_ms).fetch_all(&self.pool).await.map_err(unavailable)?;
        let keys = rows
            .into_iter()
            .map(|r| r.get::<String, _>(0))
            .filter(|key| !protected.contains(key))
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(0);
        }
        let placeholders = (0..keys.len())
            .map(|i| format!("${}", i + 2))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "DELETE FROM gateway_session_bindings WHERE expires_at_ms<=$1 AND session_key IN ({placeholders})"
        );
        let mut query = sqlx::query(&sql).bind(now_ms);
        for key in keys {
            query = query.bind(key);
        }
        Ok(query
            .execute(&self.pool)
            .await
            .map_err(unavailable)?
            .rows_affected())
    }
}

/// 根据账号 RPM 计算最小间隔，供持久裁决和管理展示共用。
/// @param rpm 配置的每分钟次数，非正数表示无限制。
/// @return 向上取整的毫秒间隔。
pub fn send_interval_ms(rpm: i32) -> i64 {
    if rpm <= 0 {
        0
    } else {
        (60000 + i64::from(rpm) - 1) / i64::from(rpm)
    }
}

fn binding_from_row(row: AnyRow) -> Result<SessionBinding, AppError> {
    Ok(SessionBinding {
        session_key: row.try_get("session_key").map_err(unavailable)?,
        account_id: integer_from_row(&row, "account_id")?,
        revision: row.try_get("revision").map_err(unavailable)?,
        created_at_ms: integer_from_row(&row, "created_at_ms")?,
        last_seen_at_ms: integer_from_row(&row, "last_seen_at_ms")?,
        expires_at_ms: integer_from_row(&row, "expires_at_ms")?,
    })
}

/// 严格解析整数文本，非法状态故障关闭，不把截断时间或默认零放行。
/// @param row 显式文本投影的行。
/// @param column 已定义的整数列名。
/// @return 完整 i64 或固定存储错误。
fn integer_from_row(row: &AnyRow, column: &str) -> Result<i64, AppError> {
    row.try_get::<String, _>(column)
        .map_err(unavailable)?
        .parse()
        .map_err(|_| AppError::ServiceUnavailable("调度状态数值不可用".into()))
}

fn unavailable(_: SqlxError) -> AppError {
    AppError::ServiceUnavailable("调度状态存储不可用".into())
}
