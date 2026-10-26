use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use serde::de::DeserializeOwned;
use sqlx::{AnyPool, Row};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::error::AppError;
use crate::model::request_diagnostics::{
    AttemptDiagnostic, DiagnosticDetail, DiagnosticHealth, DiagnosticPage, DiagnosticQuery,
    RequestDiagnostic, normalize_correlation_id,
};

const QUEUE_CAPACITY: usize = 1024;
const MAX_RECORD_BYTES: usize = 16 * 1024;
const FILTER: &str = "WHERE ($1=0 OR r.account_id=$1 OR EXISTS(SELECT 1 FROM request_diagnostic_attempts a WHERE a.request_id=r.request_id AND a.account_id=$1))
 AND ($2='' OR r.request_id=$2 OR r.response_request_id=$2 OR EXISTS(SELECT 1 FROM request_diagnostic_attempts a WHERE a.request_id=r.request_id AND a.upstream_request_id=$2))
 AND ($3='' OR r.outcome=$3) AND ($4='' OR r.started_at >= $4) AND ($5='' OR r.started_at < $5)";

/// 一次无正文的诊断写入；尝试完成时同时保证父请求存在。
pub(crate) struct DiagnosticEvent {
    pub request: RequestDiagnostic,
    pub attempt: Option<AttemptDiagnostic>,
}

enum WriteCommand {
    Record(DiagnosticEvent),
    Flush(oneshot::Sender<()>),
}

/// 独立诊断持久化与有界后台写入，不参与费用统计。
pub struct DiagnosticsStore {
    pool: AnyPool,
    sender: mpsc::Sender<WriteCommand>,
    dropped: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
    observation_failures: AtomicU64,
}

impl DiagnosticsStore {
    /// 创建后台写入器，须在 Tokio 运行时内调用。
    /// @param pool 已迁移的共享数据库连接池。
    /// @return 可供网关与管理查询共享的存储。
    pub fn new(pool: AnyPool) -> Self {
        let (sender, mut receiver) = mpsc::channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let failures = Arc::new(AtomicU64::new(0));
        let writer_pool = pool.clone();
        let writer_failures = failures.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tokio::select! {
                    command = receiver.recv() => match command {
                        Some(WriteCommand::Record(event)) => {
                            if write_event(&writer_pool, &event).await.is_err() {
                                writer_failures.fetch_add(1, Ordering::Relaxed);
                                warn!(request_id = %event.request.request_id, "请求诊断写入失败");
                            }
                        }
                        Some(WriteCommand::Flush(done)) => { let _ = done.send(()); }
                        None => break,
                    },
                    _ = interval.tick() => {
                        if prune(&writer_pool, Utc::now()).await.is_err() {
                            writer_failures.fetch_add(1, Ordering::Relaxed);
                            warn!("请求诊断历史清理失败");
                        }
                    }
                }
            }
        });
        Self {
            pool,
            sender,
            dropped,
            failures,
            observation_failures: AtomicU64::new(0),
        }
    }

    /// 幂等创建 SQLite/PostgreSQL 共用的诊断表与索引。
    /// @param pool 数据库连接池。
    /// @return 迁移结果。
    pub async fn migrate(pool: &AnyPool) -> Result<(), sqlx::Error> {
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

    /// 尝试入队，队列饱和时只记录失败计数，不阻塞转发。
    /// @param event 已完成白名单提取的诊断事件。
    /// @return 无返回值。
    pub(crate) fn submit(&self, event: DiagnosticEvent) {
        if self.sender.try_send(WriteCommand::Record(event)).is_err() {
            let count = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if count == 1 || count % 100 == 0 {
                warn!(dropped_records = count, "请求诊断队列不可用或已满");
            }
        }
    }

    /// 计数有界采样或解码失败，只输出固定诊断文案。
    /// @return 无返回值。
    pub(crate) fn observation_failed(&self) {
        let count = self.observation_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if count == 1 || count % 100 == 0 {
            warn!(observation_failures = count, "请求诊断响应采样不完整");
        }
    }

    /// 等待此前队列中的写入结束，用于受控收尾和验证。
    /// @return 写入器可用且队列已消费，或明确的存储不可用错误。
    pub async fn flush(&self) -> Result<(), AppError> {
        let (done, wait) = oneshot::channel();
        tokio::time::timeout(Duration::from_secs(5), async {
            self.sender
                .send(WriteCommand::Flush(done))
                .await
                .map_err(|_| unavailable())?;
            wait.await.map_err(|_| unavailable())
        })
        .await
        .map_err(|_| unavailable())?
    }

    /// 分页读取诊断，不触发清理或上游请求。
    /// @param query 管理员提供的过滤条件。
    /// @return 请求列表、总数与采集失败计数。
    pub async fn list(&self, query: DiagnosticQuery) -> Result<DiagnosticPage, AppError> {
        let page = query.page.unwrap_or(1);
        let page_size = query.page_size.unwrap_or(50);
        if page == 0 || !(1..=200).contains(&page_size) || query.account_id.is_some_and(|v| v <= 0)
        {
            return Err(AppError::BadRequest("无效的诊断分页或账号参数".into()));
        }
        let id = query.correlation_id.unwrap_or_default();
        if id.len() > 128 {
            return Err(AppError::BadRequest("关联标识过长".into()));
        }
        let id = if id.is_empty() {
            id
        } else {
            normalize_correlation_id(&id)
        };
        let outcome = query.outcome.unwrap_or_default();
        if !outcome.is_empty()
            && ![
                "in_progress",
                "success",
                "local_rejected",
                "local_response",
                "mock",
                "cache_hit",
                "upstream_rejected",
                "upstream_error",
                "transport_error",
                "body_error",
                "cancelled",
                "incomplete",
            ]
            .contains(&outcome.as_str())
        {
            return Err(AppError::BadRequest("未知的诊断结果".into()));
        }
        let since = query_time(query.since)?;
        let until = query_time(query.until)?;
        if !since.is_empty() && !until.is_empty() && since >= until {
            return Err(AppError::BadRequest("诊断时间范围无效".into()));
        }
        let account = query.account_id.unwrap_or(0);
        let sql = format!("SELECT CAST(COUNT(*) AS BIGINT) FROM request_diagnostics r {FILTER}");
        let total: i64 = sqlx::query_scalar(&sql)
            .bind(account)
            .bind(&id)
            .bind(&outcome)
            .bind(&since)
            .bind(&until)
            .fetch_one(&self.pool)
            .await
            .map_err(|_| unavailable())?;
        let sql = format!(
            "SELECT r.summary_json FROM request_diagnostics r {FILTER} ORDER BY r.started_at DESC,r.request_id DESC LIMIT $6 OFFSET $7"
        );
        let rows = sqlx::query(&sql)
            .bind(account)
            .bind(&id)
            .bind(&outcome)
            .bind(&since)
            .bind(&until)
            .bind(i64::from(page_size))
            .bind(i64::from(page - 1) * i64::from(page_size))
            .fetch_all(&self.pool)
            .await
            .map_err(|_| unavailable())?;
        let items = rows
            .iter()
            .map(|row| decode(row.try_get("summary_json").map_err(|_| unavailable())?))
            .collect::<Result<_, _>>()?;
        Ok(DiagnosticPage {
            items,
            total,
            page,
            page_size,
            health: DiagnosticHealth {
                dropped_records: self.dropped.load(Ordering::Relaxed),
                write_failures: self.failures.load(Ordering::Relaxed),
                observation_failures: self.observation_failures.load(Ordering::Relaxed),
            },
        })
    }

    /// 按内部请求 UUID 获取完整尝试链。
    /// @param request_id 列表或响应返回的内部请求 UUID。
    /// @return 请求详情；不存在时返回 NotFound。
    pub async fn detail(&self, request_id: &str) -> Result<DiagnosticDetail, AppError> {
        let value: Option<String> =
            sqlx::query_scalar("SELECT summary_json FROM request_diagnostics WHERE request_id=$1")
                .bind(request_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|_| unavailable())?;
        let request = decode(value.ok_or(AppError::NotFound)?)?;
        let values: Vec<String> = sqlx::query_scalar("SELECT summary_json FROM request_diagnostic_attempts WHERE request_id=$1 ORDER BY ordinal,attempt_id")
            .bind(request_id).fetch_all(&self.pool).await.map_err(|_| unavailable())?;
        let attempts = values.into_iter().map(decode).collect::<Result<_, _>>()?;
        Ok(DiagnosticDetail { request, attempts })
    }
}

fn unavailable() -> AppError {
    AppError::ServiceUnavailable("请求诊断存储不可用".into())
}

fn decode<T: DeserializeOwned>(value: String) -> Result<T, AppError> {
    serde_json::from_str(&value).map_err(|_| unavailable())
}

fn query_time(value: Option<String>) -> Result<String, AppError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|value| {
                    value
                        .with_timezone(&Utc)
                        .to_rfc3339_opts(SecondsFormat::Millis, true)
                })
                .map_err(|_| AppError::BadRequest("诊断时间须为 RFC3339".into()))
        })
        .transpose()
        .map(|v| v.unwrap_or_default())
}

async fn write_event(pool: &AnyPool, event: &DiagnosticEvent) -> Result<(), sqlx::Error> {
    let request = &event.request;
    let json = serde_json::to_string(request)
        .map_err(|_| sqlx::Error::Protocol("诊断序列化失败".into()))?;
    let attempt_json = event
        .attempt
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|_| sqlx::Error::Protocol("诊断序列化失败".into()))?;
    if json.len() > MAX_RECORD_BYTES
        || attempt_json
            .as_ref()
            .is_some_and(|json| json.len() > MAX_RECORD_BYTES)
    {
        return Err(sqlx::Error::Protocol("诊断摘要超出上限".into()));
    }
    let mut tx = pool.begin().await?;
    // 迟到的尝试不能把已经终结的请求重新写成进行中。
    sqlx::query("INSERT INTO request_diagnostics(request_id,started_at,finished_at,response_request_id,outcome,summary_json,account_id)
        VALUES($1,$2,NULLIF($3,''),NULLIF($4,''),$5,$6,NULLIF($7,0)) ON CONFLICT(request_id) DO UPDATE SET
        finished_at=COALESCE(excluded.finished_at,request_diagnostics.finished_at),
        response_request_id=COALESCE(excluded.response_request_id,request_diagnostics.response_request_id),
        account_id=COALESCE(excluded.account_id,request_diagnostics.account_id),
        outcome=CASE WHEN excluded.finished_at IS NOT NULL THEN excluded.outcome ELSE request_diagnostics.outcome END,
        summary_json=CASE WHEN excluded.finished_at IS NOT NULL OR request_diagnostics.finished_at IS NULL THEN excluded.summary_json ELSE request_diagnostics.summary_json END")
        // SQLx Any 的空值使用 i32 编码；绑定类型固定，再用 NULLIF 还原未知值，避免 PG 缓存参数类型漂移。
        .bind(&request.request_id).bind(&request.started_at).bind(request.finished_at.as_deref().unwrap_or_default()).bind(request.response_request_id.as_deref().unwrap_or_default())
        .bind(&request.outcome).bind(json).bind(request.account_id.unwrap_or(0)).execute(&mut *tx).await?;
    if let Some(attempt) = &event.attempt {
        sqlx::query("INSERT INTO request_diagnostic_attempts(attempt_id,request_id,ordinal,account_id,started_at,upstream_request_id,summary_json,finished_at)
            VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,NULLIF($8,'')) ON CONFLICT(attempt_id) DO UPDATE SET
            finished_at=COALESCE(excluded.finished_at,request_diagnostic_attempts.finished_at),
            upstream_request_id=COALESCE(excluded.upstream_request_id,request_diagnostic_attempts.upstream_request_id),
            summary_json=CASE WHEN excluded.finished_at IS NOT NULL OR request_diagnostic_attempts.finished_at IS NULL THEN excluded.summary_json ELSE request_diagnostic_attempts.summary_json END")
            .bind(&attempt.attempt_id).bind(&attempt.request_id).bind(i64::from(attempt.ordinal)).bind(attempt.account_id)
            .bind(&attempt.started_at).bind(attempt.upstream_request_id.as_deref().unwrap_or_default()).bind(attempt_json.unwrap_or_default()).bind(attempt.finished_at.as_deref().unwrap_or_default())
            .execute(&mut *tx).await?;
    }
    tx.commit().await
}

async fn prune(pool: &AnyPool, now: DateTime<Utc>) -> Result<(), sqlx::Error> {
    let cutoff = (now - ChronoDuration::days(8)).to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM request_diagnostic_attempts WHERE request_id IN (SELECT request_id FROM request_diagnostics WHERE started_at < $1)")
        .bind(&cutoff).execute(&mut *tx).await?;
    sqlx::query("DELETE FROM request_diagnostics WHERE started_at < $1")
        .bind(cutoff)
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

#[cfg(test)]
mod tests {
    use super::{DiagnosticEvent, DiagnosticsStore, QUEUE_CAPACITY, prune, write_event};
    use crate::error::AppError;
    use crate::model::request_diagnostics::{
        AttemptDiagnostic, DiagnosticParameters, DiagnosticQuery, DiagnosticSessions,
        DiagnosticTokens, RequestDiagnostic,
    };
    use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
    use sqlx::any::AnyPoolOptions;
    use std::sync::atomic::Ordering;
    use uuid::Uuid;

    fn request(at: DateTime<Utc>) -> RequestDiagnostic {
        let request_id = Uuid::new_v4().to_string();
        RequestDiagnostic {
            request_id: request_id.clone(),
            started_at: at.to_rfc3339_opts(SecondsFormat::Millis, true),
            finished_at: Some(at.to_rfc3339_opts(SecondsFormat::Millis, true)),
            status_code: Some(200),
            account_id: Some(7),
            response_request_id: Some(request_id),
            outcome: "success".into(),
            error_stage: None,
            attempt_count: 1,
        }
    }

    fn attempt(request: &RequestDiagnostic) -> AttemptDiagnostic {
        AttemptDiagnostic {
            attempt_id: Uuid::new_v4().to_string(),
            request_id: request.request_id.clone(),
            ordinal: 1,
            account_id: 7,
            profile_version: "2.1.293".into(),
            profile_selection: "client_version".into(),
            parameters: DiagnosticParameters::default(),
            sessions: DiagnosticSessions::default(),
            started_at: request.started_at.clone(),
            finished_at: request.finished_at.clone(),
            ttft_ms: Some(10),
            duration_ms: Some(20),
            status_code: Some(200),
            upstream_request_id: Some("req_synthetic0".into()),
            response_model: None,
            response_message_hash: None,
            stop_reason: Some("end_turn".into()),
            tokens: DiagnosticTokens::default(),
            partial: false,
            outcome: "success".into(),
            error_stage: None,
            error_type: None,
            retry_reason: None,
        }
    }

    async fn storage_contract(dsn: &str) {
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(2)
            .connect(dsn)
            .await
            .unwrap();
        DiagnosticsStore::migrate(&pool).await.unwrap();
        DiagnosticsStore::migrate(&pool).await.unwrap();
        // 用未来一天作为裁剪参考，使后台真实时钟不会影响精确边界断言。
        let now = Utc::now() + ChronoDuration::days(1);
        let old = request(now - ChronoDuration::days(8) - ChronoDuration::seconds(1));
        let mut boundary = request(now - ChronoDuration::days(8));
        boundary.account_id = None;
        boundary.attempt_count = 0;
        let current = request(now);
        let attempted = attempt(&current);
        let event = DiagnosticEvent {
            request: current.clone(),
            attempt: Some(attempted.clone()),
        };
        // 真实入口先写全部可空字段为空的开始记录，再在同连接的缓存语句中写终态。
        let mut started = boundary.clone();
        started.finished_at = None;
        started.response_request_id = None;
        started.outcome = "in_progress".into();
        let mut started_attempt = attempted.clone();
        started_attempt.finished_at = None;
        started_attempt.upstream_request_id = None;
        started_attempt.outcome = "in_progress".into();
        started_attempt.partial = true;
        write_event(
            &pool,
            &DiagnosticEvent {
                request: started,
                attempt: None,
            },
        )
        .await
        .unwrap();
        let mut started_current = current.clone();
        started_current.finished_at = None;
        started_current.response_request_id = None;
        started_current.outcome = "in_progress".into();
        write_event(
            &pool,
            &DiagnosticEvent {
                request: started_current,
                attempt: Some(started_attempt),
            },
        )
        .await
        .unwrap();
        write_event(
            &pool,
            &DiagnosticEvent {
                request: old.clone(),
                attempt: Some(attempt(&old)),
            },
        )
        .await
        .unwrap();
        write_event(
            &pool,
            &DiagnosticEvent {
                request: boundary.clone(),
                attempt: None,
            },
        )
        .await
        .unwrap();
        write_event(&pool, &event).await.unwrap();
        write_event(&pool, &event).await.unwrap();
        let mut late = current.clone();
        late.finished_at = None;
        late.outcome = "in_progress".into();
        write_event(
            &pool,
            &DiagnosticEvent {
                request: late,
                attempt: None,
            },
        )
        .await
        .unwrap();
        prune(&pool, now).await.unwrap();
        let store = DiagnosticsStore::new(pool.clone());
        store.flush().await.unwrap();
        let by_id = store
            .list(DiagnosticQuery {
                correlation_id: Some("req_synthetic0".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(by_id.total, 1);
        assert_eq!(by_id.items[0].request_id, current.request_id);
        assert_eq!(by_id.items[0].outcome, "success");
        assert_eq!(
            store
                .list(DiagnosticQuery {
                    account_id: Some(7),
                    ..Default::default()
                })
                .await
                .unwrap()
                .total,
            1
        );
        assert_eq!(
            store
                .list(DiagnosticQuery {
                    since: Some(boundary.started_at.clone()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .total,
            2
        );
        let page = store
            .list(DiagnosticQuery {
                page: Some(2),
                page_size: Some(1),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(page.items[0].request_id, boundary.request_id);
        assert!(matches!(
            store.detail(&old.request_id).await,
            Err(AppError::NotFound)
        ));
        for query in [
            DiagnosticQuery {
                page: Some(0),
                ..Default::default()
            },
            DiagnosticQuery {
                page_size: Some(201),
                ..Default::default()
            },
            DiagnosticQuery {
                since: Some("错误时间".into()),
                ..Default::default()
            },
            DiagnosticQuery {
                outcome: Some("未知结果".into()),
                ..Default::default()
            },
        ] {
            assert!(matches!(
                store.list(query).await,
                Err(AppError::BadRequest(_))
            ));
        }
        drop(store);
        pool.close().await;
        let reader = AnyPoolOptions::new()
            .max_connections(1)
            .connect(dsn)
            .await
            .unwrap();
        let reopened = DiagnosticsStore::new(reader.clone());
        let detail = reopened.detail(&current.request_id).await.unwrap();
        assert_eq!(detail.attempts.len(), 1);
        assert_eq!(detail.attempts[0].attempt_id, attempted.attempt_id);
        drop(reopened);
        reader.close().await;
    }

    /// 验证 SQLite 独立连接、重启、重复终结、查询和八天保留。
    #[tokio::test]
    async fn sqlite_diagnostics_storage_contract() {
        let path = std::env::temp_dir().join(format!("cc2api-diagnostics-{}.db", Uuid::new_v4()));
        storage_contract(&format!("sqlite:{}?mode=rwc", path.display())).await;
        let _ = std::fs::remove_file(path);
    }

    /// 在独立临时 PostgreSQL 上执行完全相同的存储契约。
    #[tokio::test]
    #[ignore = "须提供 CC2API_DIAGNOSTICS_TEST_POSTGRES_DSN 指向独立临时数据库"]
    async fn postgres_diagnostics_storage_contract() {
        let dsn = std::env::var("CC2API_DIAGNOSTICS_TEST_POSTGRES_DSN").unwrap();
        storage_contract(&dsn).await;
    }

    /// 队列饱和必须立即返回，并显式报告丢弃和数据库故障。
    #[tokio::test]
    async fn diagnostics_overload_and_write_failures_are_observable() {
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        DiagnosticsStore::migrate(&pool).await.unwrap();
        let store = DiagnosticsStore::new(pool.clone());
        let root = request(Utc::now());
        for _ in 0..QUEUE_CAPACITY + 5 {
            store.submit(DiagnosticEvent {
                request: root.clone(),
                attempt: None,
            });
        }
        assert!(store.dropped.load(Ordering::Relaxed) >= 5);
        store.flush().await.unwrap();
        let page = store.list(DiagnosticQuery::default()).await.unwrap();
        assert!(page.health.dropped_records >= 5);
        sqlx::query("DROP TABLE request_diagnostic_attempts")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE request_diagnostics")
            .execute(&pool)
            .await
            .unwrap();
        store.submit(DiagnosticEvent {
            request: root,
            attempt: None,
        });
        store.flush().await.unwrap();
        assert!(store.failures.load(Ordering::Relaxed) > 0);
        assert!(store.list(DiagnosticQuery::default()).await.is_err());
    }
}
