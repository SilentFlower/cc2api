use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use chrono::Utc;
use futures_util::future::join_all;
use sqlx::{AnyPool, Row};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use claude_code_gateway::error::AppError;
use claude_code_gateway::model::account::{
    Account, AccountAuthType, AccountStatus, BillingMode, DEFAULT_UPSTREAM_SESSION_POOL_SIZE,
    DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY, DEFAULT_UPSTREAM_SESSION_TTL_MINUTES,
};
use claude_code_gateway::service::account::{
    AccountSelectionContext, AccountService, DEFAULT_REQUEST_SLOT_UNITS,
};
use claude_code_gateway::service::v3_scheduling::{
    RequestPhase, SchedulingCandidate, SchedulingConfig, V3Scheduler, now_ms, send_interval_ms,
};
use claude_code_gateway::store::account_store::AccountStore;
use claude_code_gateway::store::db;
use claude_code_gateway::store::memory::MemoryStore;
use claude_code_gateway::store::scheduling_store::{SchedulingStore, SendClaim};
use claude_code_gateway::store::settings_store::SettingsStore;

/// 隔离数据库、账号服务和调度器，测试不接触上游模型。
struct Fixture {
    path: Option<PathBuf>,
    pool: AnyPool,
    accounts: Arc<AccountService>,
    scheduler: Arc<V3Scheduler>,
    settings: Arc<SettingsStore>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Fixture {
    /// 保存合成账号的 RPM，并走正式服务的等待唤醒路径。
    /// @param account 当前合成账号，保留既有身份字段。
    /// @param rpm 新 RPM。
    /// @return 保存成功；PG 夹具只更新其最小账号行。
    async fn set_rpm(&self, account: &mut Account, rpm: i32) {
        account.rpm_limit = rpm;
        if self.path.is_none() {
            sqlx::query("UPDATE accounts SET rpm_limit=$2 WHERE id=$1")
                .bind(account.id)
                .bind(rpm)
                .execute(&self.pool)
                .await
                .unwrap();
            self.scheduler.account_updated(account.id);
        } else {
            self.accounts.update_account(account).await.unwrap();
        }
    }

    /// 创建仅供本测试的数据库。
    /// @param postgres 显式隔离 PostgreSQL DSN，None 创建临时 SQLite。
    /// @return 服务与存储夹具。
    async fn new(postgres: Option<&str>) -> Self {
        sqlx::any::install_default_drivers();
        let path = postgres
            .is_none()
            .then(|| std::env::temp_dir().join(format!("v3-scheduling-{}.db", Uuid::new_v4())));
        let driver = if postgres.is_some() {
            "postgres"
        } else {
            "sqlite"
        };
        let dsn = postgres
            .map(str::to_owned)
            .unwrap_or_else(|| path.as_ref().unwrap().to_string_lossy().into_owned());
        let pool = db::init_db(driver, &dsn).await.unwrap();
        db::migrate(&pool, driver).await.unwrap();
        let settings = Arc::new(SettingsStore::new_with_driver(pool.clone(), driver.into()));
        let scheduler = Arc::new(V3Scheduler::new(Arc::new(SchedulingStore::new(
            pool.clone(),
        ))));
        scheduler.reload(&settings).await.unwrap();
        let accounts = Arc::new(
            AccountService::new(
                Arc::new(AccountStore::new(pool.clone(), driver.into())),
                Arc::new(MemoryStore::new()),
                settings.clone(),
            )
            .with_scheduler(scheduler.clone()),
        );
        Self {
            path,
            pool,
            accounts,
            scheduler,
            settings,
        }
    }

    /// 创建纯合成的可调度账号。
    /// @param name 测试名称。
    /// @return 已入库账号。
    async fn account(&self, name: &str) -> Account {
        let mut account = Account {
            id: 0,
            name: name.into(),
            email: format!("{}@example.invalid", Uuid::new_v4()),
            status: AccountStatus::Active,
            auth_type: AccountAuthType::SetupToken,
            setup_token: "synthetic-setup-token".into(),
            access_token: String::new(),
            refresh_token: String::new(),
            expires_at: None,
            oauth_refreshed_at: None,
            auth_error: String::new(),
            proxy_url: String::new(),
            device_id: String::new(),
            canonical_env: serde_json::json!({}),
            canonical_prompt: serde_json::json!({}),
            canonical_process: serde_json::json!({}),
            request_backend: "legacy".into(),
            billing_mode: BillingMode::Strip,
            account_uuid: None,
            organization_uuid: None,
            subscription_type: None,
            rate_limit_tier: None,
            concurrency: 1,
            priority: 50,
            rpm_limit: 0,
            five_hour_usage_limit_percent: 0,
            seven_day_usage_limit_percent: 0,
            rate_limited_at: None,
            rate_limit_reset_at: None,
            disable_reason: String::new(),
            auto_telemetry: false,
            auto_poll_usage: false,
            allow_1m_models: "opus".into(),
            allow_fast_mode: false,
            upstream_session_pool_enabled: false,
            upstream_session_pool_size: DEFAULT_UPSTREAM_SESSION_POOL_SIZE,
            upstream_session_ttl_minutes: DEFAULT_UPSTREAM_SESSION_TTL_MINUTES,
            upstream_session_refresh_policy: DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY.into(),
            telemetry_count: 0,
            usage_data: serde_json::json!({}),
            usage_fetched_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        if self.path.is_none() {
            // PG 状态测试只需要外键账号，避免将历史账号写入的可空时间转换缺陷混入新状态合同。
            let rows = sqlx::query(
                "INSERT INTO accounts(name,email,token,device_id) VALUES($1,$2,$3,$4) RETURNING id",
            )
            .bind(&account.name)
            .bind(&account.email)
            .bind(&account.setup_token)
            .bind("synthetic-device")
            .fetch_all(&self.pool)
            .await
            .unwrap();
            account.id = rows[0].get("id");
        } else {
            self.accounts.create_account(&mut account).await.unwrap();
        }
        account
    }
}

/// 同一会话竞争只允许单一账号获胜，失败事务归还发送预算。
async fn atomic_binding_contract(f: &Fixture) {
    let a = f.account("A").await;
    let b = f.account("B").await;
    let key = hex::encode([1_u8; 32]);
    let now = now_ms();
    let operations = (0..24).map(|i| {
        let store = f.scheduler.store.clone();
        let key = key.clone();
        let id = if i % 2 == 0 { a.id } else { b.id };
        tokio::spawn(async move { (id, store.claim(id, 0, now, Some(&key), None).await.unwrap()) })
    });
    let results = join_all(operations).await;
    let winners = results
        .into_iter()
        .map(Result::unwrap)
        .filter_map(|(id, result)| matches!(result, SendClaim::Acquired(_)).then_some(id))
        .collect::<HashSet<_>>();
    assert_eq!(winners.len(), 1);
    let bound = f.scheduler.store.binding(&key, now).await.unwrap().unwrap();
    assert_eq!(bound.created_at_ms, now);
    assert_eq!(bound.last_seen_at_ms, now);
    assert_eq!(bound.expires_at_ms, now + 3_600_000);
    assert!(winners.contains(&bound.account_id));
    let loser = if bound.account_id == a.id { b.id } else { a.id };
    assert_eq!(f.scheduler.store.next_send(loser, 1200).await.unwrap(), 0);
    assert!(matches!(
        f.scheduler
            .store
            .claim(loser, 1200, now, None, None)
            .await
            .unwrap(),
        SendClaim::Acquired(None)
    ));
    // 只有读到原版本的既有硬资格迁移才可换号；旧请求不能覆盖新绑定。
    assert!(matches!(
        f.scheduler
            .store
            .claim(loser, 0, now + 1, Some(&key), Some(&bound))
            .await
            .unwrap(),
        SendClaim::Acquired(_)
    ));
    assert!(matches!(
        f.scheduler
            .store
            .claim(bound.account_id, 0, now + 2, Some(&key), Some(&bound))
            .await
            .unwrap(),
        SendClaim::BindingConflict
    ));
    let current = f
        .scheduler
        .store
        .binding(&key, now + 3)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !f.scheduler
            .store
            .renew(&bound, now + 3, false)
            .await
            .unwrap()
    );
    assert!(
        f.scheduler
            .store
            .renew(&current, now + 4, false)
            .await
            .unwrap()
    );
    let counts = f
        .scheduler
        .store
        .counts(&[a.id, b.id], now + 5)
        .await
        .unwrap();
    assert_eq!(counts.get(&loser), Some(&1));
    let (rows, total) = f
        .scheduler
        .store
        .sessions(loser, now + 5, 1, 20, &key[..4], &[key.clone()], Some(true))
        .await
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows[0].session_key, key);
    SchedulingStore::migrate(&f.pool).await.unwrap();
    assert!(
        f.scheduler
            .store
            .binding(&key, now + 5)
            .await
            .unwrap()
            .is_some()
    );
    f.scheduler
        .store
        .renew_active(&[current], now + 5)
        .await
        .unwrap();
    f.scheduler.store.cleanup(now + 4_000_000).await.unwrap();
    assert!(
        f.scheduler
            .store
            .binding(&key, now + 4_000_000)
            .await
            .unwrap()
            .is_none()
    );
    f.accounts.delete_account(loser).await.unwrap();
    assert_eq!(f.scheduler.store.next_send(loser, 1200).await.unwrap(), 0);
}

#[tokio::test]
async fn sqlite_concurrent_binding_and_budget_rollback() {
    let f = Fixture::new(None).await;
    atomic_binding_contract(&f).await;
}

#[tokio::test]
#[ignore = "需要 CC2API_SCHEDULING_TEST_POSTGRES_DSN 指向本任务独立临时数据库"]
async fn postgres_concurrent_binding_and_budget_rollback() {
    let dsn = std::env::var("CC2API_SCHEDULING_TEST_POSTGRES_DSN").expect("必须提供隔离测试库");
    let f = Fixture::new(Some(&dsn)).await;
    atomic_binding_contract(&f).await;
    pacing_contract(&f).await;
    current_rpm_contract(&f).await;
}

/// 显式时间覆盖分钟边界、重启、RPM 变化和闲置后的禁止补发。
async fn pacing_contract(f: &Fixture) {
    let account = f.account("pacing").await;
    let store = &f.scheduler.store;
    assert_eq!(send_interval_ms(50), 1200);
    assert_eq!(send_interval_ms(0), 0);
    assert!(matches!(
        store
            .claim(account.id, 1200, 59900, None, None)
            .await
            .unwrap(),
        SendClaim::Acquired(_)
    ));
    assert!(matches!(
        store
            .claim(account.id, 1200, 60000, None, None)
            .await
            .unwrap(),
        SendClaim::WaitUntil(61100)
    ));
    let restarted = SchedulingStore::new(f.pool.clone());
    assert_eq!(restarted.next_send(account.id, 1200).await.unwrap(), 61100);
    assert!(matches!(
        restarted
            .claim(account.id, 1200, 61100, None, None)
            .await
            .unwrap(),
        SendClaim::Acquired(_)
    ));
    assert!(matches!(
        restarted
            .claim(account.id, 12000, 62000, None, None)
            .await
            .unwrap(),
        SendClaim::WaitUntil(73100)
    ));
    assert!(matches!(
        restarted
            .claim(account.id, 1200, 3600000, None, None)
            .await
            .unwrap(),
        SendClaim::Acquired(_)
    ));
    assert!(matches!(
        restarted
            .claim(account.id, 1200, 3600001, None, None)
            .await
            .unwrap(),
        SendClaim::WaitUntil(3601200)
    ));
}

#[tokio::test]
async fn sqlite_smooth_pacing_survives_boundaries_and_restart() {
    let f = Fixture::new(None).await;
    pacing_contract(&f).await;
}

#[tokio::test]
async fn busy_bound_session_stays_and_new_session_uses_free_account() {
    let f = Fixture::new(None).await;
    let a = f.account("busy").await;
    let b = f.account("free").await;
    let seed = f.scheduler.begin(1, Some("stable-session"));
    seed.selected(&a, None);
    drop(seed.admit(&a).await.unwrap());
    drop(seed);
    let queue = f.accounts.get_or_create_queue(a.id, a.concurrency).await;
    let held = queue
        .acquire(Duration::from_secs(1), DEFAULT_REQUEST_SLOT_UNITS)
        .await
        .unwrap();
    let old = f.scheduler.begin(1, Some("stable-session"));
    let selected = f
        .accounts
        .select_v3_account(
            &old,
            &[],
            &[],
            &AccountSelectionContext::disabled(),
            DEFAULT_REQUEST_SLOT_UNITS,
        )
        .await
        .unwrap();
    assert!(selected.sticky);
    assert_eq!(selected.account.id, a.id);
    assert!(
        queue
            .acquire(Duration::from_millis(10), DEFAULT_REQUEST_SLOT_UNITS)
            .await
            .is_err()
    );
    assert_eq!(
        f.scheduler
            .store
            .binding(old.key().unwrap(), now_ms())
            .await
            .unwrap()
            .unwrap()
            .account_id,
        a.id
    );
    let fresh = f.scheduler.begin(1, Some("new-session"));
    let selected = f
        .accounts
        .select_v3_account(
            &fresh,
            &[],
            &[],
            &AccountSelectionContext::disabled(),
            DEFAULT_REQUEST_SLOT_UNITS,
        )
        .await
        .unwrap();
    assert_eq!(selected.account.id, b.id);
    assert!(
        f.scheduler
            .store
            .binding(fresh.key().unwrap(), now_ms())
            .await
            .unwrap()
            .is_none()
    );
    drop(held);
}

#[tokio::test]
async fn tokens_are_isolated_and_sessionless_requests_have_no_binding() {
    let f = Fixture::new(None).await;
    let a = f.scheduler.begin(1, Some("shared"));
    let same = f.scheduler.begin(1, Some("shared"));
    let other = f.scheduler.begin(2, Some("shared"));
    let none = f.scheduler.begin(1, None);
    assert_eq!(a.key(), same.key());
    assert_ne!(a.key(), other.key());
    assert!(none.key().is_none());
    assert_eq!(a.key().unwrap().len(), 64);
    assert!(!a.key().unwrap().contains("shared"));
}

#[tokio::test]
async fn near_optimal_new_sessions_rotate_without_rebinding_old_sessions() {
    let f = Fixture::new(None).await;
    let a = f.account("A").await;
    let b = f.account("B").await;
    let mut chosen = Vec::new();
    for _ in 0..8 {
        let request = f.scheduler.begin(1, None);
        chosen.push(
            f.accounts
                .select_v3_account(&request, &[], &[], &AccountSelectionContext::disabled(), 2)
                .await
                .unwrap()
                .account
                .id,
        );
    }
    assert_eq!(chosen.iter().filter(|id| **id == a.id).count(), 4);
    assert_eq!(chosen.iter().filter(|id| **id == b.id).count(), 4);
}

#[tokio::test]
async fn observation_does_not_renew_and_one_session_can_have_two_requests() {
    let f = Fixture::new(None).await;
    let a = f.account("A").await;
    let r1 = f.scheduler.begin(1, Some("one-session"));
    r1.selected(&a, None);
    r1.admit(&a).await.unwrap().submitted();
    let r2 = f.scheduler.begin(1, Some("one-session"));
    f.accounts
        .select_v3_account(&r2, &[], &[], &AccountSelectionContext::disabled(), 2)
        .await
        .unwrap();
    r2.phase(RequestPhase::WaitingSend);
    let before = f
        .scheduler
        .store
        .binding(r1.key().unwrap(), now_ms())
        .await
        .unwrap()
        .unwrap();
    let snapshot = f.scheduler.snapshot(a.id);
    assert_eq!(snapshot.requests.len(), 2);
    assert_eq!(f.scheduler.active_keys(a.id).len(), 1);
    assert_eq!(snapshot.events.len(), 1);
    let (rows, total) = f
        .scheduler
        .store
        .sessions(
            a.id,
            now_ms(),
            1,
            20,
            "",
            &f.scheduler.active_keys(a.id),
            Some(true),
        )
        .await
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(rows.len(), 1);
    let after = f
        .scheduler
        .store
        .binding(r1.key().unwrap(), now_ms())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.expires_at_ms, after.expires_at_ms);
    assert_eq!(before.last_seen_at_ms, after.last_seen_at_ms);
    drop(r1);
    drop(r2);
    assert!(f.scheduler.snapshot(a.id).requests.is_empty());
}

#[tokio::test]
async fn cancelling_pacing_wait_releases_fifo_and_current_state() {
    let f = Fixture::new(None).await;
    let mut a = f.account("slow").await;
    f.set_rpm(&mut a, 1).await;
    f.scheduler
        .store
        .claim(a.id, 60000, now_ms(), None, None)
        .await
        .unwrap();
    let request = f.scheduler.begin(1, None);
    request.selected(&a, None);
    let task = tokio::spawn({
        let request = request.clone();
        let a = a.clone();
        async move { request.admit(&a).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while f
            .scheduler
            .snapshot(a.id)
            .requests
            .iter()
            .all(|r| r.phase != RequestPhase::WaitingSend)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    drop(request);
    assert!(f.scheduler.snapshot(a.id).requests.is_empty());
    assert!(f.scheduler.snapshot(a.id).events.is_empty());
}

#[tokio::test]
async fn storage_failure_stops_admission_without_fake_submission() {
    let f = Fixture::new(None).await;
    let a = f.account("A").await;
    let request = f.scheduler.begin(1, None);
    request.selected(&a, None);
    f.pool.close().await;
    assert!(matches!(
        request.admit(&a).await,
        Err(AppError::ServiceUnavailable(_))
    ));
    assert!(f.scheduler.snapshot(a.id).events.is_empty());
}

#[test]
fn scheduling_settings_validate_the_merged_snapshot() {
    assert_eq!(
        SchedulingConfig::parse(&HashMap::new()).unwrap().mode,
        "smart"
    );
    for value in ["NaN", "inf", "-1", "10.1", "0.11"] {
        assert!(
            SchedulingConfig::parse(&HashMap::from([(
                "v3_smart_weight_load".into(),
                value.into()
            )]))
            .is_err()
        );
    }
    let zero = ["load", "quota", "health", "latency"]
        .map(|name| (format!("v3_smart_weight_{name}"), "0".into()))
        .into_iter()
        .collect();
    assert!(SchedulingConfig::parse(&zero).is_err());
}

#[tokio::test]
async fn one_deadline_survives_config_reload_and_timeout_keeps_binding() {
    let f = Fixture::new(None).await;
    let mut a = f.account("A").await;
    let seed = f.scheduler.begin(1, Some("long-lived"));
    seed.selected(&a, None);
    drop(seed.admit(&a).await.unwrap());
    drop(seed);
    let old = f.scheduler.begin(1, None);
    f.settings
        .upsert_many(&HashMap::from([(
            "v3_admission_wait_seconds".into(),
            "1".into(),
        )]))
        .await
        .unwrap();
    f.scheduler.reload(&f.settings).await.unwrap();
    let request = f.scheduler.begin(1, Some("long-lived"));
    let binding = f
        .scheduler
        .store
        .binding(request.key().unwrap(), now_ms())
        .await
        .unwrap();
    f.set_rpm(&mut a, 1).await;
    request.selected(&a, binding);
    assert!(matches!(
        request.admit(&a).await,
        Err(AppError::AdmissionLimited { .. })
    ));
    assert!(old.remaining().unwrap() > Duration::from_secs(110));
    assert_eq!(
        f.scheduler
            .store
            .binding(request.key().unwrap(), now_ms())
            .await
            .unwrap()
            .unwrap()
            .account_id,
        a.id
    );
    assert!(f.scheduler.snapshot(a.id).events.is_empty());
}

#[tokio::test]
async fn active_binding_survives_expiry_cleanup_and_recovers_by_cas() {
    let f = Fixture::new(None).await;
    let a = f.account("A").await;
    let _b = f.account("B").await;
    let active = f.scheduler.begin(1, Some("active"));
    active.selected(&a, None);
    drop(active.admit(&a).await.unwrap());
    sqlx::query("UPDATE gateway_session_bindings SET expires_at_ms=0 WHERE session_key=$1")
        .bind(active.key().unwrap())
        .execute(&f.pool)
        .await
        .unwrap();
    f.scheduler.cleanup_expired().await.unwrap();
    let follow = f.scheduler.begin(1, Some("active"));
    let selected = f
        .accounts
        .select_v3_account(&follow, &[], &[], &AccountSelectionContext::disabled(), 2)
        .await
        .unwrap();
    assert!(selected.sticky);
    assert_eq!(selected.account.id, a.id);
}

#[tokio::test]
async fn real_failures_reduce_health_and_user_errors_do_not_reward_latency() {
    let f = Fixture::new(None).await;
    let a = f.account("A").await;
    let b = f.account("B").await;
    let snapshot = |account: Account| SchedulingCandidate {
        account,
        active_units: 0,
        max_units: 2,
        queued: 0,
        queued_units: 0,
        legacy_score: 0.0,
        next_send_ms: 0,
    };
    let request = f.scheduler.begin(1, None);
    request.selected(&a, None);
    request.submitted(a.id);
    request.response_headers(a.id, 400);
    let before = f
        .scheduler
        .factors(&snapshot(a.clone()), &f.scheduler.config(), now_ms());
    assert_eq!(before.health, 0.5);
    assert_eq!(before.latency, 0.5);
    request.submitted(a.id);
    request.response_headers(a.id, 503);
    let failed = f
        .scheduler
        .factors(&snapshot(a), &f.scheduler.config(), now_ms());
    let unknown = f
        .scheduler
        .factors(&snapshot(b), &f.scheduler.config(), now_ms());
    assert!(failed.health < unknown.health);
    assert_eq!(failed.latency, 0.5);
    assert!(failed.score < unknown.score);
}

#[tokio::test]
async fn hard_migration_updates_active_binding_counts_without_moving_old_execution() {
    let f = Fixture::new(None).await;
    let a = f.account("A").await;
    let b = f.account("B").await;
    let old = f.scheduler.begin(1, Some("migrating"));
    old.selected(&a, None);
    old.admit(&a).await.unwrap().submitted();
    let original = f
        .scheduler
        .store
        .binding(old.key().unwrap(), now_ms())
        .await
        .unwrap()
        .unwrap();
    let moved = f.scheduler.begin(1, Some("migrating"));
    moved.selected(&b, Some(original));
    drop(moved.admit(&b).await.unwrap());
    assert!(f.scheduler.active_keys(a.id).is_empty());
    assert_eq!(
        f.scheduler.snapshot(a.id).requests[0].phase,
        RequestPhase::Running
    );
    assert_eq!(f.scheduler.active_keys(b.id).len(), 1);
    let current = f
        .scheduler
        .store
        .binding(old.key().unwrap(), now_ms())
        .await
        .unwrap()
        .unwrap();
    let returned = f.scheduler.begin(1, Some("migrating"));
    returned.selected(&a, Some(current));
    drop(returned.admit(&a).await.unwrap());
    drop(returned);
    drop(moved);
    sqlx::query("UPDATE gateway_session_bindings SET expires_at_ms=0 WHERE session_key=$1")
        .bind(old.key().unwrap())
        .execute(&f.pool)
        .await
        .unwrap();
    f.scheduler.cleanup_expired().await.unwrap();
    assert_eq!(
        f.scheduler
            .store
            .binding(old.key().unwrap(), now_ms())
            .await
            .unwrap()
            .unwrap()
            .account_id,
        a.id
    );
    assert_eq!(f.scheduler.active_keys(a.id).len(), 1);
}

/// 写锁等待和持有许可期间的准备耗时均不能抵扣后续提交间隔。
#[tokio::test]
async fn sqlite_write_lock_and_held_permit_cannot_bank_send_time() {
    let f = Fixture::new(None).await;
    let mut account = f.account("write-lock").await;
    f.set_rpm(&mut account, 300).await;
    let mut writer = f.pool.acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *writer)
        .await
        .unwrap();
    let first = f.scheduler.begin(1, None);
    first.selected(&account, None);
    let task = tokio::spawn({
        let account = account.clone();
        async move { first.admit(&account).await.unwrap() }
    });
    sleep(Duration::from_millis(350)).await;
    assert!(!task.is_finished(), "写锁未释放时不能认领发送许可");
    let unlocked_at = now_ms();
    sqlx::query("COMMIT").execute(&mut *writer).await.unwrap();
    let permit = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    let persisted_next = f.scheduler.store.next_send(account.id, 200).await.unwrap();
    assert!(
        persisted_next >= unlocked_at + 200,
        "持久时间={persisted_next}，解锁时间={unlocked_at}"
    );
    assert!(f.scheduler.snapshot(account.id).events.is_empty());

    let next = f.scheduler.begin(1, None);
    next.selected(&account, None);
    let task = tokio::spawn({
        let account = account.clone();
        async move {
            next.admit(&account).await.unwrap().submitted();
            StdInstant::now()
        }
    });
    sleep(Duration::from_millis(250)).await;
    assert!(
        !task.is_finished(),
        "前一个请求尚未提交时，后一个请求不能抢跑"
    );
    let submitted_at = StdInstant::now();
    permit.submitted();
    let next_at = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(next_at.duration_since(submitted_at) >= Duration::from_millis(200));
    assert_eq!(f.scheduler.snapshot(account.id).events.len(), 2);
}

/// 真实账号配置变化必须影响队首，并保持原准入期限。
/// @param f SQLite 或隔离 PostgreSQL 夹具。
/// @return 两次配置更新后的行为均符合当前 RPM。
async fn current_rpm_contract(f: &Fixture) {
    let mut account = f.account("hot-rpm").await;
    f.set_rpm(&mut account, 300).await;
    let first = f.scheduler.begin(1, None);
    first.selected(&account, None);
    first.admit(&account).await.unwrap().submitted();
    let first_at = StdInstant::now();
    let waiting = f.scheduler.begin(1, None);
    let deadline = waiting.deadline();
    waiting.selected(&account, None);
    let task = tokio::spawn({
        let request = waiting.clone();
        let old_account = account.clone();
        async move {
            request.admit(&old_account).await.unwrap().submitted();
            StdInstant::now()
        }
    });
    sleep(Duration::from_millis(40)).await;
    f.set_rpm(&mut account, 60).await;
    let second_at = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(second_at.duration_since(first_at) >= Duration::from_millis(990));
    assert_eq!(waiting.deadline(), deadline);

    f.set_rpm(&mut account, 1).await;
    let waiting = f.scheduler.begin(1, None);
    let deadline = waiting.deadline();
    waiting.selected(&account, None);
    let task = tokio::spawn({
        let request = waiting.clone();
        let old_account = account.clone();
        async move {
            request.admit(&old_account).await.unwrap().submitted();
            StdInstant::now()
        }
    });
    sleep(Duration::from_millis(40)).await;
    assert!(!task.is_finished());
    f.set_rpm(&mut account, 300).await;
    let third_at = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(third_at.duration_since(second_at) >= Duration::from_millis(190));
    assert_eq!(waiting.deadline(), deadline);
    assert_eq!(f.scheduler.snapshot(account.id).events.len(), 3);
}

/// SQLite 上通过正式账号服务验证调低和调高 RPM 都会立即重新裁决。
#[tokio::test]
async fn waiting_requests_reload_current_rpm_without_resetting_deadline() {
    let f = Fixture::new(None).await;
    current_rpm_contract(&f).await;
}

/// 容量文案必须解释前方请求及等待预算，不能在不可准入时返回 ready。
#[tokio::test]
async fn capacity_reason_matches_pending_work_and_wait_budget() {
    let f = Fixture::new(None).await;
    let mut account = f.account("capacity").await;
    account.concurrency = 4;
    account.rpm_limit = 1;
    let request = f.scheduler.begin(1, None);
    request.selected(&account, None);
    request.phase(RequestPhase::Preparing);
    let candidate = SchedulingCandidate {
        account,
        active_units: 2,
        max_units: 8,
        queued: 0,
        queued_units: 0,
        legacy_score: 0.0,
        next_send_ms: 0,
    };
    let view = f.scheduler.capacity(&candidate, 2, 5000, now_ms());
    assert!(!view.immediate && !view.can_queue);
    assert_eq!(view.reason, "wait_budget");
    let view = f.scheduler.capacity(&candidate, 2, 120000, now_ms());
    assert!(!view.immediate && view.can_queue);
    assert_eq!(view.reason, "pending_submissions");
    drop(request);
    let view = f.scheduler.capacity(&candidate, 2, 5000, now_ms());
    assert!(view.immediate && view.can_queue);
    assert_eq!(view.reason, "ready");
}
