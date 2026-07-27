use chrono::{Duration, Utc};
use sqlx::AnyPool;
use std::sync::Arc;

use claude_code_gateway::model::account::{
    Account, AccountAuthType, AccountStatus, BillingMode, DEFAULT_UPSTREAM_SESSION_POOL_SIZE,
    DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY, DEFAULT_UPSTREAM_SESSION_TTL_MINUTES,
};
use claude_code_gateway::store::account_store::AccountStore;

async fn setup() -> Arc<AccountStore> {
    sqlx::any::install_default_drivers();
    let tmp = std::env::temp_dir().join(format!("ccgw_ts_test_{}.db", rand::random::<u64>()));
    let dsn = format!("sqlite:{}?mode=rwc", tmp.display());
    let pool = AnyPool::connect(&dsn)
        .await
        .expect("failed to create sqlite pool");
    claude_code_gateway::store::db::migrate(&pool, "sqlite")
        .await
        .expect("failed to run migrations");
    Arc::new(AccountStore::new(pool, "sqlite".into()))
}

fn new_account(email: &str) -> Account {
    Account {
        id: 0,
        name: "ts-test".into(),
        email: email.into(),
        status: AccountStatus::Active,
        auth_type: AccountAuthType::Oauth,
        setup_token: "sk-ant-oat01-test-placeholder".into(),
        access_token: "access-tok".into(),
        refresh_token: "refresh-tok".into(),
        expires_at: Some(Utc::now() + Duration::hours(1)),
        oauth_refreshed_at: Some(Utc::now()),
        auth_error: String::new(),
        proxy_url: String::new(),
        device_id: String::new(),
        canonical_env: serde_json::json!({}),
        canonical_prompt: serde_json::json!({}),
        canonical_process: serde_json::json!({}),
        billing_mode: BillingMode::Strip,
        account_uuid: None,
        organization_uuid: None,
        subscription_type: None,
        concurrency: 3,
        priority: 50,
        rpm_limit: 0,
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
    }
}

// ─── CREATE: oauth_expires_at and oauth_refreshed_at bound as timestamps ───

#[tokio::test]
async fn test_create_account_with_oauth_timestamps() {
    let store = setup().await;
    let mut a = new_account("create-ts@example.com");
    store.create(&mut a).await.expect("create failed");
    assert!(a.id > 0);

    let fetched = store.get_by_id(a.id).await.expect("get_by_id failed");
    assert!(fetched.expires_at.is_some());
    assert!(fetched.oauth_refreshed_at.is_some());
}

#[tokio::test]
async fn test_create_account_with_null_timestamps() {
    let store = setup().await;
    let mut a = new_account("create-null-ts@example.com");
    a.expires_at = None;
    a.oauth_refreshed_at = None;
    store.create(&mut a).await.expect("create failed");

    let fetched = store.get_by_id(a.id).await.expect("get_by_id failed");
    assert!(fetched.expires_at.is_none());
    assert!(fetched.oauth_refreshed_at.is_none());
}

#[tokio::test]
async fn test_account_upstream_session_pool_fields_round_trip() {
    let store = setup().await;
    let mut a = new_account("upstream-session-pool@example.com");
    a.upstream_session_pool_enabled = true;
    a.upstream_session_pool_size = 4;
    a.upstream_session_ttl_minutes = 120;
    a.upstream_session_refresh_policy = "owner_only".into();
    store.create(&mut a).await.expect("create failed");

    let mut fetched = store.get_by_id(a.id).await.expect("get_by_id failed");
    assert!(fetched.upstream_session_pool_enabled);
    assert_eq!(fetched.upstream_session_pool_size, 4);
    assert_eq!(fetched.upstream_session_ttl_minutes, 120);
    assert_eq!(fetched.upstream_session_refresh_policy, "owner_only");

    fetched.upstream_session_pool_enabled = false;
    fetched.upstream_session_pool_size = 0;
    fetched.upstream_session_refresh_policy = "mapped_request".into();
    store.update(&fetched).await.expect("update failed");
    let updated = store.get_by_id(a.id).await.expect("get updated failed");
    assert!(!updated.upstream_session_pool_enabled);
    assert_eq!(updated.upstream_session_pool_size, 0);
    assert_eq!(updated.upstream_session_refresh_policy, "mapped_request");
}

#[tokio::test]
async fn test_account_fast_mode_defaults_to_disabled_and_round_trips() {
    let store = setup().await;
    let mut a = new_account("fast-mode@example.com");
    store.create(&mut a).await.expect("create failed");

    let mut fetched = store.get_by_id(a.id).await.expect("get_by_id failed");
    assert!(!fetched.allow_fast_mode);

    fetched.allow_fast_mode = true;
    store.update(&fetched).await.expect("update failed");
    let updated = store.get_by_id(a.id).await.expect("get updated failed");
    assert!(updated.allow_fast_mode);
}

// ─── UPDATE: oauth_expires_at and oauth_refreshed_at ───

#[tokio::test]
async fn test_update_account_with_oauth_timestamps() {
    let store = setup().await;
    let mut a = new_account("update-ts@example.com");
    store.create(&mut a).await.unwrap();

    let new_expiry = Utc::now() + Duration::hours(2);
    let new_refreshed = Utc::now();
    a.expires_at = Some(new_expiry);
    a.oauth_refreshed_at = Some(new_refreshed);
    store.update(&a).await.expect("update failed");

    let fetched = store.get_by_id(a.id).await.unwrap();
    assert!(fetched.expires_at.is_some());
    assert!(fetched.oauth_refreshed_at.is_some());
}

// ─── UPDATE_OAUTH_TOKENS: oauth_expires_at and oauth_refreshed_at ───

#[tokio::test]
async fn test_update_oauth_tokens_timestamps() {
    let store = setup().await;
    let mut a = new_account("oauth-tok@example.com");
    store.create(&mut a).await.unwrap();

    let new_expiry = Utc::now() + Duration::hours(3);
    store
        .update_oauth_tokens(a.id, "new-access", "new-refresh", new_expiry)
        .await
        .expect("update_oauth_tokens failed");

    let fetched = store.get_by_id(a.id).await.unwrap();
    assert_eq!(fetched.access_token, "new-access");
    assert_eq!(fetched.refresh_token, "new-refresh");
    assert!(fetched.expires_at.is_some());
    assert!(fetched.oauth_refreshed_at.is_some());
    assert!(fetched.auth_error.is_empty());
}

// ─── SET_RATE_LIMIT: rate_limited_at and rate_limit_reset_at ───

#[tokio::test]
async fn test_set_rate_limit_timestamps() {
    let store = setup().await;
    let mut a = new_account("rate-limit@example.com");
    store.create(&mut a).await.unwrap();

    let reset_at = Utc::now() + Duration::hours(5);
    store
        .set_rate_limit(a.id, reset_at)
        .await
        .expect("set_rate_limit failed");

    let fetched = store.get_by_id(a.id).await.unwrap();
    assert!(fetched.rate_limited_at.is_some());
    assert!(fetched.rate_limit_reset_at.is_some());
}

// ─── DISABLE_ACCOUNT: rate_limited_at and rate_limit_reset_at ───

#[tokio::test]
async fn test_disable_account_with_rate_limit_timestamps() {
    let store = setup().await;
    let mut a = new_account("disable-ts@example.com");
    store.create(&mut a).await.unwrap();

    let reset_at = Utc::now() + Duration::hours(1);
    store
        .disable_account(a.id, AccountStatus::Active, "rate limited", Some(reset_at))
        .await
        .expect("disable_account failed");

    let fetched = store.get_by_id(a.id).await.unwrap();
    assert!(fetched.rate_limited_at.is_some());
    assert!(fetched.rate_limit_reset_at.is_some());
    assert_eq!(fetched.disable_reason, "rate limited");
}

#[tokio::test]
async fn test_disable_account_without_rate_limit() {
    let store = setup().await;
    let mut a = new_account("disable-no-rl@example.com");
    store.create(&mut a).await.unwrap();

    store
        .disable_account(a.id, AccountStatus::Disabled, "manual", None)
        .await
        .expect("disable_account failed");

    let fetched = store.get_by_id(a.id).await.unwrap();
    assert!(fetched.rate_limited_at.is_none());
    assert!(fetched.rate_limit_reset_at.is_none());
    assert_eq!(fetched.status, AccountStatus::Disabled);
}

#[tokio::test]
async fn test_disable_for_auth_failure_updates_reason_and_clears_rate_limit() {
    let store = setup().await;
    let mut account = new_account("auth-failure@example.com");
    let usage_data = serde_json::json!({
        "seven_day": {
            "utilization": 42,
            "resets_at": (Utc::now() + Duration::days(3)).to_rfc3339()
        }
    });
    store.create(&mut account).await.unwrap();
    store
        .update_usage(account.id, &serde_json::to_string(&usage_data).unwrap())
        .await
        .unwrap();
    store
        .set_rate_limit(account.id, Utc::now() + Duration::hours(5))
        .await
        .unwrap();

    store
        .disable_for_auth_failure(account.id, "401 认证失败")
        .await
        .unwrap();

    let fetched = store.get_by_id(account.id).await.unwrap();
    assert_eq!(fetched.status, AccountStatus::Disabled);
    assert_eq!(fetched.auth_error, "401 认证失败");
    assert_eq!(fetched.disable_reason, "401 认证失败");
    assert!(fetched.rate_limited_at.is_none());
    assert!(fetched.rate_limit_reset_at.is_none());
    assert_eq!(fetched.usage_data, usage_data);
}

// ─── UPDATE_USAGE: usage_fetched_at ───

#[tokio::test]
async fn test_update_usage_timestamp() {
    let store = setup().await;
    let mut a = new_account("usage-ts@example.com");
    store.create(&mut a).await.unwrap();

    store
        .update_usage(a.id, r#"{"tokens": 1000}"#)
        .await
        .expect("update_usage failed");

    let fetched = store.get_by_id(a.id).await.unwrap();
    assert!(fetched.usage_fetched_at.is_some());
}

// ─── LIST_SCHEDULABLE: rate_limit_reset_at comparison ───

#[tokio::test]
async fn test_list_schedulable_excludes_rate_limited() {
    let store = setup().await;
    let mut a = new_account("sched-limited@example.com");
    store.create(&mut a).await.unwrap();

    // Set a future rate limit
    let reset_at = Utc::now() + Duration::hours(1);
    store.set_rate_limit(a.id, reset_at).await.unwrap();

    let schedulable = store.list_schedulable().await.unwrap();
    assert!(
        schedulable.iter().all(|acc| acc.id != a.id),
        "rate-limited account should not be schedulable"
    );
}

#[tokio::test]
async fn test_list_schedulable_includes_expired_rate_limit() {
    let store = setup().await;
    let mut a = new_account("sched-expired@example.com");
    store.create(&mut a).await.unwrap();

    // Set an already-expired rate limit
    let reset_at = Utc::now() - Duration::seconds(10);
    store.set_rate_limit(a.id, reset_at).await.unwrap();

    let schedulable = store.list_schedulable().await.unwrap();
    assert!(
        schedulable.iter().any(|acc| acc.id == a.id),
        "account with expired rate limit should be schedulable"
    );
}
