use chrono::{Duration, Timelike, Utc};
use serde_json::json;
use sqlx::AnyPool;
use std::sync::Arc;

use claude_code_gateway::error::AppError;
use claude_code_gateway::model::account::{
    Account, AccountAuthType, AccountStatus, DEFAULT_UPSTREAM_SESSION_POOL_SIZE,
    DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY, DEFAULT_UPSTREAM_SESSION_TTL_MINUTES,
};
use claude_code_gateway::service::account::{
    AccountSelectionContext, AccountService, UsageObservationKind,
};
use claude_code_gateway::store::account_store::AccountStore;
use claude_code_gateway::store::memory::MemoryStore;
use claude_code_gateway::store::settings_store::SettingsStore;

/// 创建测试用的临时文件数据库和服务
async fn setup() -> (Arc<AccountStore>, Arc<AccountService>) {
    sqlx::any::install_default_drivers();
    // SQLite in-memory 对 sqlx Any 不兼容，使用临时文件
    let tmp = std::env::temp_dir().join(format!("ccgw_test_{}.db", rand::random::<u64>()));
    let dsn = format!("sqlite:{}?mode=rwc", tmp.display());
    let pool = AnyPool::connect(&dsn)
        .await
        .expect("failed to create sqlite pool");
    claude_code_gateway::store::db::migrate(&pool, "sqlite")
        .await
        .expect("failed to run migrations");

    let store = Arc::new(AccountStore::new(pool.clone(), "sqlite".into()));
    let cache = Arc::new(MemoryStore::new());
    let settings_store = Arc::new(SettingsStore::new(pool));
    let svc = Arc::new(AccountService::new(store.clone(), cache, settings_store));
    (store, svc)
}

/// 创建一个基础测试账号
async fn create_test_account(svc: &AccountService, email: &str) -> Account {
    let mut account = Account {
        id: 0,
        name: "test".into(),
        email: email.into(),
        status: AccountStatus::Active,
        auth_type: claude_code_gateway::model::account::AccountAuthType::SetupToken,
        setup_token: "sk-ant-oat01-test-token-placeholder-value".into(),
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
        billing_mode: claude_code_gateway::model::account::BillingMode::Strip,
        account_uuid: None,
        organization_uuid: None,
        subscription_type: None,
        concurrency: 3,
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
    svc.create_account(&mut account)
        .await
        .expect("failed to create account");
    account
}

async fn mark_oauth_account(svc: &AccountService, account: &mut Account) {
    account.auth_type = AccountAuthType::Oauth;
    account.setup_token.clear();
    account.access_token = "access-token".into();
    account.refresh_token = "refresh-token".into();
    account.expires_at = Some(Utc::now() + Duration::hours(1));
    svc.update_account(account)
        .await
        .expect("failed to mark account as OAuth");
}

/// 避开 RPM 分钟窗口末尾，防止粘性等待测试在等待期间跨入下一分钟。
async fn wait_for_stable_rpm_window() {
    let second = Utc::now().second();
    if second >= 52 {
        tokio::time::sleep(std::time::Duration::from_secs((61 - second) as u64)).await;
    }
}

fn fable_selection_context(enabled: bool, model: &str) -> AccountSelectionContext {
    fable_selection_context_with_limit(enabled, model, 50)
}

fn fable_selection_context_with_limit(
    enabled: bool,
    model: &str,
    limit_percent: u32,
) -> AccountSelectionContext {
    AccountSelectionContext {
        usage_limits_enabled: false,
        fable_quota_fallback_enabled: enabled,
        fable_weekly_usage_limit_percent: limit_percent,
        request_model: Some(model.into()),
    }
}

fn fable_usage_window(utilization: i64, reset_offset: Duration) -> serde_json::Value {
    serde_json::json!({
        "seven_day_fable": {
            "utilization": utilization,
            "resets_at": (Utc::now() + reset_offset).to_rfc3339(),
        }
    })
}

/// 构造执行普通用量控制线的消息选号上下文。
/// @return 不依赖模型名称的消息上下文。
fn usage_limits_context() -> AccountSelectionContext {
    AccountSelectionContext {
        usage_limits_enabled: true,
        ..AccountSelectionContext::disabled()
    }
}

/// 两类账号的两个普通窗口都必须在临界点换号，且只在真实承载后重绑。
/// @return 无；达线、粘性绑定或账号状态错误时测试失败。
#[tokio::test]
async fn usage_limits_sticky_boundary_supports_both_windows_and_auth_types() {
    for oauth in [false, true] {
        for (window, limit) in [("five_hour", 80), ("seven_day", 60)] {
            let (store, svc) = setup().await;
            let mut account = create_test_account(&svc, "limited@example.invalid").await;
            let spare = create_test_account(&svc, "spare@example.invalid").await;
            if oauth {
                mark_oauth_account(&svc, &mut account).await;
            }
            account.five_hour_usage_limit_percent = 80;
            account.seven_day_usage_limit_percent = 60;
            svc.update_account(&account).await.unwrap();
            let session = "usage-limit-boundary";
            svc.bind_selected_session(session, account.id)
                .await
                .unwrap();
            for (utilization, expected) in [
                (f64::from(limit) - 0.1, account.id),
                (f64::from(limit), spare.id),
            ] {
                let mut usage = json!({});
                usage[window] = json!({
                    "utilization": utilization,
                    "resets_at": (Utc::now() + Duration::hours(2)).to_rfc3339(),
                });
                store
                    .update_usage(account.id, &usage.to_string())
                    .await
                    .unwrap();
                let selected = svc
                    .select_account_with_selection_context(
                        session,
                        &[],
                        &[],
                        &usage_limits_context(),
                    )
                    .await
                    .unwrap();
                assert_eq!(selected.account.id, expected, "{window}, OAuth={oauth}");
                assert_eq!(selected.sticky, expected == account.id);
                assert_eq!(selected.should_bind_session, expected != account.id);
            }
            let original = svc
                .select_account_with_context(session, &[], &[])
                .await
                .unwrap();
            assert_eq!(original.account.id, account.id, "换号前不得删除旧绑定");
            svc.bind_selected_session(session, spare.id).await.unwrap();
            let rebound = svc
                .select_account_with_selection_context(session, &[], &[], &usage_limits_context())
                .await
                .unwrap();
            assert_eq!(rebound.account.id, spare.id);
            assert!(rebound.sticky);
            let unchanged = svc.get_account(account.id).await.unwrap();
            assert_eq!(unchanged.status, AccountStatus::Active);
            assert!(unchanged.rate_limit_reset_at.is_none());
        }
    }
}

/// 各账号使用独立百分比，达线过滤不能越过令牌范围或回退到受限账号。
/// @return 无；控制线、关闭或令牌范围错误时测试失败。
#[tokio::test]
async fn usage_limits_are_per_account_and_respect_token_scope() {
    let (store, svc) = setup().await;
    let mut first = create_test_account(&svc, "first-limit@example.invalid").await;
    let mut second = create_test_account(&svc, "second-limit@example.invalid").await;
    first.priority = 1;
    first.five_hour_usage_limit_percent = 80;
    second.five_hour_usage_limit_percent = 90;
    for account in [&first, &second] {
        svc.update_account(account).await.unwrap();
        store.update_usage(account.id, &json!({
            "five_hour": {"utilization": 85, "resets_at": (Utc::now() + Duration::hours(2)).to_rfc3339()},
        }).to_string()).await.unwrap();
    }
    let context = usage_limits_context();
    assert_eq!(
        svc.select_account_with_selection_context("", &[], &[], &context)
            .await
            .unwrap()
            .account
            .id,
        second.id
    );
    svc.bind_selected_session("limited-only", first.id)
        .await
        .unwrap();
    for (excluded, allowed) in [(vec![], vec![first.id]), (vec![second.id], vec![])] {
        let result = svc
            .select_account_with_selection_context("limited-only", &excluded, &allowed, &context)
            .await;
        assert!(
            matches!(result, Err(AppError::TooManyRequests(ref message)) if message.contains("用量上限"))
        );
    }
    assert_eq!(
        svc.select_account_with_context("limited-only", &[], &[])
            .await
            .unwrap()
            .account
            .id,
        first.id
    );
    first.five_hour_usage_limit_percent = 0;
    svc.update_account(&first).await.unwrap();
    assert_eq!(
        svc.select_account_with_selection_context("limited-only", &[], &[], &context)
            .await
            .unwrap()
            .account
            .id,
        first.id
    );
}

/// 过期、缺失或脏窗口不能误伤账号，两个窗口的有效达线判断彼此独立。
/// @return 无；窗口恢复、阈值调整或脏数据处理错误时测试失败。
#[tokio::test]
async fn usage_limits_invalid_windows_reset_and_configuration_recovery() {
    let (store, svc) = setup().await;
    let mut account = create_test_account(&svc, "window-recovery@example.invalid").await;
    account.five_hour_usage_limit_percent = 50;
    account.seven_day_usage_limit_percent = 50;
    svc.update_account(&account).await.unwrap();
    let future = (Utc::now() + Duration::hours(2)).to_rfc3339();
    let past = (Utc::now() - Duration::seconds(1)).to_rfc3339();
    for window in ["five_hour", "seven_day"] {
        for invalid in [
            json!(null),
            json!({}),
            json!({"utilization": -1, "resets_at": future}),
            json!({"utilization": "100", "resets_at": future}),
            json!({"utilization": true, "resets_at": future}),
            json!({"utilization": 100}),
            json!({"utilization": 100, "resets_at": null}),
            json!({"utilization": 100, "resets_at": "invalid"}),
            json!({"utilization": 100, "resets_at": past}),
        ] {
            let mut usage = json!({});
            usage[window] = invalid;
            store
                .update_usage(account.id, &usage.to_string())
                .await
                .unwrap();
            assert_eq!(
                svc.select_account_with_selection_context("", &[], &[], &usage_limits_context())
                    .await
                    .unwrap()
                    .account
                    .id,
                account.id
            );
        }
    }
    store
        .update_usage(
            account.id,
            &json!({
                "five_hour": {"utilization": 100, "resets_at": past},
                "seven_day": {"utilization": 50, "resets_at": future},
            })
            .to_string(),
        )
        .await
        .unwrap();
    assert!(matches!(
        svc.select_account_with_selection_context("", &[], &[], &usage_limits_context())
            .await,
        Err(AppError::TooManyRequests(_))
    ));
    // 非消息入口继续兼容；调高后下一次选号即可恢复，不依赖后台清理。
    assert_eq!(
        svc.select_account("", &[], &[]).await.unwrap().id,
        account.id
    );
    account.seven_day_usage_limit_percent = 51;
    svc.update_account(&account).await.unwrap();
    assert!(
        svc.select_account_with_selection_context("", &[], &[], &usage_limits_context())
            .await
            .is_ok()
    );
    account.seven_day_usage_limit_percent = 100;
    svc.update_account(&account).await.unwrap();
    for (utilization, blocked) in [(99.9, false), (100.0, true)] {
        store
            .update_usage(
                account.id,
                &json!({
                    "seven_day": {"utilization": utilization, "resets_at": future},
                })
                .to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            svc.select_account_with_selection_context("", &[], &[], &usage_limits_context())
                .await
                .is_err(),
            blocked
        );
    }
}

/// 普通窗口与 Fable 窗口可以分别排除不同账号，不能在过滤后重新放回。
/// @return 无；混合限制被绕过或污染非 Fable 请求时测试失败。
#[tokio::test]
async fn usage_limits_combine_with_fable_without_fallback_to_blocked_accounts() {
    let (store, svc) = setup().await;
    let mut general = create_test_account(&svc, "general-limit@example.invalid").await;
    let mut fable = create_test_account(&svc, "fable-limit@example.invalid").await;
    mark_oauth_account(&svc, &mut general).await;
    mark_oauth_account(&svc, &mut fable).await;
    general.five_hour_usage_limit_percent = 80;
    svc.update_account(&general).await.unwrap();
    store.update_usage(general.id, &json!({
        "five_hour": {"utilization": 80, "resets_at": (Utc::now() + Duration::hours(2)).to_rfc3339()},
    }).to_string()).await.unwrap();
    store
        .update_usage(
            fable.id,
            &fable_usage_window(50, Duration::days(3)).to_string(),
        )
        .await
        .unwrap();
    let mut context = fable_selection_context(true, "claude-fable-5");
    context.usage_limits_enabled = true;
    for session in ["", "mixed-limit-session"] {
        if !session.is_empty() {
            svc.bind_selected_session(session, general.id)
                .await
                .unwrap();
        }
        assert!(matches!(
            svc.select_account_with_selection_context(session, &[], &[], &context)
                .await,
            Err(AppError::TooManyRequests(_))
        ));
    }
    context.request_model = Some("claude-sonnet-5".into());
    assert_eq!(
        svc.select_account_with_selection_context("", &[], &[], &context)
            .await
            .unwrap()
            .account
            .id,
        fable.id
    );
}

/// 内部服务调用也必须拒绝非法控制线，不能只依赖 HTTP 表单校验。
/// @return 无；无效创建或更新成功时测试失败。
#[tokio::test]
async fn usage_limits_service_rejects_out_of_range_without_writing() {
    let (_, svc) = setup().await;
    let original = create_test_account(&svc, "validation@example.invalid").await;
    for value in [-1, 101] {
        for five_hour in [false, true] {
            let mut invalid = original.clone();
            if five_hour {
                invalid.five_hour_usage_limit_percent = value;
            } else {
                invalid.seven_day_usage_limit_percent = value;
            }
            assert!(matches!(
                svc.update_account(&invalid).await,
                Err(AppError::BadRequest(_))
            ));
            invalid.id = 0;
            invalid.email = "invalid-create@example.invalid".into();
            assert!(matches!(
                svc.create_account(&mut invalid).await,
                Err(AppError::BadRequest(_))
            ));
        }
    }
    let unchanged = svc.get_account(original.id).await.unwrap();
    assert_eq!(unchanged.five_hour_usage_limit_percent, 0);
    assert_eq!(unchanged.seven_day_usage_limit_percent, 0);
    assert_eq!(svc.list_accounts().await.unwrap().len(), 1);
}

// ─── 429 限流测试 ────────────────────────────────────────────

#[tokio::test]
async fn test_429_disables_account_for_5h() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "test429@example.com").await;

    // 模拟 429：停用 5 小时，状态保持 Active（靠 rate_limit_reset_at 拦截调度）
    let reset_at = Utc::now() + Duration::hours(5);
    svc.disable_account(
        account.id,
        AccountStatus::Active,
        "429 速率限制",
        Some(reset_at),
    )
    .await
    .expect("disable_account failed");

    let updated = svc
        .get_account(account.id)
        .await
        .expect("get_account failed");
    assert_eq!(updated.status, AccountStatus::Active);
    assert_eq!(updated.disable_reason, "429 速率限制");
    assert!(updated.rate_limit_reset_at.is_some());
    assert!(updated.rate_limited_at.is_some());
    // 应不可调度
    assert!(!updated.is_schedulable());
}

#[tokio::test]
async fn test_429_account_recovers_after_expiry() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "test429recovery@example.com").await;

    // 设置一个已经过期的 rate_limit_reset_at
    let reset_at = Utc::now() - Duration::seconds(10);
    svc.disable_account(
        account.id,
        AccountStatus::Active,
        "429 速率限制",
        Some(reset_at),
    )
    .await
    .expect("disable_account failed");

    let updated = svc
        .get_account(account.id)
        .await
        .expect("get_account failed");
    // rate_limit_reset_at 已过期，应该可以调度
    assert!(updated.is_schedulable());
}

#[tokio::test]
async fn test_429_account_excluded_from_schedulable_list() {
    let (store, svc) = setup().await;
    let a1 = create_test_account(&svc, "active@example.com").await;
    let a2 = create_test_account(&svc, "limited@example.com").await;

    // a2 被 429 限流
    let reset_at = Utc::now() + Duration::hours(5);
    svc.disable_account(a2.id, AccountStatus::Active, "429 速率限制", Some(reset_at))
        .await
        .unwrap();

    let schedulable = store.list_schedulable().await.unwrap();
    let ids: Vec<i64> = schedulable.iter().map(|a| a.id).collect();
    assert!(ids.contains(&a1.id));
    assert!(!ids.contains(&a2.id));
}

// ─── 403 认证失败测试 ────────────────────────────────────────

#[tokio::test]
async fn test_403_permanently_disables_account() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "test403@example.com").await;

    svc.disable_account(account.id, AccountStatus::Disabled, "403 认证失败", None)
        .await
        .expect("disable_account failed");

    let updated = svc
        .get_account(account.id)
        .await
        .expect("get_account failed");
    assert_eq!(updated.status, AccountStatus::Disabled);
    assert_eq!(updated.disable_reason, "403 认证失败");
    assert!(updated.rate_limit_reset_at.is_none());
    assert!(!updated.is_schedulable());
}

#[tokio::test]
async fn test_403_account_excluded_from_schedulable_list() {
    let (store, svc) = setup().await;
    let a1 = create_test_account(&svc, "ok@example.com").await;
    let a2 = create_test_account(&svc, "forbidden@example.com").await;

    svc.disable_account(a2.id, AccountStatus::Disabled, "403 认证失败", None)
        .await
        .unwrap();

    let schedulable = store.list_schedulable().await.unwrap();
    let ids: Vec<i64> = schedulable.iter().map(|a| a.id).collect();
    assert!(ids.contains(&a1.id));
    assert!(!ids.contains(&a2.id));
}

// ─── 手动停用/启用测试 ──────────────────────────────────────

#[tokio::test]
async fn test_manual_disable_sets_reason() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "manual@example.com").await;

    svc.disable_account(account.id, AccountStatus::Disabled, "手动停用", None)
        .await
        .unwrap();

    let updated = svc.get_account(account.id).await.unwrap();
    assert_eq!(updated.status, AccountStatus::Disabled);
    assert_eq!(updated.disable_reason, "手动停用");
    assert!(!updated.is_schedulable());
}

#[tokio::test]
async fn test_enable_clears_everything() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "enable@example.com").await;

    // 先禁用（模拟 429）
    let reset_at = Utc::now() + Duration::hours(5);
    svc.disable_account(
        account.id,
        AccountStatus::Active,
        "429 速率限制",
        Some(reset_at),
    )
    .await
    .unwrap();

    // 手动启用
    svc.enable_account(account.id).await.unwrap();

    let updated = svc.get_account(account.id).await.unwrap();
    assert_eq!(updated.status, AccountStatus::Active);
    assert_eq!(updated.disable_reason, "");
    assert!(updated.rate_limited_at.is_none());
    assert!(updated.rate_limit_reset_at.is_none());
    assert!(updated.is_schedulable());
}

#[tokio::test]
async fn test_enable_recovers_403_disabled_account() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "recover403@example.com").await;

    // 403 永久禁用
    svc.disable_account(account.id, AccountStatus::Disabled, "403 认证失败", None)
        .await
        .unwrap();

    // 手动启用
    svc.enable_account(account.id).await.unwrap();

    let updated = svc.get_account(account.id).await.unwrap();
    assert_eq!(updated.status, AccountStatus::Active);
    assert_eq!(updated.disable_reason, "");
    assert!(updated.is_schedulable());
}

// ─── select_account 集成测试 ─────────────────────────────────

#[tokio::test]
async fn test_select_account_skips_429_limited() {
    let (_store, svc) = setup().await;
    let a1 = create_test_account(&svc, "selectable@example.com").await;
    let a2 = create_test_account(&svc, "rate_limited@example.com").await;

    // a2 被限流
    let reset_at = Utc::now() + Duration::hours(5);
    svc.disable_account(a2.id, AccountStatus::Active, "429 速率限制", Some(reset_at))
        .await
        .unwrap();

    // select_account 应该只返回 a1
    let selected = svc.select_account("", &[], &[]).await.unwrap();
    assert_eq!(selected.id, a1.id);
}

#[tokio::test]
async fn test_select_account_skips_403_disabled() {
    let (_store, svc) = setup().await;
    let a1 = create_test_account(&svc, "good@example.com").await;
    let a2 = create_test_account(&svc, "bad@example.com").await;

    // a2 被 403 禁用
    svc.disable_account(a2.id, AccountStatus::Disabled, "403 认证失败", None)
        .await
        .unwrap();

    let selected = svc.select_account("", &[], &[]).await.unwrap();
    assert_eq!(selected.id, a1.id);
}

#[tokio::test]
async fn test_select_account_fails_when_all_disabled() {
    let (_store, svc) = setup().await;
    let a1 = create_test_account(&svc, "only@example.com").await;

    svc.disable_account(a1.id, AccountStatus::Disabled, "403 认证失败", None)
        .await
        .unwrap();

    let result = svc.select_account("", &[], &[]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::ServiceUnavailable(_) => {} // 预期
        e => panic!("expected ServiceUnavailable, got: {:?}", e),
    }
}

// ─── RPM 调度测试 ───────────────────────────────────────────

#[tokio::test]
async fn test_rpm_disabled_allows_repeated_admission() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "rpm-off@example.com").await;

    svc.acquire_account_rpm(&account, false, "")
        .await
        .expect("rpm disabled should allow first request");
    svc.acquire_account_rpm(&account, false, "")
        .await
        .expect("rpm disabled should allow repeated requests");

    let status = svc.get_account_rpm_status(&account).await.unwrap();
    assert_eq!(status.limit, 0);
    assert_eq!(status.current, 0);
    assert_eq!(status.remaining, None);
    assert!(!status.saturated);
}

#[tokio::test]
async fn test_non_sticky_selection_skips_rpm_saturated_account() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "rpm-full@example.com").await;
    let mut a2 = create_test_account(&svc, "rpm-free@example.com").await;
    a1.priority = 10;
    a1.rpm_limit = 1;
    a2.priority = 20;
    a2.rpm_limit = 1;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();

    svc.acquire_account_rpm(&a1, false, "")
        .await
        .expect("first request should consume a1 rpm");

    let selected = svc.select_account("", &[], &[]).await.unwrap();
    assert_eq!(selected.id, a2.id);
}

#[tokio::test]
async fn test_fable_sticky_at_limit_selects_alternative_account() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "fable-sticky-full@example.com").await;
    let mut a2 = create_test_account(&svc, "fable-sticky-free@example.com").await;
    a1.priority = 10;
    a2.priority = 20;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();
    mark_oauth_account(&svc, &mut a1).await;
    mark_oauth_account(&svc, &mut a2).await;
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(50, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let session_hash = "fable-sticky-full-session";
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .unwrap();

    let selected = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(true, "claude-fable-5"),
        )
        .await
        .unwrap();

    assert_eq!(selected.account.id, a2.id);
    assert!(!selected.sticky);
    assert!(selected.should_bind_session);

    svc.bind_selected_session(session_hash, selected.account.id)
        .await
        .unwrap();
    let rebound = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(true, "claude-fable-5[1m]"),
        )
        .await
        .unwrap();
    assert_eq!(rebound.account.id, a2.id);
    assert!(rebound.sticky);
}

#[tokio::test]
async fn test_fable_sticky_below_limit_keeps_original_account() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "fable-sticky-below@example.com").await;
    let mut a2 = create_test_account(&svc, "fable-sticky-spare@example.com").await;
    a1.priority = 10;
    a2.priority = 20;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();
    mark_oauth_account(&svc, &mut a1).await;
    mark_oauth_account(&svc, &mut a2).await;
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(49, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let session_hash = "fable-sticky-below-limit-session";
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .unwrap();

    let selected = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(true, "claude-fable-5"),
        )
        .await
        .unwrap();

    assert_eq!(selected.account.id, a1.id);
    assert!(selected.sticky);
    assert!(!selected.should_bind_session);
}

#[tokio::test]
async fn test_fable_sticky_exhausted_without_alternative_keeps_binding() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "fable-sticky-only@example.com").await;
    mark_oauth_account(&svc, &mut a1).await;
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(100, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let session_hash = "fable-sticky-only-session";
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .unwrap();

    let result = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(true, "claude-fable-5"),
        )
        .await;
    match result.unwrap_err() {
        AppError::TooManyRequests(_) => {}
        e => panic!("expected TooManyRequests, got: {:?}", e),
    }

    let selected = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(false, "claude-fable-5"),
        )
        .await
        .unwrap();
    assert_eq!(selected.account.id, a1.id);
    assert!(selected.sticky);
}

#[tokio::test]
async fn test_fable_sticky_fallback_disabled_keeps_original_sticky() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "fable-disabled-full@example.com").await;
    let mut a2 = create_test_account(&svc, "fable-disabled-free@example.com").await;
    a1.priority = 10;
    a2.priority = 20;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();
    mark_oauth_account(&svc, &mut a1).await;
    mark_oauth_account(&svc, &mut a2).await;
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(100, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let session_hash = "fable-disabled-session";
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .unwrap();

    let selected = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(false, "claude-fable-5"),
        )
        .await
        .unwrap();
    assert_eq!(selected.account.id, a1.id);
    assert!(selected.sticky);
}

#[tokio::test]
async fn test_non_fable_sticky_ignores_fable_quota_exhaustion() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "non-fable-sticky-full@example.com").await;
    let mut a2 = create_test_account(&svc, "non-fable-sticky-free@example.com").await;
    a1.priority = 10;
    a2.priority = 20;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();
    mark_oauth_account(&svc, &mut a1).await;
    mark_oauth_account(&svc, &mut a2).await;
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(100, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let session_hash = "non-fable-sticky-session";
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .unwrap();

    let selected = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(true, "claude-opus-4-8"),
        )
        .await
        .unwrap();
    assert_eq!(selected.account.id, a1.id);
    assert!(selected.sticky);
}

#[tokio::test]
async fn test_setup_token_sticky_ignores_fable_quota_usage_data() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "setup-token-sticky-full@example.com").await;
    let mut a2 = create_test_account(&svc, "setup-token-sticky-free@example.com").await;
    a1.priority = 10;
    a2.priority = 20;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(100, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let session_hash = "setup-token-sticky-session";
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .unwrap();

    let selected = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context(true, "claude-fable-5"),
        )
        .await
        .unwrap();
    assert_eq!(selected.account.id, a1.id);
    assert!(selected.sticky);
}

#[tokio::test]
async fn test_non_sticky_fable_selection_filters_exhausted_accounts() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "fable-non-sticky-full@example.com").await;
    let mut a2 = create_test_account(&svc, "fable-non-sticky-free@example.com").await;
    a1.priority = 10;
    a2.priority = 20;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();
    mark_oauth_account(&svc, &mut a1).await;
    mark_oauth_account(&svc, &mut a2).await;
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(50, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let selected = svc
        .select_account_with_selection_context(
            "",
            &[],
            &[],
            &fable_selection_context(true, "claude-fable-5"),
        )
        .await
        .unwrap();
    assert_eq!(selected.account.id, a2.id);
    assert!(!selected.sticky);
    assert!(!selected.should_bind_session);
}

#[tokio::test]
async fn test_non_sticky_fable_selection_returns_429_when_all_accounts_reach_limit() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "fable-all-limit-a@example.com").await;
    let mut a2 = create_test_account(&svc, "fable-all-limit-b@example.com").await;
    mark_oauth_account(&svc, &mut a1).await;
    mark_oauth_account(&svc, &mut a2).await;
    for account in [&a1, &a2] {
        svc.update_passive_usage(
            account.id,
            fable_usage_window(50, Duration::days(3)),
            UsageObservationKind::Allowed,
        )
        .await
        .unwrap();
    }

    let result = svc
        .select_account_with_selection_context(
            "",
            &[],
            &[],
            &fable_selection_context(true, "claude-fable-5"),
        )
        .await;

    match result.unwrap_err() {
        AppError::TooManyRequests(message) => assert!(message.contains("50%")),
        error => panic!("expected TooManyRequests, got: {:?}", error),
    }
}

#[tokio::test]
async fn test_fable_limit_100_preserves_previous_exhaustion_boundary() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "fable-limit-100@example.com").await;
    let mut a2 = create_test_account(&svc, "fable-limit-100-spare@example.com").await;
    a1.priority = 10;
    a2.priority = 20;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();
    mark_oauth_account(&svc, &mut a1).await;
    mark_oauth_account(&svc, &mut a2).await;
    svc.update_passive_usage(
        a1.id,
        fable_usage_window(99, Duration::days(3)),
        UsageObservationKind::Allowed,
    )
    .await
    .unwrap();

    let session_hash = "fable-limit-100-session";
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .unwrap();

    let selected = svc
        .select_account_with_selection_context(
            session_hash,
            &[],
            &[],
            &fable_selection_context_with_limit(true, "claude-fable-5", 100),
        )
        .await
        .unwrap();

    assert_eq!(selected.account.id, a1.id);
    assert!(selected.sticky);
}

#[tokio::test]
async fn test_sticky_selection_does_not_switch_when_rpm_saturated() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "sticky-rpm-full@example.com").await;
    let mut a2 = create_test_account(&svc, "sticky-rpm-free@example.com").await;
    a1.priority = 10;
    a1.rpm_limit = 1;
    a2.priority = 20;
    a2.rpm_limit = 1;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();

    let session_hash = "sticky-rpm-session";
    let selected = svc
        .select_account_with_context(session_hash, &[], &[])
        .await
        .unwrap();
    assert_eq!(selected.account.id, a1.id);
    assert!(!selected.sticky);
    assert!(selected.should_bind_session);

    svc.acquire_account_rpm(&a1, false, session_hash)
        .await
        .expect("first request should consume sticky account rpm");
    svc.bind_selected_session(session_hash, a1.id)
        .await
        .expect("successful request should bind sticky session");

    let selected = svc
        .select_account_with_context(session_hash, &[], &[])
        .await
        .unwrap();
    assert_eq!(selected.account.id, a1.id);
    assert!(selected.sticky);
}

#[tokio::test]
async fn test_uncommitted_session_selection_does_not_pollute_rpm_retry() {
    let (_store, svc) = setup().await;
    let mut a1 = create_test_account(&svc, "rpm-retry-full@example.com").await;
    let mut a2 = create_test_account(&svc, "rpm-retry-free@example.com").await;
    a1.priority = 10;
    a1.rpm_limit = 1;
    a2.priority = 20;
    a2.rpm_limit = 1;
    svc.update_account(&a1).await.unwrap();
    svc.update_account(&a2).await.unwrap();

    let session_hash = "rpm-retry-session";
    svc.acquire_account_rpm(&a1, false, "")
        .await
        .expect("first request should saturate a1 rpm");

    let selected = svc
        .select_account_with_context(session_hash, &[], &[])
        .await
        .unwrap();
    assert_eq!(selected.account.id, a2.id);
    assert!(!selected.sticky);
    assert!(selected.should_bind_session);

    let selected = svc
        .select_account_with_context(session_hash, &[a2.id], &[])
        .await
        .unwrap();
    assert_eq!(selected.account.id, a1.id);
    assert!(!selected.sticky);
}

#[tokio::test]
async fn test_sticky_rpm_saturation_rejects_instead_of_switching() {
    wait_for_stable_rpm_window().await;

    let (_store, svc) = setup().await;
    let mut account = create_test_account(&svc, "sticky-rpm-reject@example.com").await;
    account.rpm_limit = 1;
    svc.update_account(&account).await.unwrap();

    svc.acquire_account_rpm(&account, true, "sticky-rpm-reject")
        .await
        .expect("first sticky request should consume rpm");

    let started = std::time::Instant::now();
    let result = svc
        .acquire_account_rpm(&account, true, "sticky-rpm-reject")
        .await;
    assert!(started.elapsed() >= std::time::Duration::from_secs(1));
    match result.unwrap_err() {
        AppError::TooManyRequests(_) => {}
        e => panic!("expected TooManyRequests, got: {:?}", e),
    }
}

// ─── disable_reason 覆盖测试 ────────────────────────────────

#[tokio::test]
async fn test_disable_reason_updates_on_repeated_429() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "repeat429@example.com").await;

    // 第一次 429
    let reset1 = Utc::now() + Duration::hours(5);
    svc.disable_account(
        account.id,
        AccountStatus::Active,
        "429 速率限制",
        Some(reset1),
    )
    .await
    .unwrap();

    // 第二次 429（重新计时）
    let reset2 = Utc::now() + Duration::hours(5);
    svc.disable_account(
        account.id,
        AccountStatus::Active,
        "429 速率限制",
        Some(reset2),
    )
    .await
    .unwrap();

    let updated = svc.get_account(account.id).await.unwrap();
    assert_eq!(updated.disable_reason, "429 速率限制");
    assert!(!updated.is_schedulable());
}

#[tokio::test]
async fn test_403_during_429_does_not_escalate() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "no_escalate@example.com").await;

    // 先 429 限流
    let reset_at = Utc::now() + Duration::hours(5);
    svc.disable_account(
        account.id,
        AccountStatus::Active,
        "429 速率限制",
        Some(reset_at),
    )
    .await
    .unwrap();

    // 模拟网关逻辑：403 到达时检测到已在限流中，不执行永久停用
    let updated = svc.get_account(account.id).await.unwrap();
    let is_rate_limited = updated
        .rate_limit_reset_at
        .map(|r| Utc::now() < r)
        .unwrap_or(false);
    assert!(is_rate_limited, "account should be rate-limited");

    // 因此不调用 disable_account(Disabled)，账号保持 429 限流状态
    let final_state = svc.get_account(account.id).await.unwrap();
    assert_eq!(final_state.status, AccountStatus::Active);
    assert_eq!(final_state.disable_reason, "429 速率限制");
    assert!(final_state.rate_limit_reset_at.is_some());
}

#[tokio::test]
async fn test_403_after_429_expired_does_disable() {
    let (_store, svc) = setup().await;
    let account = create_test_account(&svc, "expired_then_403@example.com").await;

    // 429 已过期
    let reset_at = Utc::now() - Duration::seconds(10);
    svc.disable_account(
        account.id,
        AccountStatus::Active,
        "429 速率限制",
        Some(reset_at),
    )
    .await
    .unwrap();

    // 限流已过期，403 应该正常永久停用
    let updated = svc.get_account(account.id).await.unwrap();
    let is_rate_limited = updated
        .rate_limit_reset_at
        .map(|r| Utc::now() < r)
        .unwrap_or(false);
    assert!(!is_rate_limited);

    // 执行 403 停用
    svc.disable_account(account.id, AccountStatus::Disabled, "403 认证失败", None)
        .await
        .unwrap();

    let final_state = svc.get_account(account.id).await.unwrap();
    assert_eq!(final_state.status, AccountStatus::Disabled);
    assert_eq!(final_state.disable_reason, "403 认证失败");
}
