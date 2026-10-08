//! 验证两种数据库中的画像、准入、账号软件同步及事务回滚。

use claude_code_gateway::service::access_policy::AccessPolicy;
use claude_code_gateway::service::version_profile::profile_for_key;
use claude_code_gateway::store::db::migrate;
use claude_code_gateway::store::settings_store::SettingsStore;
use serde_json::Value;
use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use std::collections::HashMap;

async fn check_profile_settings(driver: &str, dsn: &str) {
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect(dsn)
        .await
        .unwrap();
    migrate(&pool, driver).await.unwrap();
    sqlx::query("INSERT INTO accounts (email,token,device_id,canonical_env,canonical_prompt_env,canonical_process,concurrency,rpm_limit,priority) VALUES ('synthetic@example.invalid','synthetic-token','synthetic-device','{\"version\":\"2.1.280\",\"custom\":{\"keep\":true}}','{}','{}',2,7,9)")
        .execute(&pool).await.unwrap();
    let store = SettingsStore::new_with_driver(pool.clone(), driver.into());
    let target = HashMap::from([
        (
            "claude_code_profile_selection_mode".into(),
            "client_version".into(),
        ),
        ("claude_code_version_profile".into(), "2.1.293".into()),
        (
            "allowed_claude_code_versions".into(),
            "2.1.89-2.1.293".into(),
        ),
        (
            "blocked_claude_code_versions".into(),
            "2.1.89-2.1.279,2.1.281-2.1.292".into(),
        ),
        ("allowed_user_agents".into(), "synthetic-client*".into()),
    ]);
    store
        .upsert_many_with_profile(&target, Some(profile_for_key("2.1.293").unwrap()))
        .await
        .unwrap();
    let settings = store.get_all().await.unwrap();
    for (key, value) in &target {
        assert_eq!(&settings[key], value);
    }
    let policy = AccessPolicy::parse(
        &settings["allowed_claude_code_versions"],
        &settings["blocked_claude_code_versions"],
        &settings["allowed_user_agents"],
    )
    .unwrap();
    for version in ["2.1.280", "2.1.293"] {
        assert!(
            policy
                .check_user_agent(&format!("claude-cli/{version} (external, cli)"))
                .is_ok()
        );
    }
    for version in ["2.1.260", "2.1.279", "2.1.281", "2.1.292", "2.1.294"] {
        assert!(
            policy
                .check_user_agent(&format!("claude-code/{version}"))
                .is_err()
        );
    }
    assert!(policy.check_user_agent("synthetic-client/1").is_ok());
    assert!(policy.check_user_agent("claude-code/").is_err());
    let env = account_env(&pool).await;
    assert_eq!(env["version"], "2.1.293");
    assert_eq!(env["custom"]["keep"], true);
    let identity: (String, String, i32, i32, i32) =
        sqlx::query_as("SELECT device_id,token,concurrency,rpm_limit,priority FROM accounts")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        identity,
        ("synthetic-device".into(), "synthetic-token".into(), 2, 7, 9)
    );

    // 只切换默认回退不能收窄客户端准入，也不能丢掉禁止区间。
    store
        .upsert_many_with_profile(
            &HashMap::from([("claude_code_version_profile".into(), "2.1.280".into())]),
            Some(profile_for_key("2.1.280").unwrap()),
        )
        .await
        .unwrap();
    let retained = store.get_all().await.unwrap();
    assert_eq!(
        retained["allowed_claude_code_versions"],
        target["allowed_claude_code_versions"]
    );
    assert_eq!(
        retained["blocked_claude_code_versions"],
        target["blocked_claude_code_versions"]
    );

    // 人工制造设置写入失败，确认先执行的账号环境更新也会回滚。
    if driver == "postgres" {
        sqlx::query("ALTER TABLE settings ADD CONSTRAINT synthetic_reject CHECK (key <> 'synthetic-reject')").execute(&pool).await.unwrap();
    } else {
        sqlx::query("CREATE TRIGGER synthetic_reject BEFORE INSERT ON settings WHEN NEW.key='synthetic-reject' BEGIN SELECT RAISE(ABORT,'synthetic reject'); END").execute(&pool).await.unwrap();
    }
    let before = account_env(&pool).await;
    let mut failure = target.clone();
    failure.insert("synthetic-reject".into(), "value".into());
    assert!(
        store
            .upsert_many_with_profile(&failure, Some(profile_for_key("2.1.293").unwrap()))
            .await
            .is_err()
    );
    assert_eq!(store.get_all().await.unwrap(), retained);
    assert_eq!(account_env(&pool).await, before);

    let account_mode = HashMap::from([
        (
            "claude_code_profile_selection_mode".into(),
            "account".into(),
        ),
        ("allowed_claude_code_versions".into(), "2.1.*".into()),
    ]);
    store
        .upsert_many_with_profile(&account_mode, Some(profile_for_key("2.1.260").unwrap()))
        .await
        .unwrap();
    assert_eq!(
        store.get_all().await.unwrap()["allowed_claude_code_versions"],
        "2.1.89-2.1.260"
    );

    // 旧出厂配对升级，两项设置与软件环境同时提交；自定义禁止与 UA 保留。
    store
        .upsert_many(&HashMap::from([
            (
                "claude_code_profile_selection_mode".into(),
                "client_version".into(),
            ),
            ("claude_code_version_profile".into(), "2.1.280".into()),
            (
                "allowed_claude_code_versions".into(),
                "2.1.89-2.1.280".into(),
            ),
        ]))
        .await
        .unwrap();
    migrate(&pool, driver).await.unwrap();
    let upgraded = store.get_all().await.unwrap();
    assert_eq!(upgraded["claude_code_version_profile"], "2.1.293");
    assert_eq!(upgraded["allowed_claude_code_versions"], "2.1.89-2.1.293");
    assert_eq!(
        upgraded["blocked_claude_code_versions"],
        target["blocked_claude_code_versions"]
    );
    assert_eq!(upgraded["allowed_user_agents"], "synthetic-client*");
    migrate(&pool, driver).await.unwrap();
    assert_eq!(store.get_all().await.unwrap(), upgraded);

    for profile in ["2.1.260", "2.1.280", "2.1.293"] {
        store
            .upsert_many(&HashMap::from([
                ("claude_code_version_profile".into(), profile.into()),
                (
                    "allowed_claude_code_versions".into(),
                    "2.1.260,2.1.280,2.1.293".into(),
                ),
            ]))
            .await
            .unwrap();
        let explicit = store.get_all().await.unwrap();
        migrate(&pool, driver).await.unwrap();
        assert_eq!(store.get_all().await.unwrap(), explicit);
        assert_eq!(account_env(&pool).await["version"], profile);
    }

    // 账号模式是管理员显式选择；即使画像和范围碰巧等于旧出厂值也不自动升级。
    for profile in ["2.1.260", "2.1.280"] {
        store
            .upsert_many(&HashMap::from([
                (
                    "claude_code_profile_selection_mode".into(),
                    "account".into(),
                ),
                ("claude_code_version_profile".into(), profile.into()),
                (
                    "allowed_claude_code_versions".into(),
                    format!("2.1.89-{profile}"),
                ),
            ]))
            .await
            .unwrap();
        let explicit = store.get_all().await.unwrap();
        migrate(&pool, driver).await.unwrap();
        assert_eq!(store.get_all().await.unwrap(), explicit);
        assert_eq!(account_env(&pool).await["version"], profile);
        migrate(&pool, driver).await.unwrap();
        assert_eq!(store.get_all().await.unwrap(), explicit);
    }
}

async fn account_env(pool: &AnyPool) -> Value {
    let raw: String = sqlx::query_scalar("SELECT CAST(canonical_env AS TEXT) FROM accounts")
        .fetch_one(pool)
        .await
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

#[tokio::test]
async fn sqlite_profile_settings_and_rollback() {
    check_profile_settings("sqlite", "sqlite::memory:").await;
}

#[tokio::test]
#[ignore = "需要通过 CC2API_PROFILE_TEST_POSTGRES_DSN 指定独立临时数据库"]
async fn postgres_profile_settings_and_rollback() {
    let dsn =
        std::env::var("CC2API_PROFILE_TEST_POSTGRES_DSN").expect("必须提供临时 PostgreSQL DSN");
    check_profile_settings("postgres", &dsn).await;
}
