//! 数据库版本台账及离线维护入口，保持原 PostgreSQL 初始化语义。
use std::collections::BTreeMap;
use std::path::Path;

use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::any::AnyPoolOptions;
use sqlx::{AnyPool, Row};
use uuid::Uuid;

use crate::config::Config;
use crate::store::database_schema::migrate_legacy;
use crate::store::db::init_db;

/// 返回初始迁移源码摘要，测试代码不进入持久合同。
/// @return 纯迁移定义的 SHA256。
pub fn schema_checksum() -> String {
    let source = include_str!("database_schema.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap_or_default();
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

fn registry() -> Result<Value, sqlx::Error> {
    let value: Value = serde_json::from_str(include_str!("database-migration-registry.json"))
        .map_err(|_| sqlx::Error::Protocol("迁移登记无效".into()))?;
    let steps = value["steps"]
        .as_array()
        .ok_or_else(|| sqlx::Error::Protocol("迁移登记步骤无效".into()))?;
    if value["schema_version"] != 1
        || value["database"] != "gateway"
        || steps.is_empty()
        || steps[0]["checksum"] != schema_checksum()
    {
        return Err(sqlx::Error::Protocol("迁移定义与登记不符".into()));
    }
    for (index, step) in steps.iter().enumerate() {
        if step["version"] != index + 1
            || step["predecessor"] != index
            || step["engine"] != "sqlite"
            || step["rollback_mode"] != "compatible"
            || !matches!(step["kind"].as_str(), Some("schema" | "data"))
        {
            return Err(sqlx::Error::Protocol("迁移登记版本链无效".into()));
        }
    }
    Ok(value)
}

fn contract(value: &Value) -> Result<String, sqlx::Error> {
    let mut steps = Vec::new();
    for step in value["steps"]
        .as_array()
        .ok_or_else(|| sqlx::Error::Protocol("迁移步骤缺失".into()))?
    {
        let sorted: BTreeMap<_, _> = step
            .as_object()
            .ok_or_else(|| sqlx::Error::Protocol("迁移步骤无效".into()))?
            .iter()
            .collect();
        steps.push(sorted);
    }
    let canonical = json!({"database":"gateway","schema_version":1,"steps":steps});
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|_| sqlx::Error::Protocol("迁移摘要编码失败".into()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

type SqliteStructure = BTreeMap<String, BTreeMap<String, (String, String)>>;

fn affinity(declaration: &str) -> String {
    let value = declaration.to_ascii_uppercase();
    if value.contains("INT") {
        "INTEGER"
    } else if ["CHAR", "CLOB", "TEXT"]
        .iter()
        .any(|part| value.contains(part))
    {
        "TEXT"
    } else if value.is_empty() || value.contains("BLOB") {
        "BLOB"
    } else if ["REAL", "FLOA", "DOUB"]
        .iter()
        .any(|part| value.contains(part))
    {
        "REAL"
    } else {
        "NUMERIC"
    }
    .to_string()
}

async fn structure(pool: &AnyPool) -> Result<SqliteStructure, sqlx::Error> {
    let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'system_schema_migrations'").fetch_all(pool).await?;
    let mut result = BTreeMap::new();
    for table in tables {
        let rows =
            sqlx::query("SELECT name,type,CAST(pk AS TEXT) AS position FROM pragma_table_info($1)")
                .bind(&table)
                .fetch_all(pool)
                .await?;
        let mut columns = BTreeMap::new();
        for row in rows {
            let name: String = row.try_get("name")?;
            let declaration: String = row.try_get("type")?;
            let position: String = row.try_get("position")?;
            columns.insert(name, (affinity(&declaration), position));
        }
        result.insert(table, columns);
    }
    Ok(result)
}

/// 从冻结基线核对 SQLite 最低结构，兼容后来新增字段。
/// @param pool 当前数据库池。
/// @param completed 已登记或执行完成时检查完整最低结构。
/// @return 结构符合合同或明确的拒绝原因。
pub async fn validate_structure(pool: &AnyPool, completed: bool) -> Result<(), sqlx::Error> {
    // 独立内存库复用纯定义，避免把业务默认值重新引入持久合同。
    let expected_pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    migrate_legacy(&expected_pool, "sqlite").await?;
    let expected = structure(&expected_pool).await?;
    expected_pool.close().await;
    let actual = structure(pool).await?;
    let invalid: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE name='accounts' AND type<>'table'",
    )
    .fetch_one(pool)
    .await?;
    if invalid != 0 || (!completed && !actual.is_empty() && !actual.contains_key("accounts")) {
        return Err(sqlx::Error::Protocol("旧库账号基础结构无效".into()));
    }
    if !completed && actual.is_empty() {
        return Ok(());
    }
    let core = [
        "id",
        "email",
        "token",
        "status",
        "device_id",
        "canonical_env",
    ];
    for (table, columns) in &expected {
        if !completed && table != "accounts" {
            continue;
        }
        for (column, required) in columns {
            if !completed && !core.contains(&column.as_str()) {
                continue;
            }
            let observed = actual.get(table).and_then(|fields| fields.get(column));
            let matches = observed.is_some_and(|value| {
                let numeric = matches!(value.0.as_str(), "INTEGER" | "REAL" | "NUMERIC")
                    && matches!(required.0.as_str(), "INTEGER" | "REAL" | "NUMERIC");
                value.1 == required.1 && (value.0 == required.0 || (required.1 == "0" && numeric))
            });
            if !matches {
                return Err(sqlx::Error::Protocol(format!(
                    "数据库结构不符合迁移基线：{table}.{column}"
                )));
            }
        }
    }
    if completed {
        let integrity: Vec<String> = sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_all(pool)
            .await?;
        if integrity != ["ok"] {
            return Err(sqlx::Error::Protocol("数据库完整性校验失败".into()));
        }
    }
    Ok(())
}

/// 读取并校验 SQLite 迁移登记，保持查询为只读。
/// @param pool 当前数据库池。
/// @param target 可选目标版本。
/// @return 当前计划或台账损坏错误。
pub async fn plan(pool: &AnyPool, target: Option<usize>) -> Result<Value, sqlx::Error> {
    let registered = registry()?;
    let mut steps = registered["steps"].as_array().cloned().unwrap_or_default();
    let target = target.unwrap_or(steps.len());
    if target == 0 || target > steps.len() {
        return Err(sqlx::Error::Protocol("迁移目标无效".into()));
    }
    let exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='system_schema_migrations'",
    )
    .fetch_one(pool)
    .await?;
    let rows = if exists > 0 {
        sqlx::query("SELECT migration_id,CAST(version AS TEXT) AS version,checksum FROM system_schema_migrations ORDER BY version").fetch_all(pool).await?
    } else {
        Vec::new()
    };
    for (index, row) in rows.iter().enumerate() {
        let version: String = row.try_get("version")?;
        let checksum: String = row.try_get("checksum")?;
        let identity: String = row.try_get("migration_id")?;
        if version != (index + 1).to_string()
            || checksum.len() != 64
            || !checksum.bytes().all(|b| b.is_ascii_hexdigit())
            || !identity.starts_with("gateway.")
            || steps
                .get(index)
                .is_some_and(|step| step["id"] != identity || step["checksum"] != checksum)
        {
            return Err(sqlx::Error::Protocol(
                "数据库迁移台账版本或摘要与当前程序不符".into(),
            ));
        }
    }
    validate_structure(pool, !rows.is_empty()).await?;
    for step in &mut steps {
        step["state"] = json!(
            if step["version"].as_u64().unwrap_or(u64::MAX) <= rows.len() as u64 {
                "completed"
            } else {
                "pending"
            }
        );
    }
    steps.truncate(target);
    Ok(
        json!({"database":"gateway","contract":contract(&registered)?,"current_version":rows.len(),"target_version":target,
        "steps":steps,"state":if rows.len()>target {"compatible_newer"}else{"ready"},"reason":null}),
    )
}

/// 沿兼容调用入口执行原初始化，SQLite 完成后登记版本。
/// @param pool 当前数据库池。
/// @param driver 当前数据库驱动。
/// @return 初始化结果。
pub async fn migrate(pool: &AnyPool, driver: &str) -> Result<(), sqlx::Error> {
    if driver != "sqlite" {
        return migrate_legacy(pool, driver).await;
    }
    apply(pool, &Uuid::new_v4().to_string(), None)
        .await
        .map(|_| ())
}

/// 执行本库完整幂等基线，成功后提交版本台账。
/// @param pool 当前 SQLite 数据库池。
/// @param operation_id 已验证的操作 UUID。
/// @param target 可选目标版本，省略时为程序最新版本。
/// @return 完成计划或受控数据库错误。
pub async fn apply(
    pool: &AnyPool,
    operation_id: &str,
    target: Option<usize>,
) -> Result<Value, sqlx::Error> {
    if Uuid::parse_str(operation_id).is_err() {
        return Err(sqlx::Error::Protocol("数据库迁移操作身份无效".into()));
    }
    let current = plan(pool, target).await?;
    if current["current_version"].as_u64() >= current["target_version"].as_u64() {
        return Ok(current);
    }
    for step in current["steps"]
        .as_array()
        .ok_or_else(|| sqlx::Error::Protocol("迁移步骤缺失".into()))?
    {
        if step["state"] == "completed" {
            continue;
        }
        match step["id"].as_str() {
            Some("gateway.baseline.1") => migrate_legacy(pool, "sqlite").await?,
            _ => return Err(sqlx::Error::Protocol("迁移实现尚未登记".into())),
        }
        validate_structure(pool, true).await?;
        let mut transaction = pool.begin().await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS system_schema_migrations (migration_id TEXT PRIMARY KEY,version INTEGER NOT NULL UNIQUE,checksum TEXT NOT NULL,applied_at TEXT NOT NULL,operation_id TEXT NOT NULL)")
            .execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO system_schema_migrations VALUES($1,$2,$3,$4,$5) ON CONFLICT(migration_id) DO NOTHING")
            .bind(step["id"].as_str().unwrap_or_default()).bind(step["version"].as_i64().unwrap_or_default())
            .bind(step["checksum"].as_str().unwrap_or_default()).bind(Utc::now().to_rfc3339()).bind(operation_id)
            .execute(&mut *transaction).await?;
        transaction.commit().await?;
    }
    plan(pool, target).await
}

/// 在启动业务服务前处理固定维护子命令。
/// @param arguments 包含程序名和维护参数的原始 argv。
/// @return 安全 JSON 结果；参数或执行失败返回固定说明。
pub async fn maintenance(arguments: &[String]) -> Result<Value, String> {
    if arguments.len() != 7
        || !matches!(arguments[2].as_str(), "plan" | "apply" | "verify")
        || arguments[3] != "--operation-id"
        || arguments[5] != "--target-version"
        || Uuid::parse_str(&arguments[4])
            .map(|id| id.to_string() != arguments[4])
            .unwrap_or(true)
    {
        return Err("数据库维护参数无效".into());
    }
    let target = arguments[6]
        .parse::<usize>()
        .map_err(|_| "迁移目标版本无效".to_string())?;
    let config = Config::load();
    if config.database.driver() != "sqlite" {
        return Err("离线维护仅支持本部署 SQLite".into());
    }
    let dsn = config.database.dsn();
    if Path::new(&dsn).is_symlink() {
        return Err("数据库维护路径无效".into());
    }
    sqlx::any::install_default_drivers();
    let pool = if arguments[2] == "apply" {
        init_db("sqlite", &dsn).await
    } else {
        AnyPool::connect(&format!("sqlite:{dsn}?mode=ro")).await
    }
    .map_err(|_| "数据库维护连接失败".to_string())?;
    let result = if arguments[2] == "apply" {
        apply(&pool, &arguments[4], Some(target)).await
    } else {
        plan(&pool, Some(target)).await
    };
    pool.close().await;
    let value = result.map_err(|_| "数据库迁移或台账校验失败".to_string())?;
    if arguments[2] == "verify"
        && value["current_version"].as_u64() < value["target_version"].as_u64()
    {
        return Err("数据库迁移尚未完成".into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{apply, plan};
    use crate::store::database_schema::migrate_legacy;
    use sqlx::AnyPool;
    use sqlx::any::{AnyPoolOptions, install_default_drivers};
    use uuid::Uuid;

    async fn pool() -> AnyPool {
        install_default_drivers();
        AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }

    /// 重复迁移保持合成配置及原操作台账。
    #[tokio::test]
    async fn repeated_apply_preserves_data_and_original_operation() {
        let database = pool().await;
        let operation = Uuid::new_v4().to_string();
        apply(&database, &operation, None).await.unwrap();
        sqlx::query("UPDATE settings SET value='debug' WHERE key='request_diagnostics_log_mode'")
            .execute(&database)
            .await
            .unwrap();
        sqlx::query("UPDATE settings SET value='2.1.280' WHERE key='claude_code_version_profile'")
            .execute(&database)
            .await
            .unwrap();
        let result = apply(&database, &Uuid::new_v4().to_string(), None)
            .await
            .unwrap();
        assert_eq!(result["current_version"], 1);
        let original: String =
            sqlx::query_scalar("SELECT operation_id FROM system_schema_migrations")
                .fetch_one(&database)
                .await
                .unwrap();
        assert_eq!(original, operation);
        let value: String = sqlx::query_scalar(
            "SELECT value FROM settings WHERE key='request_diagnostics_log_mode'",
        )
        .fetch_one(&database)
        .await
        .unwrap();
        assert_eq!(value, "debug");
        let profile: String = sqlx::query_scalar(
            "SELECT value FROM settings WHERE key='claude_code_version_profile'",
        )
        .fetch_one(&database)
        .await
        .unwrap();
        assert_eq!(profile, "2.1.280");
    }

    /// 台账摘要冲突在业务修改前明确拒绝。
    #[tokio::test]
    async fn corrupted_ledger_is_rejected() {
        let database = pool().await;
        apply(&database, &Uuid::new_v4().to_string(), None)
            .await
            .unwrap();
        sqlx::query("UPDATE system_schema_migrations SET checksum=$1")
            .bind("f".repeat(64))
            .execute(&database)
            .await
            .unwrap();
        assert!(
            apply(&database, &Uuid::new_v4().to_string(), None)
                .await
                .is_err()
        );
    }

    /// 旧程序保留兼容后续版本的结构与新写入。
    #[tokio::test]
    async fn compatible_newer_ledger_keeps_later_data() {
        let database = pool().await;
        apply(&database, &Uuid::new_v4().to_string(), None)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE accounts ADD COLUMN synthetic_note TEXT")
            .execute(&database)
            .await
            .unwrap();
        sqlx::query("INSERT INTO system_schema_migrations VALUES('gateway.synthetic.2',2,$1,'synthetic-time',$2)")
            .bind("a".repeat(64)).bind(Uuid::new_v4().to_string()).execute(&database).await.unwrap();
        let result = apply(&database, &Uuid::new_v4().to_string(), None)
            .await
            .unwrap();
        assert_eq!(result["state"], "compatible_newer");
        assert_eq!(result["current_version"], 2);
        assert!(plan(&database, Some(0)).await.is_err());
    }

    /// 未登记旧库的必要 token 列缺失时保持台账为空。
    #[tokio::test]
    async fn missing_legacy_token_does_not_receive_a_ledger() {
        let database = pool().await;
        migrate_legacy(&database, "sqlite").await.unwrap();
        sqlx::query("ALTER TABLE accounts DROP COLUMN token")
            .execute(&database)
            .await
            .unwrap();
        assert!(
            apply(&database, &Uuid::new_v4().to_string(), None)
                .await
                .is_err()
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='system_schema_migrations'",
        )
        .fetch_one(&database)
        .await
        .unwrap();
        assert_eq!(count, 0);
    }

    /// 已完成版本仍复核实际结构，拒绝损坏后的再次成功回执。
    #[tokio::test]
    async fn registered_schema_damage_is_rejected() {
        let database = pool().await;
        apply(&database, &Uuid::new_v4().to_string(), None)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE accounts DROP COLUMN token")
            .execute(&database)
            .await
            .unwrap();
        assert!(plan(&database, None).await.is_err());
        assert!(
            apply(&database, &Uuid::new_v4().to_string(), None)
                .await
                .is_err()
        );
    }

    /// 账号核心主键类型异常在结构迁移前明确拒绝。
    #[tokio::test]
    async fn incompatible_legacy_primary_key_is_rejected() {
        let database = pool().await;
        sqlx::query("CREATE TABLE accounts(id TEXT PRIMARY KEY,email TEXT,token TEXT,status TEXT,device_id TEXT,canonical_env TEXT)")
            .execute(&database).await.unwrap();
        assert!(
            apply(&database, &Uuid::new_v4().to_string(), None)
                .await
                .is_err()
        );
    }
}
