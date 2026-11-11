//! 数据库连接及版本化迁移的兼容入口。
use sqlx::AnyPool;
use std::path::Path;

pub use super::database_migrations::migrate;

/// 创建原有数据库连接，SQLite 使用持久 WAL。
/// @param driver 原数据库驱动。
/// @param dsn 原数据库连接配置。
/// @return SQLx 连接池或连接错误。
pub async fn init_db(driver: &str, dsn: &str) -> Result<AnyPool, sqlx::Error> {
    if driver == "sqlite" {
        if let Some(parent) = Path::new(dsn).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let pool = AnyPool::connect(&format!("sqlite:{}?mode=rwc", dsn)).await?;
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await
            .ok();
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&pool)
            .await
            .ok();
        Ok(pool)
    } else {
        let pool = AnyPool::connect(dsn).await?;
        Ok(pool)
    }
}
