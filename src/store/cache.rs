use crate::error::AppError;
use std::time::Duration;

#[axum::async_trait]
pub trait CacheStore: Send + Sync {
    async fn get_session_account_id(&self, session_hash: &str) -> Result<Option<i64>, AppError>;
    async fn set_session_account_id(
        &self,
        session_hash: &str,
        account_id: i64,
        ttl: Duration,
    ) -> Result<(), AppError>;
    async fn delete_session(&self, session_hash: &str) -> Result<(), AppError>;
    async fn acquire_slot(&self, key: &str, max: i32, ttl: Duration) -> Result<bool, AppError>;
    async fn release_slot(&self, key: &str);
    /// 获取指定 key 的当前槽位占用数（用于负载感知选择）。
    async fn get_slot_count(&self, key: &str) -> i64;
    async fn acquire_lock(
        &self,
        key: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, AppError>;
    async fn release_lock(&self, key: &str, owner: &str);
}
