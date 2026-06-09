use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::error::AppError;
use crate::store::cache::{CacheStore, RpmAcquire};

struct SessionEntry {
    account_id: i64,
    expires_at: tokio::time::Instant,
}

struct LockEntry {
    owner: String,
    expires_at: tokio::time::Instant,
}

struct RpmEntry {
    count: i64,
    expires_at: tokio::time::Instant,
}

pub struct MemoryStore {
    sessions: Mutex<HashMap<String, SessionEntry>>,
    slots: Mutex<HashMap<String, i64>>,
    locks: Mutex<HashMap<String, LockEntry>>,
    rpm: Mutex<HashMap<String, RpmEntry>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            slots: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            rpm: Mutex::new(HashMap::new()),
        }
    }
}

fn rpm_key(account_id: i64, minute_ts: i64) -> String {
    format!("rpm:{}:{}", account_id, minute_ts)
}

#[axum::async_trait]
impl CacheStore for MemoryStore {
    async fn get_session_account_id(&self, session_hash: &str) -> Result<Option<i64>, AppError> {
        let mut sessions = self.sessions.lock().await;
        let key = format!("session:{}", session_hash);
        if let Some(entry) = sessions.get(&key) {
            if tokio::time::Instant::now() > entry.expires_at {
                sessions.remove(&key);
                return Ok(None);
            }
            return Ok(Some(entry.account_id));
        }
        Ok(None)
    }

    async fn set_session_account_id(
        &self,
        session_hash: &str,
        account_id: i64,
        ttl: Duration,
    ) -> Result<(), AppError> {
        let mut sessions = self.sessions.lock().await;
        let key = format!("session:{}", session_hash);
        sessions.insert(
            key,
            SessionEntry {
                account_id,
                expires_at: tokio::time::Instant::now() + ttl,
            },
        );
        Ok(())
    }

    async fn delete_session(&self, session_hash: &str) -> Result<(), AppError> {
        let mut sessions = self.sessions.lock().await;
        sessions.remove(&format!("session:{}", session_hash));
        Ok(())
    }

    async fn acquire_slot(&self, key: &str, max: i32, _ttl: Duration) -> Result<bool, AppError> {
        let mut slots = self.slots.lock().await;
        let val = slots.entry(key.to_string()).or_insert(0);
        *val += 1;
        if *val > max as i64 {
            *val -= 1;
            return Ok(false);
        }
        Ok(true)
    }

    async fn release_slot(&self, key: &str) {
        let mut slots = self.slots.lock().await;
        if let Some(val) = slots.get_mut(key) {
            if *val > 0 {
                *val -= 1;
            }
        }
    }

    async fn get_slot_count(&self, key: &str) -> i64 {
        let slots = self.slots.lock().await;
        slots.get(key).copied().unwrap_or(0)
    }

    async fn get_account_rpm(&self, account_id: i64, minute_ts: i64) -> Result<i64, AppError> {
        let mut rpm = self.rpm.lock().await;
        let key = rpm_key(account_id, minute_ts);
        if let Some(entry) = rpm.get(&key) {
            if tokio::time::Instant::now() <= entry.expires_at {
                return Ok(entry.count);
            }
            rpm.remove(&key);
        }
        Ok(0)
    }

    async fn try_acquire_account_rpm(
        &self,
        account_id: i64,
        minute_ts: i64,
        limit: i32,
        ttl: Duration,
    ) -> Result<RpmAcquire, AppError> {
        if limit <= 0 {
            return Ok(RpmAcquire {
                acquired: true,
                current: 0,
            });
        }
        let mut rpm = self.rpm.lock().await;
        let now = tokio::time::Instant::now();
        let key = rpm_key(account_id, minute_ts);
        if rpm.get(&key).is_some_and(|entry| now > entry.expires_at) {
            rpm.remove(&key);
        }
        let entry = rpm.entry(key).or_insert(RpmEntry {
            count: 0,
            expires_at: now + ttl,
        });
        if entry.count >= limit as i64 {
            return Ok(RpmAcquire {
                acquired: false,
                current: entry.count,
            });
        }
        entry.count += 1;
        entry.expires_at = now + ttl;
        Ok(RpmAcquire {
            acquired: true,
            current: entry.count,
        })
    }

    async fn acquire_lock(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError> {
        let mut locks = self.locks.lock().await;
        let now = tokio::time::Instant::now();
        if let Some(existing) = locks.get(key) {
            if now <= existing.expires_at {
                return Ok(false);
            }
        }
        locks.insert(
            key.to_string(),
            LockEntry {
                owner: owner.to_string(),
                expires_at: now + ttl,
            },
        );
        Ok(true)
    }

    async fn release_lock(&self, key: &str, owner: &str) {
        let mut locks = self.locks.lock().await;
        if let Some(existing) = locks.get(key) {
            if existing.owner == owner {
                locks.remove(key);
            }
        }
    }
}
