use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::error::AppError;
use crate::store::cache::{
    CacheStore, RpmAcquire, UpstreamSessionPoolAction, UpstreamSessionPoolResolve,
    UpstreamSessionPoolStatus, UpstreamSessionRefreshPolicy, stable_upstream_session_hash,
};

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

#[derive(Default)]
struct UpstreamSessionPoolState {
    members: HashMap<String, i64>,
    mappings: HashMap<String, UpstreamSessionMapping>,
}

struct UpstreamSessionMapping {
    upstream_session_id: String,
    last_seen_ms: i64,
}

pub struct MemoryStore {
    sessions: Mutex<HashMap<String, SessionEntry>>,
    slots: Mutex<HashMap<String, i64>>,
    locks: Mutex<HashMap<String, LockEntry>>,
    rpm: Mutex<HashMap<String, RpmEntry>>,
    upstream_session_pools: Mutex<HashMap<i64, UpstreamSessionPoolState>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            slots: Mutex::new(HashMap::new()),
            locks: Mutex::new(HashMap::new()),
            rpm: Mutex::new(HashMap::new()),
            upstream_session_pools: Mutex::new(HashMap::new()),
        }
    }
}

fn rpm_key(account_id: i64, minute_ts: i64) -> String {
    format!("rpm:{}:{}", account_id, minute_ts)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn pool_time_bounds(members: &HashMap<String, i64>) -> (Option<i64>, Option<i64>) {
    let oldest = members.values().copied().min();
    let newest = members.values().copied().max();
    (oldest, newest)
}

fn cleanup_upstream_session_pool(pool: &mut UpstreamSessionPoolState, pool_size: i32, cutoff: i64) {
    pool.members.retain(|_, last_seen| *last_seen > cutoff);
    pool.mappings
        .retain(|_, mapping| mapping.last_seen_ms > cutoff);

    let capacity = pool_size.max(0) as usize;
    if pool.members.len() > capacity {
        let mut oldest = pool
            .members
            .iter()
            .map(|(session_id, last_seen)| (session_id.clone(), *last_seen))
            .collect::<Vec<_>>();
        oldest.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        for (session_id, _) in oldest.into_iter().take(pool.members.len() - capacity) {
            pool.members.remove(&session_id);
        }
    }

    // 映射目标一旦过期或被缩容淘汰，就不能继续作为只读遥测结果返回。
    pool.mappings
        .retain(|_, mapping| pool.members.contains_key(&mapping.upstream_session_id));
}

fn save_upstream_session_mapping(
    pool: &mut UpstreamSessionPoolState,
    real_session_id: &str,
    upstream_session_id: &str,
    now: i64,
) {
    pool.mappings.insert(
        real_session_id.to_string(),
        UpstreamSessionMapping {
            upstream_session_id: upstream_session_id.to_string(),
            last_seen_ms: now,
        },
    );
}

fn resolve_pool_from_map(
    pool: &mut UpstreamSessionPoolState,
    real_session_id: &str,
    pool_size: i32,
    ttl: Duration,
    refresh_policy: UpstreamSessionRefreshPolicy,
    allow_insert: bool,
) -> UpstreamSessionPoolResolve {
    let now = now_millis();
    let ttl_ms = ttl.as_millis().min(i64::MAX as u128) as i64;
    let cutoff = now.saturating_sub(ttl_ms);
    cleanup_upstream_session_pool(pool, pool_size, cutoff);

    let action;
    let upstream_session_id;
    if pool.members.contains_key(real_session_id) {
        if allow_insert {
            pool.members.insert(real_session_id.to_string(), now);
            save_upstream_session_mapping(pool, real_session_id, real_session_id, now);
            action = UpstreamSessionPoolAction::OwnerHit;
        } else {
            action = UpstreamSessionPoolAction::LookupOwnerHit;
        }
        upstream_session_id = Some(real_session_id.to_string());
    } else if let Some(mapped_session_id) = pool
        .mappings
        .get(real_session_id)
        .map(|mapping| mapping.upstream_session_id.clone())
    {
        if allow_insert {
            if refresh_policy == UpstreamSessionRefreshPolicy::MappedRequest {
                pool.members.insert(mapped_session_id.clone(), now);
            }
            save_upstream_session_mapping(pool, real_session_id, &mapped_session_id, now);
            action = UpstreamSessionPoolAction::Mapped;
        } else {
            action = UpstreamSessionPoolAction::LookupMapped;
        }
        upstream_session_id = Some(mapped_session_id);
    } else if allow_insert && pool_size > 0 && pool.members.len() < pool_size as usize {
        pool.members.insert(real_session_id.to_string(), now);
        save_upstream_session_mapping(pool, real_session_id, real_session_id, now);
        action = UpstreamSessionPoolAction::Admitted;
        upstream_session_id = Some(real_session_id.to_string());
    } else if !allow_insert || pool.members.is_empty() {
        action = UpstreamSessionPoolAction::Empty;
        upstream_session_id = None;
    } else {
        let mut members = pool.members.keys().cloned().collect::<Vec<_>>();
        members.sort();
        let idx = (stable_upstream_session_hash(real_session_id) as usize) % members.len();
        let selected = members[idx].clone();
        if refresh_policy == UpstreamSessionRefreshPolicy::MappedRequest {
            pool.members.insert(selected.clone(), now);
        }
        save_upstream_session_mapping(pool, real_session_id, &selected, now);
        action = UpstreamSessionPoolAction::Mapped;
        upstream_session_id = Some(selected);
    }

    let (oldest_last_seen_ms, newest_last_seen_ms) = pool_time_bounds(&pool.members);
    UpstreamSessionPoolResolve {
        real_session_id: real_session_id.to_string(),
        upstream_session_id,
        action,
        active_count: pool.members.len() as i64,
        oldest_last_seen_ms,
        newest_last_seen_ms,
    }
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

    async fn resolve_upstream_session_pool(
        &self,
        account_id: i64,
        real_session_id: &str,
        pool_size: i32,
        ttl: Duration,
        refresh_policy: UpstreamSessionRefreshPolicy,
        allow_insert: bool,
    ) -> Result<UpstreamSessionPoolResolve, AppError> {
        let mut pools = self.upstream_session_pools.lock().await;
        let pool = pools.entry(account_id).or_default();
        Ok(resolve_pool_from_map(
            pool,
            real_session_id,
            pool_size,
            ttl,
            refresh_policy,
            allow_insert,
        ))
    }

    async fn get_upstream_session_pool_status(
        &self,
        account_id: i64,
        pool_size: i32,
        ttl: Duration,
    ) -> Result<UpstreamSessionPoolStatus, AppError> {
        let mut pools = self.upstream_session_pools.lock().await;
        let Some(pool) = pools.get_mut(&account_id) else {
            return Ok(UpstreamSessionPoolStatus {
                active_count: 0,
                oldest_last_seen_ms: None,
                newest_last_seen_ms: None,
            });
        };
        let now = now_millis();
        let ttl_ms = ttl.as_millis().min(i64::MAX as u128) as i64;
        let cutoff = now.saturating_sub(ttl_ms);
        cleanup_upstream_session_pool(pool, pool_size, cutoff);
        let (oldest_last_seen_ms, newest_last_seen_ms) = pool_time_bounds(&pool.members);
        Ok(UpstreamSessionPoolStatus {
            active_count: pool.members.len() as i64,
            oldest_last_seen_ms,
            newest_last_seen_ms,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upstream_session_pool_caps_and_maps_stably() {
        let store = MemoryStore::new();
        let ttl = Duration::from_secs(60);

        let first = store
            .resolve_upstream_session_pool(
                1,
                "real-a",
                2,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("first resolve");
        let second = store
            .resolve_upstream_session_pool(
                1,
                "real-b",
                2,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("second resolve");
        let mapped = store
            .resolve_upstream_session_pool(
                1,
                "real-c",
                2,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("mapped resolve");
        let mapped_again = store
            .resolve_upstream_session_pool(
                1,
                "real-c",
                2,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("mapped again");

        assert_eq!(first.action, UpstreamSessionPoolAction::Admitted);
        assert_eq!(second.action, UpstreamSessionPoolAction::Admitted);
        assert_eq!(mapped.action, UpstreamSessionPoolAction::Mapped);
        assert_eq!(mapped.active_count, 2);
        assert_eq!(mapped.upstream_session_id, mapped_again.upstream_session_id);
        assert!(matches!(
            mapped.upstream_session_id.as_deref(),
            Some("real-a" | "real-b")
        ));
    }

    #[tokio::test]
    async fn upstream_session_pool_lookup_does_not_create_member() {
        let store = MemoryStore::new();
        let resolved = store
            .resolve_upstream_session_pool(
                1,
                "telemetry-only",
                3,
                Duration::from_secs(60),
                UpstreamSessionRefreshPolicy::MappedRequest,
                false,
            )
            .await
            .expect("lookup resolve");

        assert_eq!(resolved.action, UpstreamSessionPoolAction::Empty);
        assert_eq!(resolved.upstream_session_id, None);
        assert_eq!(resolved.active_count, 0);
    }

    #[tokio::test]
    async fn upstream_session_pool_mapped_request_refreshes_borrowed_member() {
        let store = MemoryStore::new();
        let ttl = Duration::from_millis(80);
        store
            .resolve_upstream_session_pool(
                1,
                "owner",
                1,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("owner resolve");
        tokio::time::sleep(Duration::from_millis(40)).await;
        store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                1,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("borrower resolve");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = store
            .get_upstream_session_pool_status(1, 1, ttl)
            .await
            .expect("status");
        assert_eq!(status.active_count, 1);
    }

    #[tokio::test]
    async fn upstream_session_pool_owner_only_does_not_refresh_borrowed_member() {
        let store = MemoryStore::new();
        let ttl = Duration::from_millis(80);
        store
            .resolve_upstream_session_pool(
                1,
                "owner",
                1,
                ttl,
                UpstreamSessionRefreshPolicy::OwnerOnly,
                true,
            )
            .await
            .expect("owner resolve");
        tokio::time::sleep(Duration::from_millis(40)).await;
        store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                1,
                ttl,
                UpstreamSessionRefreshPolicy::OwnerOnly,
                true,
            )
            .await
            .expect("borrower resolve");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = store
            .get_upstream_session_pool_status(1, 1, ttl)
            .await
            .expect("status");
        assert_eq!(status.active_count, 0);
    }

    #[tokio::test]
    async fn upstream_session_pool_shrink_evicts_oldest_member_and_invalidates_mapping() {
        let store = MemoryStore::new();
        let now = now_millis();
        {
            let mut pools = store.upstream_session_pools.lock().await;
            let pool = pools.entry(1).or_default();
            pool.members.insert("oldest".into(), now - 30);
            pool.members.insert("middle".into(), now - 20);
            pool.members.insert("newest".into(), now - 10);
            pool.mappings.insert(
                "borrower".into(),
                UpstreamSessionMapping {
                    upstream_session_id: "oldest".into(),
                    last_seen_ms: now,
                },
            );
        }

        let status = store
            .get_upstream_session_pool_status(1, 2, Duration::from_secs(60))
            .await
            .expect("shrink status");
        let lookup = store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                2,
                Duration::from_secs(60),
                UpstreamSessionRefreshPolicy::MappedRequest,
                false,
            )
            .await
            .expect("lookup after shrink");

        assert_eq!(status.active_count, 2);
        assert_eq!(lookup.upstream_session_id, None);
        let pools = store.upstream_session_pools.lock().await;
        let pool = pools.get(&1).expect("pool");
        assert!(!pool.members.contains_key("oldest"));
        assert!(pool.members.contains_key("middle"));
        assert!(pool.members.contains_key("newest"));
        assert!(!pool.mappings.contains_key("borrower"));
    }

    #[tokio::test]
    async fn upstream_session_pool_keeps_saved_mapping_when_other_members_change() {
        let store = MemoryStore::new();
        let ttl = Duration::from_secs(60);
        for owner in ["owner-a", "owner-b"] {
            store
                .resolve_upstream_session_pool(
                    1,
                    owner,
                    2,
                    ttl,
                    UpstreamSessionRefreshPolicy::MappedRequest,
                    true,
                )
                .await
                .expect("admit owner");
        }
        let mapped = store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                2,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("map borrower");
        let selected = mapped.upstream_session_id.expect("selected member");
        {
            let mut pools = store.upstream_session_pools.lock().await;
            let pool = pools.get_mut(&1).expect("pool");
            let other = if selected == "owner-a" {
                "owner-b"
            } else {
                "owner-a"
            };
            pool.members.remove(other);
            pool.members.insert("owner-c".into(), now_millis());
        }

        let lookup = store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                2,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                false,
            )
            .await
            .expect("telemetry lookup");
        let mapped_again = store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                2,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("mapped again");

        assert_eq!(lookup.action, UpstreamSessionPoolAction::LookupMapped);
        assert_eq!(
            lookup.upstream_session_id.as_deref(),
            Some(selected.as_str())
        );
        assert_eq!(
            mapped_again.upstream_session_id.as_deref(),
            Some(selected.as_str())
        );
    }

    #[tokio::test]
    async fn upstream_session_pool_read_only_lookup_does_not_rehash_missing_mapping() {
        let store = MemoryStore::new();
        store
            .resolve_upstream_session_pool(
                1,
                "owner",
                1,
                Duration::from_secs(60),
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("admit owner");

        let lookup = store
            .resolve_upstream_session_pool(
                1,
                "telemetry-only",
                1,
                Duration::from_secs(60),
                UpstreamSessionRefreshPolicy::MappedRequest,
                false,
            )
            .await
            .expect("telemetry lookup");

        assert_eq!(lookup.action, UpstreamSessionPoolAction::Empty);
        assert_eq!(lookup.upstream_session_id, None);
    }

    #[tokio::test]
    async fn upstream_session_pool_read_only_lookup_does_not_refresh_member_ttl() {
        let store = MemoryStore::new();
        let ttl = Duration::from_millis(80);
        store
            .resolve_upstream_session_pool(
                1,
                "owner",
                1,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("admit owner");
        store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                1,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                true,
            )
            .await
            .expect("map borrower");
        tokio::time::sleep(Duration::from_millis(40)).await;
        store
            .resolve_upstream_session_pool(
                1,
                "borrower",
                1,
                ttl,
                UpstreamSessionRefreshPolicy::MappedRequest,
                false,
            )
            .await
            .expect("read-only lookup");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = store
            .get_upstream_session_pool_status(1, 1, ttl)
            .await
            .expect("status");
        assert_eq!(status.active_count, 0);
    }
}
