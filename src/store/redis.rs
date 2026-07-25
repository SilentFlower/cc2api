use redis::AsyncCommands;
use std::time::Duration;

use crate::error::AppError;
use crate::store::cache::{
    CacheStore, RpmAcquire, SessionHelloProbeState, UpstreamSessionPoolAction,
    UpstreamSessionPoolResolve, UpstreamSessionPoolStatus, UpstreamSessionRefreshPolicy,
    stable_upstream_session_hash,
};

pub struct RedisStore {
    client: redis::aio::ConnectionManager,
}

/// Build a Redis connection URL from discrete components.
///
/// Kept as a pure function so it can be unit-tested without a live Redis server.
/// The database number is encoded exactly once as the URL path.
pub fn build_redis_url(host: &str, port: u16, password: &str, db: i64) -> String {
    if password.is_empty() {
        format!("redis://{}:{}/{}", host, port, db)
    } else {
        format!("redis://:{}@{}:{}/{}", password, host, port, db)
    }
}

impl RedisStore {
    pub async fn new(host: &str, port: u16, password: &str, db: i64) -> Result<Self, AppError> {
        let url = build_redis_url(host, port, password, db);
        let client = redis::Client::open(url)
            .map_err(|e| AppError::Internal(format!("redis open: {}", e)))?;
        let mgr = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(|e| AppError::Internal(format!("redis connect: {}", e)))?;
        Ok(Self { client: mgr })
    }
}

fn rpm_key(account_id: i64, minute_ts: i64) -> String {
    format!("rpm:{}:{}", account_id, minute_ts)
}

fn upstream_session_pool_key(account_id: i64) -> String {
    format!("upstream_session_pool:{}", account_id)
}

fn upstream_session_pool_mapping_key(account_id: i64) -> String {
    format!("upstream_session_pool_mapping:{}", account_id)
}

fn upstream_session_pool_mapping_seen_key(account_id: i64) -> String {
    format!("upstream_session_pool_mapping_seen:{}", account_id)
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn parse_optional_i64(value: Option<&String>) -> Option<i64> {
    value.and_then(|value| value.parse::<i64>().ok())
}

fn parse_upstream_session_action(action: &str) -> UpstreamSessionPoolAction {
    match action {
        "owner_hit" => UpstreamSessionPoolAction::OwnerHit,
        "admitted" => UpstreamSessionPoolAction::Admitted,
        "mapped" => UpstreamSessionPoolAction::Mapped,
        "lookup_owner_hit" => UpstreamSessionPoolAction::LookupOwnerHit,
        "lookup_mapped" => UpstreamSessionPoolAction::LookupMapped,
        _ => UpstreamSessionPoolAction::Empty,
    }
}

#[axum::async_trait]
impl CacheStore for RedisStore {
    async fn get_session_account_id(&self, session_hash: &str) -> Result<Option<i64>, AppError> {
        let key = format!("session:{}", session_hash);
        let val: Option<String> = self
            .client
            .clone()
            .get(&key)
            .await
            .map_err(|e| AppError::Internal(format!("redis get: {}", e)))?;
        match val {
            Some(s) => {
                let id = s
                    .parse::<i64>()
                    .map_err(|e| AppError::Internal(format!("redis parse: {}", e)))?;
                Ok(Some(id))
            }
            None => Ok(None),
        }
    }

    async fn set_session_account_id(
        &self,
        session_hash: &str,
        account_id: i64,
        ttl: Duration,
    ) -> Result<(), AppError> {
        let key = format!("session:{}", session_hash);
        let _: () = self
            .client
            .clone()
            .set_ex(&key, account_id.to_string(), ttl.as_secs())
            .await
            .map_err(|e| AppError::Internal(format!("redis set: {}", e)))?;
        Ok(())
    }

    async fn delete_session(&self, session_hash: &str) -> Result<(), AppError> {
        let key = format!("session:{}", session_hash);
        let _: () = self
            .client
            .clone()
            .del(&key)
            .await
            .map_err(|e| AppError::Internal(format!("redis del: {}", e)))?;
        Ok(())
    }

    async fn acquire_slot(&self, key: &str, max: i32, ttl: Duration) -> Result<bool, AppError> {
        let mut conn = self.client.clone();
        let val: i64 = conn
            .incr(key, 1i64)
            .await
            .map_err(|e| AppError::Internal(format!("redis incr: {}", e)))?;
        if val == 1 {
            let _: () = conn.expire(key, ttl.as_secs() as i64).await.unwrap_or(());
        }
        if val > max as i64 {
            let _: () = conn.decr(key, 1i64).await.unwrap_or(());
            return Ok(false);
        }
        Ok(true)
    }

    async fn release_slot(&self, key: &str) {
        let _: Result<(), _> = self.client.clone().decr(key, 1i64).await;
    }

    async fn get_slot_count(&self, key: &str) -> i64 {
        self.client
            .clone()
            .get::<_, Option<i64>>(key)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    async fn get_account_rpm(&self, account_id: i64, minute_ts: i64) -> Result<i64, AppError> {
        let key = rpm_key(account_id, minute_ts);
        self.client
            .clone()
            .get::<_, Option<i64>>(key)
            .await
            .map(|v| v.unwrap_or(0))
            .map_err(|e| AppError::Internal(format!("redis rpm get: {}", e)))
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
        let key = rpm_key(account_id, minute_ts);
        let mut conn = self.client.clone();
        let val: i64 = conn
            .incr(&key, 1i64)
            .await
            .map_err(|e| AppError::Internal(format!("redis rpm incr: {}", e)))?;
        if val == 1 {
            let _: () = conn
                .expire(&key, ttl.as_secs().max(1) as i64)
                .await
                .unwrap_or(());
        }
        if val > limit as i64 {
            let current = conn
                .decr::<_, _, i64>(&key, 1i64)
                .await
                .unwrap_or(limit as i64);
            return Ok(RpmAcquire {
                acquired: false,
                current,
            });
        }
        Ok(RpmAcquire {
            acquired: true,
            current: val,
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
        let key = upstream_session_pool_key(account_id);
        let mapping_key = upstream_session_pool_mapping_key(account_id);
        let mapping_seen_key = upstream_session_pool_mapping_seen_key(account_id);
        let now_ms = now_millis();
        let ttl_ms = ttl.as_millis().max(1).min(i64::MAX as u128) as i64;
        let expire_ms = ttl_ms.saturating_mul(2).max(1000);
        let hash = stable_upstream_session_hash(real_session_id);
        let mut conn = self.client.clone();
        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local mapping_key = KEYS[2]
            local mapping_seen_key = KEYS[3]
            local real = ARGV[1]
            local pool_size = tonumber(ARGV[2])
            local now_ms = tonumber(ARGV[3])
            local ttl_ms = tonumber(ARGV[4])
            local policy = ARGV[5]
            local allow_insert = ARGV[6] == "1"
            local hash_value = tonumber(ARGV[7])
            local expire_ms = tonumber(ARGV[8])
            local cutoff = now_ms - ttl_ms

            redis.call("ZREMRANGEBYSCORE", key, "-inf", cutoff)
            local expired_mappings = redis.call("ZRANGEBYSCORE", mapping_seen_key, "-inf", cutoff)
            for _, expired_real in ipairs(expired_mappings) do
                redis.call("HDEL", mapping_key, expired_real)
            end
            redis.call("ZREMRANGEBYSCORE", mapping_seen_key, "-inf", cutoff)

            local count = redis.call("ZCARD", key)
            local excess = count - pool_size
            if excess > 0 then
                local evicted = redis.call("ZRANGE", key, 0, excess - 1)
                for _, evicted_session in ipairs(evicted) do
                    redis.call("ZREM", key, evicted_session)
                end
            end

            local function stats()
                local with_scores = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
                local count = #with_scores / 2
                local oldest = ""
                local newest = ""
                for i = 2, #with_scores, 2 do
                    local score = tonumber(with_scores[i])
                    if oldest == "" or score < tonumber(oldest) then
                        oldest = tostring(score)
                    end
                    if newest == "" or score > tonumber(newest) then
                        newest = tostring(score)
                    end
                end
                return tostring(count), oldest, newest
            end

            local action = "empty"
            local upstream = ""
            local exists = redis.call("ZSCORE", key, real)
            local mapped = redis.call("HGET", mapping_key, real)
            local mapped_seen = redis.call("ZSCORE", mapping_seen_key, real)
            if mapped and (not mapped_seen or not redis.call("ZSCORE", key, mapped)) then
                redis.call("HDEL", mapping_key, real)
                redis.call("ZREM", mapping_seen_key, real)
                mapped = false
            end
            count = redis.call("ZCARD", key)
            if exists then
                upstream = real
                if allow_insert then
                    redis.call("ZADD", key, now_ms, real)
                    redis.call("HSET", mapping_key, real, real)
                    redis.call("ZADD", mapping_seen_key, now_ms, real)
                    action = "owner_hit"
                else
                    action = "lookup_owner_hit"
                end
            elseif mapped then
                upstream = mapped
                if allow_insert then
                    if policy == "mapped_request" then
                        redis.call("ZADD", key, now_ms, upstream)
                    end
                    redis.call("ZADD", mapping_seen_key, now_ms, real)
                    action = "mapped"
                else
                    action = "lookup_mapped"
                end
            elseif allow_insert and pool_size > 0 and count < pool_size then
                redis.call("ZADD", key, now_ms, real)
                redis.call("HSET", mapping_key, real, real)
                redis.call("ZADD", mapping_seen_key, now_ms, real)
                upstream = real
                action = "admitted"
            elseif allow_insert and count > 0 then
                local members = redis.call("ZRANGE", key, 0, -1)
                table.sort(members)
                local idx = (hash_value % #members) + 1
                upstream = members[idx]
                if policy == "mapped_request" then
                    redis.call("ZADD", key, now_ms, upstream)
                end
                redis.call("HSET", mapping_key, real, upstream)
                redis.call("ZADD", mapping_seen_key, now_ms, real)
                action = "mapped"
            end

            if redis.call("ZCARD", key) > 0 then
                redis.call("PEXPIRE", key, expire_ms)
                redis.call("PEXPIRE", mapping_key, expire_ms)
                redis.call("PEXPIRE", mapping_seen_key, expire_ms)
            else
                redis.call("DEL", key, mapping_key, mapping_seen_key)
            end

            local active_count, oldest, newest = stats()
            return {upstream, action, active_count, oldest, newest}
            "#,
        );
        let values: Vec<String> = script
            .key(&key)
            .key(&mapping_key)
            .key(&mapping_seen_key)
            .arg(real_session_id)
            .arg(pool_size.max(0))
            .arg(now_ms)
            .arg(ttl_ms)
            .arg(refresh_policy.as_str())
            .arg(if allow_insert { "1" } else { "0" })
            .arg(hash)
            .arg(expire_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| AppError::Internal(format!("redis upstream session pool: {}", e)))?;

        let upstream = values.first().filter(|value| !value.is_empty()).cloned();
        let action = values
            .get(1)
            .map(|value| parse_upstream_session_action(value))
            .unwrap_or(UpstreamSessionPoolAction::Empty);
        Ok(UpstreamSessionPoolResolve {
            real_session_id: real_session_id.to_string(),
            upstream_session_id: upstream,
            action,
            active_count: parse_optional_i64(values.get(2)).unwrap_or(0),
            oldest_last_seen_ms: parse_optional_i64(values.get(3)),
            newest_last_seen_ms: parse_optional_i64(values.get(4)),
        })
    }

    async fn get_upstream_session_pool_status(
        &self,
        account_id: i64,
        pool_size: i32,
        ttl: Duration,
    ) -> Result<UpstreamSessionPoolStatus, AppError> {
        let key = upstream_session_pool_key(account_id);
        let mapping_key = upstream_session_pool_mapping_key(account_id);
        let mapping_seen_key = upstream_session_pool_mapping_seen_key(account_id);
        let now_ms = now_millis();
        let ttl_ms = ttl.as_millis().max(1).min(i64::MAX as u128) as i64;
        let expire_ms = ttl_ms.saturating_mul(2).max(1000);
        let mut conn = self.client.clone();
        let script = redis::Script::new(
            r#"
            local key = KEYS[1]
            local mapping_key = KEYS[2]
            local mapping_seen_key = KEYS[3]
            local cutoff = tonumber(ARGV[1]) - tonumber(ARGV[2])
            local pool_size = tonumber(ARGV[3])
            local expire_ms = tonumber(ARGV[4])
            redis.call("ZREMRANGEBYSCORE", key, "-inf", cutoff)
            local expired_mappings = redis.call("ZRANGEBYSCORE", mapping_seen_key, "-inf", cutoff)
            for _, expired_real in ipairs(expired_mappings) do
                redis.call("HDEL", mapping_key, expired_real)
            end
            redis.call("ZREMRANGEBYSCORE", mapping_seen_key, "-inf", cutoff)
            local current_count = redis.call("ZCARD", key)
            local excess = current_count - pool_size
            if excess > 0 then
                local evicted = redis.call("ZRANGE", key, 0, excess - 1)
                for _, evicted_session in ipairs(evicted) do
                    redis.call("ZREM", key, evicted_session)
                end
            end
            local with_scores = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
            local count = #with_scores / 2
            local oldest = ""
            local newest = ""
            for i = 2, #with_scores, 2 do
                local score = tonumber(with_scores[i])
                if oldest == "" or score < tonumber(oldest) then
                    oldest = tostring(score)
                end
                if newest == "" or score > tonumber(newest) then
                    newest = tostring(score)
                end
            end
            if count == 0 then
                redis.call("DEL", key, mapping_key, mapping_seen_key)
            else
                redis.call("PEXPIRE", key, expire_ms)
                redis.call("PEXPIRE", mapping_key, expire_ms)
                redis.call("PEXPIRE", mapping_seen_key, expire_ms)
            end
            return {tostring(count), oldest, newest}
            "#,
        );
        let values: Vec<String> = script
            .key(&key)
            .key(&mapping_key)
            .key(&mapping_seen_key)
            .arg(now_ms)
            .arg(ttl_ms)
            .arg(pool_size.max(0))
            .arg(expire_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| {
                AppError::Internal(format!("redis upstream session pool status: {}", e))
            })?;
        Ok(UpstreamSessionPoolStatus {
            active_count: parse_optional_i64(values.first()).unwrap_or(0),
            oldest_last_seen_ms: parse_optional_i64(values.get(1)),
            newest_last_seen_ms: parse_optional_i64(values.get(2)),
        })
    }

    async fn get_session_hello_probe_state(
        &self,
        key: &str,
        success_ttl: Duration,
    ) -> Result<Option<SessionHelloProbeState>, AppError> {
        let mut conn = self.client.clone();
        let script = redis::Script::new(
            r#"
            local value = redis.call("GET", KEYS[1])
            if value == "success" then
                redis.call("EXPIRE", KEYS[1], ARGV[1])
            end
            return value
            "#,
        );
        let value: Option<String> = script
            .key(key)
            .arg(success_ttl.as_secs().max(1))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| AppError::Internal(format!("redis hello probe get: {}", e)))?;
        Ok(value.and_then(|raw| SessionHelloProbeState::parse(&raw)))
    }

    async fn set_session_hello_probe_state(
        &self,
        key: &str,
        state: SessionHelloProbeState,
        ttl: Duration,
    ) -> Result<(), AppError> {
        let _: () = self
            .client
            .clone()
            .set_ex(key, state.as_str(), ttl.as_secs().max(1))
            .await
            .map_err(|e| AppError::Internal(format!("redis hello probe set: {}", e)))?;
        Ok(())
    }

    async fn acquire_lock(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError> {
        let mut conn = self.client.clone();
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(ttl.as_secs().max(1))
            .query_async(&mut conn)
            .await
            .map_err(|e| AppError::Internal(format!("redis lock set: {}", e)))?;
        Ok(result.is_some())
    }

    async fn release_lock(&self, key: &str, owner: &str) {
        let mut conn = self.client.clone();
        let script = redis::Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            end
            return 0
            "#,
        );
        let _: Result<i32, _> = script.key(key).arg(owner).invoke_async(&mut conn).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_hello_probe_success_renews_but_failure_does_not_when_redis_available() {
        let Ok(port) = std::env::var("CC2API_TEST_REDIS_PORT") else {
            return;
        };
        let port = port.parse::<u16>().expect("解析测试 Redis 端口");
        let store = RedisStore::new("127.0.0.1", port, "", 15)
            .await
            .expect("连接测试 Redis");
        let success_key = format!("test:hello:success:{}", uuid::Uuid::new_v4());
        let failure_key = format!("test:hello:failure:{}", uuid::Uuid::new_v4());

        store
            .set_session_hello_probe_state(
                &success_key,
                SessionHelloProbeState::Success,
                Duration::from_secs(1),
            )
            .await
            .expect("写入成功状态");
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            store
                .get_session_hello_probe_state(&success_key, Duration::from_secs(2))
                .await
                .expect("续期成功状态"),
            Some(SessionHelloProbeState::Success)
        );
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            store
                .get_session_hello_probe_state(&success_key, Duration::from_secs(2))
                .await
                .expect("读取续期后的成功状态"),
            Some(SessionHelloProbeState::Success)
        );

        store
            .set_session_hello_probe_state(
                &failure_key,
                SessionHelloProbeState::Failure,
                Duration::from_secs(1),
            )
            .await
            .expect("写入失败状态");
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            store
                .get_session_hello_probe_state(&failure_key, Duration::from_secs(2))
                .await
                .expect("读取失败状态"),
            Some(SessionHelloProbeState::Failure)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            store
                .get_session_hello_probe_state(&failure_key, Duration::from_secs(2))
                .await
                .expect("确认失败状态未续期"),
            None
        );

        let _: Result<(), _> = store.client.clone().del(&success_key).await;
        let _: Result<(), _> = store.client.clone().del(&failure_key).await;
    }
}
