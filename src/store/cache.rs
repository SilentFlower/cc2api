use crate::error::AppError;
use sha2::{Digest, Sha256};
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct RpmAcquire {
    pub acquired: bool,
    pub current: i64,
}

/// 下游 Session 首次 Hello 代理探测状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionHelloProbeState {
    /// 最近一次探测成功。
    Success,
    /// 最近一次探测发生网络错误或返回非 200。
    Failure,
    /// 最近一次探测超过配置的总超时。
    Timeout,
}

impl SessionHelloProbeState {
    /// 返回缓存持久化使用的稳定字符串。
    ///
    /// @return 当前状态的字符串表示。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Timeout => "timeout",
        }
    }

    /// 从缓存字符串解析探测状态。
    ///
    /// @param value 缓存中读取的原始字符串。
    /// @return 合法状态返回枚举，未知值返回 `None`。
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "failure" => Some(Self::Failure),
            "timeout" => Some(Self::Timeout),
            _ => None,
        }
    }
}

/// 上游 session 池 TTL 刷新策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamSessionRefreshPolicy {
    /// 任何映射到该 upstream session 的请求都会刷新 TTL。
    MappedRequest,
    /// 只有 upstream session 所属真实 session 自己请求时刷新 TTL。
    OwnerOnly,
}

impl UpstreamSessionRefreshPolicy {
    /// 从账号配置字符串解析刷新策略。
    ///
    /// @param value 账号配置中的策略值。
    /// @return 合法策略返回枚举，未知值返回 `None`。
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "mapped_request" => Some(Self::MappedRequest),
            "owner_only" => Some(Self::OwnerOnly),
            _ => None,
        }
    }

    /// 返回持久化和日志使用的稳定字符串。
    ///
    /// @return 当前策略的字符串表示。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MappedRequest => "mapped_request",
            Self::OwnerOnly => "owner_only",
        }
    }
}

/// 上游 session 池解析动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamSessionPoolAction {
    /// 真实 session 已是池成员。
    OwnerHit,
    /// 池未满，真实 session 被接纳为新 upstream session。
    Admitted,
    /// 池已满，真实 session 被映射到已有 upstream session。
    Mapped,
    /// 只读映射命中已有真实 session。
    LookupOwnerHit,
    /// 只读映射复用了已有 upstream session。
    LookupMapped,
    /// 只读映射时池为空。
    Empty,
}

impl UpstreamSessionPoolAction {
    /// 返回日志和测试断言使用的动作名。
    ///
    /// @return 当前动作的稳定字符串。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OwnerHit => "owner_hit",
            Self::Admitted => "admitted",
            Self::Mapped => "mapped",
            Self::LookupOwnerHit => "lookup_owner_hit",
            Self::LookupMapped => "lookup_mapped",
            Self::Empty => "empty",
        }
    }
}

/// 上游 session 池解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamSessionPoolResolve {
    /// 真实下游 Claude Code session。
    pub real_session_id: String,
    /// 最终发给上游的 session；池为空或无法映射时为 `None`。
    pub upstream_session_id: Option<String>,
    /// 本次解析动作。
    pub action: UpstreamSessionPoolAction,
    /// 懒清理后的活跃池成员数量。
    pub active_count: i64,
    /// 当前池内最早活跃时间戳（毫秒）。
    pub oldest_last_seen_ms: Option<i64>,
    /// 当前池内最晚活跃时间戳（毫秒）。
    pub newest_last_seen_ms: Option<i64>,
}

/// 上游 session 池状态，用于管理端展示。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamSessionPoolStatus {
    /// 懒清理后的活跃成员数量。
    pub active_count: i64,
    /// 当前池内最早活跃时间戳（毫秒）。
    pub oldest_last_seen_ms: Option<i64>,
    /// 当前池内最晚活跃时间戳（毫秒）。
    pub newest_last_seen_ms: Option<i64>,
}

/// 为真实 session 生成稳定哈希值，供池满时选择已有 upstream session。
///
/// @param session_id 真实下游 session。
/// @return 31-bit 正整数，避免 Redis Lua number 精度问题。
pub fn stable_upstream_session_hash(session_id: &str) -> i64 {
    let digest = Sha256::digest(session_id.as_bytes());
    i64::from(u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) & 0x7fff_ffff)
}

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
    /// 获取账号在指定分钟窗口内的 RPM 计数。
    async fn get_account_rpm(&self, account_id: i64, minute_ts: i64) -> Result<i64, AppError>;
    /// 预占一个账号 RPM 名额，超过上限时不保留递增。
    async fn try_acquire_account_rpm(
        &self,
        account_id: i64,
        minute_ts: i64,
        limit: i32,
        ttl: Duration,
    ) -> Result<RpmAcquire, AppError>;
    /// 解析账号级上游 session 池。
    ///
    /// @param account_id 账号 ID。
    /// @param real_session_id 真实下游 Claude Code session。
    /// @param pool_size 池容量；`0` 表示不入池。
    /// @param ttl 活跃 TTL。
    /// @param refresh_policy TTL 刷新策略。
    /// @param allow_insert 是否允许把真实 session 接纳为新池成员。
    /// @return 返回最终 upstream session 与池状态。
    async fn resolve_upstream_session_pool(
        &self,
        account_id: i64,
        real_session_id: &str,
        pool_size: i32,
        ttl: Duration,
        refresh_policy: UpstreamSessionRefreshPolicy,
        allow_insert: bool,
    ) -> Result<UpstreamSessionPoolResolve, AppError>;
    /// 读取账号级上游 session 池状态，并在读取前按 TTL 懒清理。
    ///
    /// @param account_id 账号 ID。
    /// @param pool_size 当前池容量，用于配置缩小时立即收敛。
    /// @param ttl 活跃 TTL。
    /// @return 返回当前活跃成员数量和时间范围。
    async fn get_upstream_session_pool_status(
        &self,
        account_id: i64,
        pool_size: i32,
        ttl: Duration,
    ) -> Result<UpstreamSessionPoolStatus, AppError>;
    /// 读取 Session Hello 探测状态；成功命中时原子续期形成滑动 TTL。
    ///
    /// @param key 已脱敏的探测状态 key。
    /// @param success_ttl 成功状态命中后续期的时长。
    /// @return 返回仍在有效期内的状态；不存在或已过期时返回 `None`。
    async fn get_session_hello_probe_state(
        &self,
        key: &str,
        success_ttl: Duration,
    ) -> Result<Option<SessionHelloProbeState>, AppError>;
    /// 写入 Session Hello 探测状态和固定有效期。
    ///
    /// @param key 已脱敏的探测状态 key。
    /// @param state 本次探测状态。
    /// @param ttl 状态有效期；成功和失败分别由调用方传入不同配置。
    /// @return 写入成功返回 `Ok(())`。
    async fn set_session_hello_probe_state(
        &self,
        key: &str,
        state: SessionHelloProbeState,
        ttl: Duration,
    ) -> Result<(), AppError>;
    async fn acquire_lock(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError>;
    async fn release_lock(&self, key: &str, owner: &str);
}
