use chrono::{DateTime, Utc};
use serde::Serialize;

/// 账号的本地网关用量；与官方额度观察分别保存。
#[derive(Debug, Clone, Serialize)]
pub struct RequestUsage {
    pub recorded_since: DateTime<Utc>,
    pub five_hour: WindowUsage,
    pub seven_day: WindowUsage,
}

/// 一个额度周期或滚动时间段内的请求统计。
#[derive(Debug, Clone, Serialize)]
pub struct WindowUsage {
    pub requests: i64,
    pub tokens: i64,
    pub cost_usd: f64,
    pub unpriced_requests: i64,
    pub partial_requests: i64,
    pub starts_at: DateTime<Utc>,
    pub ends_at: DateTime<Utc>,
    pub basis: &'static str,
    pub coverage_complete: bool,
    pub estimated_total_cost_usd: Option<f64>,
}
