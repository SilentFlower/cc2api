use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 新账号默认允许透传 `context-1m-2025-08-07` 的模型白名单。
pub const DEFAULT_ALLOW_1M_MODELS: &str = "opus,claude-sonnet-5";
/// 上游 session 池默认容量；只有账号显式开启时生效。
pub const DEFAULT_UPSTREAM_SESSION_POOL_SIZE: i32 = 3;
/// 上游 session 池容量上限。
pub const MAX_UPSTREAM_SESSION_POOL_SIZE: i32 = 20;
/// 上游 session 池默认活跃 TTL（分钟）。
pub const DEFAULT_UPSTREAM_SESSION_TTL_MINUTES: i32 = 60;
/// 上游 session 池最小活跃 TTL（分钟）。
pub const MIN_UPSTREAM_SESSION_TTL_MINUTES: i32 = 5;
/// 上游 session 池最大活跃 TTL（分钟）。
pub const MAX_UPSTREAM_SESSION_TTL_MINUTES: i32 = 1440;
/// 上游 session 池默认 TTL 刷新策略。
pub const DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY: &str = "mapped_request";

mod optional_timestamp_millis {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(dt) => serializer.serialize_i64(dt.timestamp_millis()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<i64>::deserialize(deserializer)?;
        value
            .map(|ms| {
                Utc.timestamp_millis_opt(ms)
                    .single()
                    .ok_or_else(|| serde::de::Error::custom("invalid timestamp millis"))
            })
            .transpose()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AccountStatus {
    Active,
    Error,
    Disabled,
}

impl Default for AccountStatus {
    fn default() -> Self {
        Self::Active
    }
}

impl std::fmt::Display for AccountStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Error => write!(f, "error"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

impl From<String> for AccountStatus {
    fn from(s: String) -> Self {
        match s.as_str() {
            "active" => Self::Active,
            "error" => Self::Error,
            "disabled" => Self::Disabled,
            _ => Self::Active,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BillingMode {
    Strip,
    Rewrite,
}

impl Default for BillingMode {
    fn default() -> Self {
        Self::Strip
    }
}

impl std::fmt::Display for BillingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strip => write!(f, "strip"),
            Self::Rewrite => write!(f, "rewrite"),
        }
    }
}

impl From<String> for BillingMode {
    fn from(s: String) -> Self {
        match s.as_str() {
            "rewrite" => Self::Rewrite,
            _ => Self::Strip,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AccountAuthType {
    SetupToken,
    Oauth,
}

/// 真实 CLI 重授权产生的受控凭据导入；不允许顺便修改账号环境。
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthCredentialImport {
    pub operation_id: String,
    pub owner: String,
    pub generation: String,
    pub email: String,
    pub account_uuid: Option<String>,
    pub organization_uuid: Option<String>,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
}

impl Default for AccountAuthType {
    fn default() -> Self {
        Self::SetupToken
    }
}

impl std::fmt::Display for AccountAuthType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SetupToken => write!(f, "setup_token"),
            Self::Oauth => write!(f, "oauth"),
        }
    }
}

impl From<String> for AccountAuthType {
    fn from(s: String) -> Self {
        match s.as_str() {
            "oauth" => Self::Oauth,
            _ => Self::SetupToken,
        }
    }
}

/// 网关账号的身份、运行配置、凭据和最近观测用量。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub status: AccountStatus,
    #[serde(default)]
    pub auth_type: AccountAuthType,
    #[serde(default)]
    pub setup_token: String,
    #[serde(default)]
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default, with = "optional_timestamp_millis")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oauth_refreshed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub auth_error: String,
    #[serde(default)]
    pub proxy_url: String,
    pub device_id: String,
    pub canonical_env: Value,
    #[serde(rename = "canonical_prompt_env")]
    pub canonical_prompt: Value,
    pub canonical_process: Value,
    pub billing_mode: BillingMode,
    /// OAuth account UUID（强烈推荐填写，用于遥测改写）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_uuid: Option<String>,
    /// OAuth organization UUID（强烈推荐填写，用于遥测改写）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_uuid: Option<String>,
    /// 订阅类型：max / pro / team / enterprise（强烈推荐填写，用于遥测改写）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_type: Option<String>,
    #[serde(default = "default_concurrency")]
    pub concurrency: i32,
    #[serde(default = "default_priority")]
    pub priority: i32,
    /// 账号级每分钟请求数上限，0 表示不限制。
    #[serde(default)]
    pub rpm_limit: i32,
    /// 账号 5 小时用量百分比上限，0 表示不限制，启用时为 1～100。
    #[serde(default)]
    pub five_hour_usage_limit_percent: i32,
    /// 账号 7 天用量百分比上限，0 表示不限制，启用时为 1～100。
    #[serde(default)]
    pub seven_day_usage_limit_percent: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limited_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_reset_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub disable_reason: String,
    /// 是否启用自动遥测。
    #[serde(default)]
    pub auto_telemetry: bool,
    /// 是否启用后台自动轮询用量数据（仅 OAuth 账号有效）。
    #[serde(default)]
    pub auto_poll_usage: bool,
    /// 允许透传 `context-1m-2025-08-07` beta 的模型模式列表，逗号分隔，大小写不敏感，
    /// 匹配方式为"子串包含"。留空 = 不放行（所有模型都 filter 掉 context-1m）。
    /// 默认 `"opus,claude-sonnet-5"`，与 2.1.197 Sonnet 5 抓包一致；
    /// 不使用宽泛 `"sonnet"`，避免误放行 Sonnet 4.6。
    #[serde(default = "default_allow_1m_models")]
    pub allow_1m_models: String,
    /// 是否允许客户端申请 Fast；默认禁止，同时移除根级 `speed: "fast"` 和 Fast beta，回落普通模式。
    #[serde(default)]
    pub allow_fast_mode: bool,
    /// 是否启用账号级上游 session 池；默认关闭，保持旧协议行为。
    #[serde(default)]
    pub upstream_session_pool_enabled: bool,
    /// 上游 session 池容量；`0` 等价关闭，启用时有效范围为 `1..=20`。
    #[serde(default = "default_upstream_session_pool_size")]
    pub upstream_session_pool_size: i32,
    /// 上游 session 池活跃 TTL（分钟），有效范围为 `5..=1440`。
    #[serde(default = "default_upstream_session_ttl_minutes")]
    pub upstream_session_ttl_minutes: i32,
    /// 上游 session 池 TTL 刷新策略：`mapped_request` 或 `owner_only`。
    #[serde(default = "default_upstream_session_refresh_policy")]
    pub upstream_session_refresh_policy: String,
    /// 累计发送的遥测请求次数。
    #[serde(default)]
    pub telemetry_count: i64,
    #[serde(default)]
    pub usage_data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage_fetched_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

fn default_concurrency() -> i32 {
    3
}
fn default_priority() -> i32 {
    50
}
fn default_allow_1m_models() -> String {
    DEFAULT_ALLOW_1M_MODELS.to_string()
}
fn default_upstream_session_pool_size() -> i32 {
    DEFAULT_UPSTREAM_SESSION_POOL_SIZE
}
fn default_upstream_session_ttl_minutes() -> i32 {
    DEFAULT_UPSTREAM_SESSION_TTL_MINUTES
}
fn default_upstream_session_refresh_policy() -> String {
    DEFAULT_UPSTREAM_SESSION_REFRESH_POLICY.to_string()
}

impl Account {
    pub fn is_schedulable(&self) -> bool {
        if self.status != AccountStatus::Active {
            return false;
        }
        if let Some(reset) = self.rate_limit_reset_at {
            if Utc::now() < reset {
                return false;
            }
        }
        true
    }

    pub fn has_valid_oauth_access_token(&self, buffer_seconds: i64) -> bool {
        if self.auth_type != AccountAuthType::Oauth || self.access_token.is_empty() {
            return false;
        }
        self.expires_at
            .map(|expires_at| expires_at > Utc::now() + chrono::Duration::seconds(buffer_seconds))
            .unwrap_or(false)
    }
}

fn default_true() -> bool {
    true
}

/// 存储 20+ 维度的环境指纹数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalEnvData {
    pub platform: String,
    pub platform_raw: String,
    pub arch: String,
    pub node_version: String,
    pub terminal: String,
    pub package_managers: String,
    pub runtimes: String,
    #[serde(default = "default_true")]
    pub is_running_with_bun: bool,
    #[serde(default)]
    pub is_ci: bool,
    #[serde(default)]
    pub is_claubbit: bool,
    #[serde(default)]
    pub is_claude_code_remote: bool,
    #[serde(default)]
    pub is_local_agent_mode: bool,
    #[serde(default)]
    pub is_conductor: bool,
    #[serde(default)]
    pub is_github_action: bool,
    #[serde(default)]
    pub is_claude_code_action: bool,
    #[serde(default)]
    pub is_claude_ai_auth: bool,
    pub version: String,
    pub version_base: String,
    pub build_time: String,
    pub deployment_environment: String,
    pub vcs: String,
    #[serde(default)]
    pub linux_distro_id: String,
    #[serde(default)]
    pub linux_distro_version: String,
    #[serde(default)]
    pub linux_kernel: String,
}

impl Default for CanonicalEnvData {
    fn default() -> Self {
        Self {
            platform: String::new(),
            platform_raw: String::new(),
            arch: String::new(),
            node_version: String::new(),
            terminal: String::new(),
            package_managers: String::new(),
            runtimes: String::new(),
            is_running_with_bun: true,
            is_ci: false,
            is_claubbit: false,
            is_claude_code_remote: false,
            is_local_agent_mode: false,
            is_conductor: false,
            is_github_action: false,
            is_claude_code_action: false,
            is_claude_ai_auth: false,
            version: String::new(),
            version_base: String::new(),
            build_time: String::new(),
            deployment_environment: String::new(),
            vcs: String::new(),
            linux_distro_id: String::new(),
            linux_distro_version: String::new(),
            linux_kernel: String::new(),
        }
    }
}

/// 系统提示词中的环境改写数据。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CanonicalPromptEnvData {
    pub platform: String,
    pub shell: String,
    pub os_version: String,
    pub working_dir: String,
}

/// 硬件指纹配置。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct CanonicalProcessData {
    pub constrained_memory: i64,
    pub rss_range: [i64; 2],
    pub heap_total_range: [i64; 2],
    pub heap_used_range: [i64; 2],
    #[serde(default)]
    pub external_range: [i64; 2],
    #[serde(default)]
    pub array_buffers_range: [i64; 2],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observation: Option<CliProcessObservation>,
}

/// v3 官方 CLI 的只读实测来源；空 RSS 明确表示尚未采到进程。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliProcessObservation {
    pub source: String,
    pub environment_id: String,
    pub cli_version: String,
    pub observed_at: f64,
    pub sampled_at: Option<f64>,
    pub sample_count: u64,
    pub rss_bytes: Option<i64>,
    pub rss_min_bytes: Option<i64>,
    pub rss_max_bytes: Option<i64>,
}

impl CanonicalProcessData {
    /// 验证实测数据的环境、版本、时间和值域，不为未知字段生成默认值。
    /// @param environment_id 当前固定环境。
    /// @param version 当前官方 CLI 版本。
    /// @return 实测来源及样本完整性均通过时返回 true。
    pub fn has_valid_cli_observation(&self, environment_id: &str, version: &str) -> bool {
        let Some(observed) = &self.observation else {
            return false;
        };
        let now = Utc::now().timestamp_millis() as f64 / 1000.0;
        if observed.source != "v3:procfs:cli"
            || observed.environment_id != environment_id
            || environment_id.len() != 32
            || !environment_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || observed.cli_version != version
            || !Regex::new(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9._-]+)?$")
                .unwrap()
                .is_match(version)
            || self.constrained_memory < 0
            || self.constrained_memory >= 1_i64 << 53
            || observed.sample_count >= 1_u64 << 53
            || !observed.observed_at.is_finite()
            || observed.observed_at <= 0.0
            || observed.observed_at > now + 60.0
        {
            return false;
        }
        match (
            observed.sampled_at,
            observed.rss_bytes,
            observed.rss_min_bytes,
            observed.rss_max_bytes,
        ) {
            (None, None, None, None) => observed.sample_count == 0,
            (Some(at), Some(rss), Some(min), Some(max)) => {
                observed.sample_count > 0
                    && at.is_finite()
                    && at > 0.0
                    && at <= observed.observed_at
                    && min > 0
                    && min <= rss
                    && rss <= max
                    && max < 1_i64 << 53
            }
            _ => false,
        }
    }
}

/// 内部服务用于更新原账号进程指标的独立载荷，不接受凭据或策略字段。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CliProcessImport {
    pub device_id: String,
    pub email: String,
    pub account_uuid: String,
    pub environment_id: String,
    pub cli_version: String,
    pub canonical_process: CanonicalProcessData,
}
