use axum::body::Body;
use axum::extract::Request;
use axum::http::{
    HeaderMap, StatusCode,
    header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING},
    response::Parts,
};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, RwLock};
use tracing::{debug, info, warn};

use crate::error::AppError;
use crate::model::account::{Account, AccountStatus};
use crate::model::api_token::ApiToken;
use crate::service::access_policy::{
    AccessPolicy, DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS, DEFAULT_ALLOWED_USER_AGENTS,
    access_policy_error_response,
};
use crate::service::account::{AccountService, QueueWaitError, RateLimitDecision};
use crate::service::rewriter::{
    CacheControlTtlRewrite, ClientType, DisabledThinkingRewrite, EnvPassthrough,
    MessageCacheControlRewrite, Rewriter, StatefulCacheCompletion, StatefulCacheUsage,
    detect_client_type, ordered_anthropic_headers,
};
use crate::service::telemetry::{
    MessageTelemetryContext, MessageTelemetryResult, TelemetryService,
};
use crate::store::settings_store::{
    DEFAULT_ALLOW_SYSTEM_ROLE_MODELS, DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS,
    DEFAULT_BOOTSTRAP_MODEL_OPTIONS_MODE, DEFAULT_CACHE_CONTROL_TTL_REWRITE,
    DEFAULT_INTERCEPT_ASSISTANT_PREFILL_ENABLED, DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
    DEFAULT_INTERCEPT_WARMUP_HAIKU_PROBE_ENABLED, DEFAULT_INTERCEPT_WARMUP_SUGGESTION_ENABLED,
    DEFAULT_INTERCEPT_WARMUP_TITLE_ENABLED, DEFAULT_LOG_429_REQUEST_BODY_LIMIT,
    DEFAULT_LOG_429_REQUEST_ENABLED, DEFAULT_MESSAGE_CACHE_CONTROL_REWRITE,
    DEFAULT_PASSTHROUGH_OS_VERSION, DEFAULT_PASSTHROUGH_SHELL, DEFAULT_PASSTHROUGH_WORKING_DIR,
    DEFAULT_REWRITE_DISABLED_THINKING_ENABLED, DEFAULT_REWRITE_DISABLED_THINKING_MODELS,
    SettingsStore,
};

const UPSTREAM_BASE: &str = "https://api.anthropic.com";
/// 账号级 FIFO 排队的最长等待时长。超时后会降级到其他账号；队列上限仍由 concurrency 控制。
const SLOT_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// TTFB（Time To First Byte）超时：从 send() 到收到响应头的最长等待时间。
/// 握手 + 发请求 + 等上游开始响应，120s 足以覆盖非流式 Opus + 扩展思考场景；超过则认为上游卡住。
const UPSTREAM_TTFB_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// 流式读间隔超时：两次 bytes_stream chunk 之间允许的最长静默时间。
/// Anthropic SSE 每几秒至少有一个 ping event，120s 无数据视为连接卡死。
/// 该超时不限制流总时长，健康长流（Opus 扩展思考等）可持续任意时间。
const UPSTREAM_STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
/// signature 错误体只用于识别上游错误类型，限制读取大小避免异常响应占用内存。
const SIGNATURE_ERROR_BODY_LIMIT: usize = 1024 * 1024;
/// 上游错误响应日志最多输出的字符数，避免异常错误体刷屏。
const UPSTREAM_ERROR_LOG_BODY_LIMIT: usize = 4096;
/// stateful usage SSE 解析保留的最大未完成行长度。
const STATEFUL_USAGE_BUFFER_LIMIT: usize = 64 * 1024;
/// stateful usage 旁路采样的最大响应体字节数。
///
/// 正常 Anthropic SSE 的 usage 会在尾部出现；这里仅在压缩响应上保留一份内部副本用于解压解析,
/// 不改变转发给 Claude Code 的原始响应字节。
const STATEFUL_USAGE_SIDE_TAP_LIMIT: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy)]
struct RateLimitResponseDecision {
    delay: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WarmupInterceptConfig {
    title_enabled: bool,
    suggestion_enabled: bool,
    haiku_probe_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AssistantPrefillInterceptConfig {
    enabled: bool,
    models: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RateLimitRequestLogConfig {
    enabled: bool,
    body_limit: usize,
}

/// Claude Code bootstrap response 的模型选项改写模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapModelOptionsMode {
    Passthrough,
    Configured,
    HideFable,
}

impl BootstrapModelOptionsMode {
    /// 从 settings 字符串解析 bootstrap 模型选项改写模式。
    ///
    /// @param raw settings 中保存的原始字符串。
    /// @return 解析成功返回模式,非法值返回业务错误。
    pub fn parse(raw: &str) -> Result<Self, AppError> {
        match raw.trim() {
            "passthrough" => Ok(Self::Passthrough),
            "configured" => Ok(Self::Configured),
            "hide_fable" => Ok(Self::HideFable),
            other => Err(AppError::BadRequest(format!(
                "'bootstrap_model_options_mode' 不支持的值: {}",
                other
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Passthrough => "passthrough",
            Self::Configured => "configured",
            Self::HideFable => "hide_fable",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct BootstrapProfileConfig {
    mode: BootstrapModelOptionsMode,
    additional_model_options: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WarmupInterceptType {
    TextTitle,
    JsonTitle,
    Suggestion,
    HaikuProbe,
}

impl WarmupInterceptType {
    fn as_str(self) -> &'static str {
        match self {
            Self::TextTitle => "text_title",
            Self::JsonTitle => "json_title",
            Self::Suggestion => "suggestion",
            Self::HaikuProbe => "haiku_probe",
        }
    }

    fn message_id(self) -> &'static str {
        match self {
            Self::TextTitle => "msg_mock_warmup",
            Self::JsonTitle => "msg_mock_title",
            Self::Suggestion => "msg_mock_suggestion",
            Self::HaikuProbe => "msg_mock_haiku_probe",
        }
    }

    fn mock_text(self) -> &'static str {
        match self {
            Self::TextTitle => "New Conversation",
            Self::JsonTitle => "{\"title\":\"New Conversation\"}",
            Self::Suggestion => "",
            Self::HaikuProbe => "#",
        }
    }

    fn stop_reason(self) -> &'static str {
        match self {
            Self::HaikuProbe => "max_tokens",
            Self::TextTitle | Self::JsonTitle | Self::Suggestion => "end_turn",
        }
    }
}

pub struct GatewayService {
    account_svc: Arc<AccountService>,
    rewriter: Arc<Rewriter>,
    telemetry_svc: Arc<TelemetryService>,
    settings_store: Arc<SettingsStore>,
    system_role_models: RwLock<Vec<String>>,
    access_policy: RwLock<AccessPolicy>,
    env_passthrough: RwLock<EnvPassthrough>,
    cache_control_ttl_rewrite: RwLock<CacheControlTtlRewrite>,
    message_cache_control_rewrite: RwLock<MessageCacheControlRewrite>,
    warmup_intercept_config: RwLock<WarmupInterceptConfig>,
    disabled_thinking_rewrite: RwLock<DisabledThinkingRewrite>,
    assistant_prefill_intercept_config: RwLock<AssistantPrefillInterceptConfig>,
    rate_limit_request_log_config: RwLock<RateLimitRequestLogConfig>,
    bootstrap_profile_config: RwLock<BootstrapProfileConfig>,
}

impl GatewayService {
    pub fn new(
        account_svc: Arc<AccountService>,
        rewriter: Arc<Rewriter>,
        telemetry_svc: Arc<TelemetryService>,
        settings_store: Arc<SettingsStore>,
    ) -> Self {
        Self {
            account_svc,
            rewriter,
            telemetry_svc,
            settings_store,
            system_role_models: RwLock::new(parse_system_role_model_list(
                DEFAULT_ALLOW_SYSTEM_ROLE_MODELS,
            )),
            access_policy: RwLock::new(
                AccessPolicy::parse(
                    DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS,
                    DEFAULT_ALLOWED_USER_AGENTS,
                )
                .expect("默认访问策略必须合法"),
            ),
            env_passthrough: RwLock::new(default_env_passthrough()),
            cache_control_ttl_rewrite: RwLock::new(default_cache_control_ttl_rewrite()),
            message_cache_control_rewrite: RwLock::new(default_message_cache_control_rewrite()),
            warmup_intercept_config: RwLock::new(default_warmup_intercept_config()),
            disabled_thinking_rewrite: RwLock::new(default_disabled_thinking_rewrite()),
            assistant_prefill_intercept_config: RwLock::new(
                default_assistant_prefill_intercept_config(),
            ),
            rate_limit_request_log_config: RwLock::new(default_rate_limit_request_log_config()),
            bootstrap_profile_config: RwLock::new(default_bootstrap_profile_config()),
        }
    }

    /// 从全局设置刷新允许 `messages[].role=system` 的模型白名单。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 失败时返回业务错误。
    pub async fn reload_system_role_models(&self) -> Result<(), AppError> {
        let raw_allowed = self
            .settings_store
            .get_value("allow_system_role_models", DEFAULT_ALLOW_SYSTEM_ROLE_MODELS)
            .await?;
        let allowed_models = parse_system_role_model_list(&raw_allowed);
        *self.system_role_models.write().await = allowed_models;
        Ok(())
    }

    /// 从全局设置刷新系统提示词环境字段的「真值透传」开关。
    ///
    /// 与访问策略/系统角色白名单一致,结果缓存在内存(`RwLock`),请求时只读缓存,
    /// 不在每次转发时查库。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 失败时返回业务错误。
    pub async fn reload_env_passthrough(&self) -> Result<(), AppError> {
        let shell = self
            .settings_store
            .get_value("passthrough_shell", DEFAULT_PASSTHROUGH_SHELL)
            .await?;
        let os_version = self
            .settings_store
            .get_value("passthrough_os_version", DEFAULT_PASSTHROUGH_OS_VERSION)
            .await?;
        let working_dir = self
            .settings_store
            .get_value("passthrough_working_dir", DEFAULT_PASSTHROUGH_WORKING_DIR)
            .await?;
        *self.env_passthrough.write().await = EnvPassthrough {
            shell: parse_passthrough_flag(&shell),
            os_version: parse_passthrough_flag(&os_version),
            working_dir: parse_passthrough_flag(&working_dir),
        };
        Ok(())
    }

    /// 从全局设置刷新 Anthropic cache_control TTL 改写模式。
    ///
    /// 与环境透传开关一样缓存到内存,避免每次 `/v1/messages` 转发都查询 settings 表。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 或解析枚举失败时返回业务错误。
    pub async fn reload_cache_control_ttl_rewrite(&self) -> Result<(), AppError> {
        let raw = self
            .settings_store
            .get_value(
                "cache_control_ttl_rewrite",
                DEFAULT_CACHE_CONTROL_TTL_REWRITE,
            )
            .await?;
        *self.cache_control_ttl_rewrite.write().await = CacheControlTtlRewrite::parse(&raw)?;
        Ok(())
    }

    /// 从全局设置刷新 Claude Code messages 缓存断点改写模式。
    ///
    /// 与 TTL 改写一样缓存到内存,避免每次 `/v1/messages` 转发都查询 settings 表。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 或解析枚举失败时返回业务错误。
    pub async fn reload_message_cache_control_rewrite(&self) -> Result<(), AppError> {
        let raw = self
            .settings_store
            .get_value(
                "message_cache_control_rewrite",
                DEFAULT_MESSAGE_CACHE_CONTROL_REWRITE,
            )
            .await?;
        *self.message_cache_control_rewrite.write().await =
            MessageCacheControlRewrite::parse(&raw)?;
        Ok(())
    }

    /// 从全局设置刷新预热/辅助请求拦截配置。
    ///
    /// 配置缓存到内存,让 `/v1/messages` 热路径只读 `RwLock`,不在每次请求时查库。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 失败时返回业务错误。
    pub async fn reload_warmup_intercept_config(&self) -> Result<(), AppError> {
        let title = self
            .settings_store
            .get_value(
                "intercept_warmup_title_enabled",
                DEFAULT_INTERCEPT_WARMUP_TITLE_ENABLED,
            )
            .await?;
        let suggestion = self
            .settings_store
            .get_value(
                "intercept_warmup_suggestion_enabled",
                DEFAULT_INTERCEPT_WARMUP_SUGGESTION_ENABLED,
            )
            .await?;
        let haiku_probe = self
            .settings_store
            .get_value(
                "intercept_warmup_haiku_probe_enabled",
                DEFAULT_INTERCEPT_WARMUP_HAIKU_PROBE_ENABLED,
            )
            .await?;
        *self.warmup_intercept_config.write().await = WarmupInterceptConfig {
            title_enabled: parse_setting_flag(&title),
            suggestion_enabled: parse_setting_flag(&suggestion),
            haiku_probe_enabled: parse_setting_flag(&haiku_probe),
        };
        Ok(())
    }

    /// 从全局设置刷新 `thinking.type=disabled` 兼容改写配置。
    ///
    /// 配置缓存到内存,并在 body 序列化和 CCH attestation 之前参与改写。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 失败时返回业务错误。
    pub async fn reload_disabled_thinking_rewrite(&self) -> Result<(), AppError> {
        let enabled = self
            .settings_store
            .get_value(
                "rewrite_disabled_thinking_enabled",
                DEFAULT_REWRITE_DISABLED_THINKING_ENABLED,
            )
            .await?;
        let models = self
            .settings_store
            .get_value(
                "rewrite_disabled_thinking_models",
                DEFAULT_REWRITE_DISABLED_THINKING_MODELS,
            )
            .await?;
        *self.disabled_thinking_rewrite.write().await = DisabledThinkingRewrite {
            enabled: parse_setting_flag(&enabled),
            models: parse_system_role_model_list(&models),
        };
        Ok(())
    }

    /// 从全局设置刷新 assistant prefill 本地拦截配置。
    ///
    /// 配置缓存到内存,用于在账号选择/RPM/并发槽之前拦截已知不被上游支持的请求形态。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 失败时返回业务错误。
    pub async fn reload_assistant_prefill_intercept_config(&self) -> Result<(), AppError> {
        let enabled = self
            .settings_store
            .get_value(
                "intercept_assistant_prefill_enabled",
                DEFAULT_INTERCEPT_ASSISTANT_PREFILL_ENABLED,
            )
            .await?;
        let models = self
            .settings_store
            .get_value(
                "intercept_assistant_prefill_models",
                DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS,
            )
            .await?;
        *self.assistant_prefill_intercept_config.write().await = AssistantPrefillInterceptConfig {
            enabled: parse_setting_flag(&enabled),
            models: parse_system_role_model_list(&models),
        };
        Ok(())
    }

    /// 从全局设置刷新 429 请求观测日志配置。
    ///
    /// 配置缓存到内存,只在上游返回 429 时读取,默认关闭以避免记录用户请求内容。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 失败时返回业务错误。
    pub async fn reload_rate_limit_request_log_config(&self) -> Result<(), AppError> {
        let enabled = self
            .settings_store
            .get_value("log_429_request_enabled", DEFAULT_LOG_429_REQUEST_ENABLED)
            .await?;
        let body_limit = self
            .settings_store
            .get_value(
                "log_429_request_body_limit",
                DEFAULT_LOG_429_REQUEST_BODY_LIMIT,
            )
            .await?;
        *self.rate_limit_request_log_config.write().await = RateLimitRequestLogConfig {
            enabled: parse_setting_flag(&enabled),
            body_limit: parse_rate_limit_request_body_limit(&body_limit),
        };
        Ok(())
    }

    /// 从全局设置刷新 Claude Code bootstrap response 模型选项改写配置。
    ///
    /// 配置缓存到内存,只在 `/api/claude_cli/bootstrap` 成功 JSON 响应上使用。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 或解析 JSON 失败时返回业务错误。
    pub async fn reload_bootstrap_profile_config(&self) -> Result<(), AppError> {
        let mode = self
            .settings_store
            .get_value(
                "bootstrap_model_options_mode",
                DEFAULT_BOOTSTRAP_MODEL_OPTIONS_MODE,
            )
            .await?;
        let options = self
            .settings_store
            .get_value(
                "bootstrap_additional_model_options",
                DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS,
            )
            .await?;
        *self.bootstrap_profile_config.write().await = BootstrapProfileConfig {
            mode: BootstrapModelOptionsMode::parse(&mode)?,
            additional_model_options: parse_bootstrap_additional_model_options(&options)?,
        };
        Ok(())
    }

    /// 从全局设置刷新客户端访问策略。
    ///
    /// @return 刷新成功返回 `Ok(())`,读取 settings 或解析配置失败时返回业务错误。
    pub async fn reload_access_policy(&self) -> Result<(), AppError> {
        let raw_versions = self
            .settings_store
            .get_value(
                "allowed_claude_code_versions",
                DEFAULT_ALLOWED_CLAUDE_CODE_VERSIONS,
            )
            .await?;
        let raw_user_agents = self
            .settings_store
            .get_value("allowed_user_agents", DEFAULT_ALLOWED_USER_AGENTS)
            .await?;
        let policy = AccessPolicy::parse(&raw_versions, &raw_user_agents)?;
        *self.access_policy.write().await = policy;
        Ok(())
    }

    /// 核心网关逻辑 -- axum handler。
    pub async fn handle_request(&self, req: Request, api_token: Option<&ApiToken>) -> Response {
        match self.handle_request_inner(req, api_token).await {
            Ok(resp) => resp,
            Err(e) => e.into_response(),
        }
    }

    async fn handle_request_inner(
        &self,
        req: Request,
        api_token: Option<&ApiToken>,
    ) -> Result<Response, AppError> {
        let req_start = std::time::Instant::now();
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();

        // 提取 header
        let headers = extract_headers(req.headers());
        let ua = headers
            .get("User-Agent")
            .or_else(|| headers.get("user-agent"))
            .cloned()
            .unwrap_or_default();

        if let Err(rejection) = self.access_policy.read().await.check_user_agent(&ua) {
            warn!(
                "access policy rejected request: setting={} reason={}",
                rejection.setting, rejection.reason
            );
            return Ok(access_policy_error_response(&rejection));
        }

        // 读取请求体
        let body_bytes = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
            .await
            .map_err(|e| AppError::BadRequest(format!("failed to read body: {}", e)))?;

        // 解析请求体
        let body_map: serde_json::Value = if body_bytes.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_slice(&body_bytes).unwrap_or(serde_json::json!({}))
        };

        if path.starts_with("/v1/messages") && has_system_role_message(&body_map) {
            let allowed_models = self.system_role_models.read().await;
            let model = body_map
                .get("model")
                .and_then(|m| m.as_str())
                .unwrap_or_default();
            if !is_system_role_model_allowed(model, &allowed_models) {
                return Ok(system_role_model_error_response(model, &allowed_models));
            }
        }

        // 检测客户端类型
        let client_type = detect_client_type(&ua, &path, &body_map);

        if path.starts_with("/v1/messages") {
            let assistant_prefill_config = self.assistant_prefill_intercept_config.read().await;
            if should_intercept_assistant_prefill(&body_map, &assistant_prefill_config) {
                let model = body_map
                    .get("model")
                    .and_then(|model| model.as_str())
                    .unwrap_or_default();
                warn!(
                    "assistant prefill intercepted before upstream: model={} path={} client_type={:?}",
                    model, path, client_type
                );
                return Ok(assistant_prefill_intercept_response(model));
            }
        }

        // 生成会话哈希
        let session_hash =
            crate::service::account::generate_session_hash(&ua, &body_map, client_type);

        // 根据令牌限制构建账号过滤条件
        let (allowed_ids, blocked_ids) = if let Some(t) = api_token {
            (t.allowed_account_ids(), t.blocked_account_ids())
        } else {
            (vec![], vec![])
        };

        // 429 自动换号 / 并发降级重试循环
        let mut exclude_ids = blocked_ids.clone();
        let mut backoff_retry_ids: Vec<i64> = Vec::new();
        let mut last_resp: Option<Response> = None;

        loop {
            let attempt = exclude_ids.len().saturating_sub(blocked_ids.len());
            // 选择账号
            let t0 = std::time::Instant::now();
            let selected = match self
                .account_svc
                .select_account_with_context(&session_hash, &exclude_ids, &allowed_ids)
                .await
            {
                Ok(selected) => {
                    info!(
                        "[耗时] 账号选择: {:.0}ms → {}",
                        t0.elapsed().as_millis(),
                        selected.account.name
                    );
                    selected
                }
                Err(_) if last_resp.is_some() => {
                    // 无可用账号但有上一次的 429 响应，返回给客户端
                    return Ok(last_resp.unwrap());
                }
                Err(e) => {
                    // 仅当是"无可用账号"且有运行时排除的账号时，返回 429
                    if exclude_ids.len() > blocked_ids.len() {
                        if matches!(&e, AppError::ServiceUnavailable(_)) {
                            return Err(AppError::TooManyRequests("all accounts are busy".into()));
                        }
                    }
                    return Err(AppError::ServiceUnavailable(format!(
                        "no available account: {}",
                        e
                    )));
                }
            };
            let account = selected.account;
            let sticky_account = selected.sticky;
            let should_bind_session = selected.should_bind_session;

            if attempt > 0 {
                warn!("429 retry attempt {} with account {}", attempt, account.id);
            }

            if path.starts_with("/v1/messages") {
                let warmup_config = *self.warmup_intercept_config.read().await;
                if let Some(intercept_type) =
                    detect_warmup_intercept(&body_map, client_type, warmup_config)
                {
                    if should_bind_session {
                        let _ = self
                            .account_svc
                            .bind_selected_session(&session_hash, account.id)
                            .await;
                    }
                    info!(
                        "warmup intercept: type={} account={} model={} stream={}",
                        intercept_type.as_str(),
                        account.id,
                        body_map
                            .get("model")
                            .and_then(|model| model.as_str())
                            .unwrap_or_default(),
                        is_streaming_messages_request(&body_map)
                    );
                    return Ok(mock_warmup_intercept_response(intercept_type, &body_map)?);
                }
            }

            // 自动遥测端点只返回本地假响应，不应进入 RPM 或上游转发。
            if account.auto_telemetry {
                use crate::service::telemetry::{
                    fake_metrics_enabled_response, fake_telemetry_response, is_telemetry_path,
                };

                if is_telemetry_path(&path) {
                    let body = if path.contains("/metrics_enabled") {
                        fake_metrics_enabled_response()
                    } else {
                        fake_telemetry_response()
                    };
                    debug!("telemetry: intercepted {} for account {}", path, account.id);
                    return Ok(axum::Json(body).into_response());
                }
            }

            // 瞬时 429 软退避是账号级、进程内状态。等待放在 RPM/并发前,
            // 避免把上游临时锁定窗口算进本地 RPM 或占住账号并发槽。
            self.account_svc
                .wait_transient_rate_limit_backoff(&account)
                .await;

            // RPM admission 放在并发槽位之前：粘性会话超限时等待/拒绝,非粘性请求超限时换号。
            // 这样不会因为 RPM 等待长期占用账号并发槽位,也不会把已有粘性会话随意切到其他账号。
            match self
                .account_svc
                .acquire_account_rpm(&account, sticky_account, &session_hash)
                .await
            {
                Ok(()) => {}
                Err(AppError::ServiceUnavailable(_)) if !sticky_account => {
                    exclude_ids.push(account.id);
                    continue;
                }
                Err(e) => return Err(e),
            }

            // 获取并发槽位：走账号级 FIFO 排队器（tokio Semaphore 按调用顺序授予 permit）
            let t_slot = std::time::Instant::now();
            let queue = self
                .account_svc
                .get_or_create_queue(account.id, account.concurrency)
                .await;
            let slot_permit = match queue.acquire(SLOT_WAIT_TIMEOUT).await {
                Ok(p) => p,
                Err(QueueWaitError::QueueFull) => {
                    warn!(
                        "account {} wait queue full, falling back to another account",
                        account.id
                    );
                    exclude_ids.push(account.id);
                    continue;
                }
                Err(QueueWaitError::Timeout) => {
                    warn!(
                        "account {} slot wait timed out, falling back to another account",
                        account.id
                    );
                    exclude_ids.push(account.id);
                    continue;
                }
                Err(QueueWaitError::Closed) => {
                    return Err(AppError::Internal("slot semaphore closed".into()));
                }
            };

            // 槽位释放绑定到响应体 stream 的生命周期上。
            // SlotReleaseGuard 兜底：若转发前出错或 panic，drop permit 自动归还槽位。
            // 成功包装 stream 后 defuse() 解除，由 SlotGuardBody 持有 permit 直到流结束。
            let slot_ms = t_slot.elapsed().as_millis();
            if slot_ms > 10 {
                info!("[耗时] 槽位获取: {:.0}ms (排队等待)", slot_ms);
            } else {
                info!("[耗时] 槽位获取: {:.0}ms", slot_ms);
            }
            let mut slot_guard = SlotReleaseGuard::new(slot_permit);

            // 改写请求体
            let t_rewrite = std::time::Instant::now();
            debug!(
                "request body summary BEFORE rewrite: {}",
                safe_body_summary(&body_bytes)
            );
            // 读取缓存的环境透传开关(内存 RwLock,不查库)
            let env_pt = *self.env_passthrough.read().await;
            let cache_ttl = *self.cache_control_ttl_rewrite.read().await;
            let message_cache = *self.message_cache_control_rewrite.read().await;
            let disabled_thinking = self.disabled_thinking_rewrite.read().await.clone();
            let (rewritten_body, stateful_cache_completion) =
                self.rewriter.rewrite_body_with_stateful_completion(
                    &body_bytes,
                    &path,
                    &account,
                    client_type,
                    env_pt,
                    cache_ttl,
                    message_cache,
                    &disabled_thinking,
                );
            debug!(
                "request body summary AFTER rewrite: {}",
                safe_body_summary(&rewritten_body)
            );

            // 重新解析改写后的 body
            let rewritten_body_map: serde_json::Value =
                serde_json::from_slice(&rewritten_body).unwrap_or(serde_json::json!({}));
            let cache_usage_context = if path.starts_with("/v1/messages") {
                Some(CacheUsageLogContext::from_request(
                    &body_map,
                    &rewritten_body_map,
                    client_type,
                    cache_ttl,
                    message_cache,
                    &account.name,
                ))
            } else {
                None
            };

            // 改写 header
            let model_id = body_map.get("model").and_then(|m| m.as_str()).unwrap_or("");
            let rewritten_headers = self.rewriter.rewrite_headers(
                &headers,
                &path,
                &account,
                client_type,
                model_id,
                &rewritten_body_map,
            );

            let telemetry_context = if account.auto_telemetry && path.starts_with("/v1/messages") {
                Some(build_message_telemetry_context(
                    &body_map,
                    &rewritten_body_map,
                    body_bytes.len(),
                    rewritten_body.len(),
                    client_type,
                    attempt,
                    final_beta_header(&rewritten_headers),
                ))
            } else {
                None
            };

            let final_body = rewritten_body.clone();

            let upstream_token = match self.account_svc.resolve_upstream_token(account.id).await {
                Ok(t) => t,
                Err(e) => {
                    // SlotReleaseGuard drop 会自动释放槽位
                    return Err(e);
                }
            };
            info!(
                "[耗时] 请求改写+Token解析: {:.0}ms",
                t_rewrite.elapsed().as_millis()
            );
            let mut final_headers = rewritten_headers;
            final_headers.insert("authorization".into(), format!("Bearer {}", upstream_token));

            if account.auto_telemetry && path.starts_with("/v1/messages") {
                // 遥测会话会启动后台上游请求，必须等 RPM/槽位/改写/Token 全部成功后再激活，
                // 避免被 RPM 跳过的账号产生额外上游副作用。
                let t_tel = std::time::Instant::now();
                self.telemetry_svc.activate_session(&account).await;
                if let Some(context) = telemetry_context.clone() {
                    self.telemetry_svc
                        .record_message_request(&account, context)
                        .await;
                }
                info!("[耗时] 遥测激活: {:.0}ms", t_tel.elapsed().as_millis());
            }

            if should_bind_session {
                // 新会话绑定必须等到当前账号真正准备发往上游后再提交，避免 RPM/排队/Token
                // 失败把 session 提前污染到未实际承载请求的账号上。
                let _ = self
                    .account_svc
                    .bind_selected_session(&session_hash, account.id)
                    .await;
            }

            // 转发到上游
            let t_upstream = std::time::Instant::now();
            let resp = match self
                .forward_request(
                    &method.to_string(),
                    &path,
                    &query,
                    &final_headers,
                    &final_body,
                    &account,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    if let Some(context) = telemetry_context.clone() {
                        self.telemetry_svc
                            .record_message_result(
                                &account,
                                context,
                                MessageTelemetryResult {
                                    status_code: None,
                                    duration_ms: t_upstream.elapsed().as_millis() as u64,
                                    error_kind: Some("upstream_error".into()),
                                },
                            )
                            .await;
                    }
                    info!("[耗时] 上游失败: {:.0}ms", t_upstream.elapsed().as_millis());
                    // SlotReleaseGuard drop 会自动释放槽位
                    return Err(e);
                }
            };
            let (resp, signature_retry_stage) = self
                .maybe_retry_signature_error(
                    &method.to_string(),
                    &path,
                    &query,
                    &final_headers,
                    &final_body,
                    &account,
                    client_type,
                    resp,
                )
                .await?;

            if let Some(context) = telemetry_context.clone() {
                self.telemetry_svc
                    .record_message_result(
                        &account,
                        context,
                        MessageTelemetryResult {
                            status_code: Some(resp.status().as_u16()),
                            duration_ms: t_upstream.elapsed().as_millis() as u64,
                            error_kind: signature_retry_stage.map(|stage| {
                                format!("signature_retry_{}", stage.telemetry_suffix())
                            }),
                        },
                    )
                    .await;
            }
            info!(
                "[耗时] 上游响应: {:.0}ms (HTTP {})",
                t_upstream.elapsed().as_millis(),
                resp.status().as_u16()
            );

            // 非 429：将响应体包装为 SlotGuardBody，流结束时归还槽位
            if resp.status() != StatusCode::TOO_MANY_REQUESTS {
                let should_complete_stateful_cache =
                    resp.status().is_success() && signature_retry_stage.is_none();
                // stateful 锚点描述的是本次发送给上游的 body。签名重试会换 body,非成功响应
                // 也不会建立 prompt cache,因此这些路径不能推进会话锚点。
                let stateful_cache_completion = if should_complete_stateful_cache {
                    stateful_cache_completion
                } else {
                    None
                };
                let permit = slot_guard.defuse();
                let content_codings = response_content_codings(resp.headers());
                let (parts, body) = resp.into_parts();
                let guarded_body = Body::new(SlotGuardBody::new(
                    body,
                    permit,
                    req_start,
                    account.name.clone(),
                    self.rewriter.clone(),
                    stateful_cache_completion,
                    content_codings,
                    cache_usage_context,
                ));
                return Ok(Response::from_parts(parts, guarded_body));
            }

            if let Some(decision) = resp.extensions().get::<RateLimitResponseDecision>() {
                self.account_svc
                    .set_transient_rate_limit_backoff(account.id, decision.delay)
                    .await;
                if !backoff_retry_ids.contains(&account.id) {
                    warn!(
                        "account {} returned transient 429, applying {}s soft backoff before retrying request",
                        account.id,
                        decision.delay.as_secs()
                    );
                    backoff_retry_ids.push(account.id);
                    drop(slot_guard); // 等待期间不占用账号并发槽位
                    self.account_svc
                        .wait_transient_rate_limit_backoff(&account)
                        .await;
                    continue;
                }
            }

            // 429：guard drop 会归还槽位 permit，排除该账号，尝试下一个
            warn!(
                "account {} returned 429, excluding and retrying (attempt {})",
                account.id,
                attempt + 1,
            );
            exclude_ids.push(account.id);
            drop(slot_guard); // 显式 drop → permit drop → 归还槽位
            last_resp = Some(resp);
        }
    }

    /// 对 `/v1/messages` 的 signature 相关 400 执行两阶段降级重试。
    ///
    /// @param method 原始 HTTP 方法。
    /// @param path 原始请求路径。
    /// @param query 原始查询字符串。
    /// @param headers 已改写并携带 upstream token 的请求头。
    /// @param body 已改写后的原始上游请求体。
    /// @param account 当前已选账号，重试必须复用该账号。
    /// @param client_type 原始客户端类型，用于判断 API mimicry 生成的 CCH 是否需要刷新。
    /// @param resp 首次上游响应。
    /// @return 返回最终响应和最后执行的 signature retry 阶段。
    async fn maybe_retry_signature_error(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &std::collections::HashMap<String, String>,
        body: &[u8],
        account: &Account,
        client_type: ClientType,
        resp: Response,
    ) -> Result<(Response, Option<SignatureRetryStage>), AppError> {
        if path != "/v1/messages" || resp.status() != StatusCode::BAD_REQUEST {
            return Ok((resp, None));
        }

        let (mut last_parts, mut last_body) = buffer_response_body(resp).await?;
        if !is_signature_related_error_response_body(&last_body, &last_parts.headers) {
            return Ok((response_from_buffered(last_parts, last_body), None));
        }

        warn!(
            "account {} returned signature-related 400, retrying with sanitized thinking history",
            account.id
        );

        let mut last_stage = None;
        let retry_headers = headers_without_content_length(headers);
        for stage in SignatureRetryStage::ordered() {
            let retry_body = signature_retry_body_for_stage(body, stage);
            let Some(retry_body) = retry_body else {
                continue;
            };
            let retry_body =
                self.rewriter
                    .refresh_cch_attestation(retry_body, account, client_type);

            last_stage = Some(stage);
            warn!(
                "account {} signature retry stage={} request_body={}",
                account.id,
                stage.as_str(),
                safe_body_summary(&retry_body)
            );

            let retry_resp = match self
                .forward_request(method, path, query, &retry_headers, &retry_body, account)
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    warn!(
                        "account {} signature retry stage={} failed: {}",
                        account.id,
                        stage.as_str(),
                        e
                    );
                    continue;
                }
            };

            if retry_resp.status() != StatusCode::BAD_REQUEST {
                return Ok((retry_resp, last_stage));
            }

            let (retry_parts, retry_body_bytes) = buffer_response_body(retry_resp).await?;
            if !is_signature_related_error_response_body(&retry_body_bytes, &retry_parts.headers) {
                return Ok((
                    response_from_buffered(retry_parts, retry_body_bytes),
                    last_stage,
                ));
            }

            warn!(
                "account {} signature retry stage={} still returned signature-related 400",
                account.id,
                stage.as_str()
            );
            last_parts = retry_parts;
            last_body = retry_body_bytes;
        }

        Ok((response_from_buffered(last_parts, last_body), last_stage))
    }

    async fn forward_request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &std::collections::HashMap<String, String>,
        body: &[u8],
        account: &Account,
    ) -> Result<Response, AppError> {
        let mut target_url = format!("{}{}", UPSTREAM_BASE, path);
        if !query.is_empty() {
            let q = if query.contains("beta=true") {
                query.to_string()
            } else {
                format!("{}&beta=true", query)
            };
            target_url = format!("{}?{}", target_url, q);
        } else {
            target_url = format!("{}?beta=true", target_url);
        }

        debug!("upstream URL: {}", target_url);

        let client = crate::tlsfp::get_request_client(&account.proxy_url);

        let mut req_builder = match method {
            "GET" => client.get(&target_url),
            "POST" => client.post(&target_url),
            "PUT" => client.put(&target_url),
            "DELETE" => client.delete(&target_url),
            "PATCH" => client.patch(&target_url),
            _ => client.post(&target_url),
        };

        for (k, v) in ordered_anthropic_headers(path, headers) {
            debug!("upstream header: {}: {}", k, safe_header_log_value(&k, &v));
            req_builder = req_builder.header(k, v);
        }
        req_builder = req_builder.body(body.to_vec());

        let resp = tokio::time::timeout(UPSTREAM_TTFB_TIMEOUT, req_builder.send())
            .await
            .map_err(|_| {
                warn!("upstream TTFB timeout for account {}", account.id);
                AppError::BadGateway("upstream TTFB timeout".into())
            })?
            .map_err(|e| {
                warn!("upstream error for account {}: {}", account.id, e);
                AppError::BadGateway("upstream request failed".into())
            })?;

        let status_code = resp.status().as_u16();
        debug!("upstream response: {}", status_code);

        // 429：上游拒绝了这条请求。先缓冲（很小的）响应体,据此区分两类 429：
        // - 「单请求级拒绝」（如长上下文需 usage credits、额度不足）：与账号容量无关,
        //   绝不隔离账号,直接把 429 透传回客户端(否则一条坏请求会把整个号打成「限流中」)。
        // - 「账号级限流」（撞 5h/7d 墙、真·速率限制）：交给 handle_rate_limit 决定隔离时长。
        // 判定依据(响应体关键字 / retry-after 头)由 handle_rate_limit 内部处理。
        if status_code == 429 {
            // bytes() 会消费 resp,故先克隆需要的响应头
            let headers_429 = resp.headers().clone();
            let retry_after = parse_retry_after(&headers_429);
            // 用这条 429 响应自带的 ratelimit 头做撞墙判断(被动,不再主动查 usage 接口)
            let usage_from_headers = extract_passive_usage(&headers_429);
            let body_bytes = resp.bytes().await.unwrap_or_default();
            let request_log_config = *self.rate_limit_request_log_config.read().await;
            if request_log_config.enabled {
                warn!(
                    "429_request_capture {}",
                    format_rate_limit_request_capture(
                        path,
                        account,
                        headers,
                        body,
                        request_log_config
                    )
                );
            }
            // 业务判断使用解压后的文本；返回给客户端仍使用原始 body，避免改变透传语义。
            let decoded_body = decode_upstream_error_body(&body_bytes, &headers_429);
            let body_snippet = String::from_utf8_lossy(&decoded_body).into_owned();
            warn!(
                "上游错误响应: account={} path={} status={} body={}",
                account.id,
                path,
                status_code,
                safe_upstream_error_log_body(&body_bytes, &headers_429)
            );

            let rate_limit_decision = match self
                .account_svc
                .handle_rate_limit(account, retry_after, &body_snippet, usage_from_headers)
                .await
            {
                Ok(decision) => Some(decision),
                Err(e) => {
                    warn!(
                        "failed to handle rate limit for account {}: {}",
                        account.id, e
                    );
                    None
                }
            };

            // 用缓冲的 body 重建 429 响应（无需流式）,过滤掉网关指纹响应头后返回。
            let mut rb = Response::builder().status(StatusCode::TOO_MANY_REQUESTS);
            for (k, v) in headers_429.iter() {
                if is_gateway_fingerprint_header(k.as_str()) {
                    continue;
                }
                rb = rb.header(k.clone(), v.clone());
            }
            let mut response = rb
                .body(Body::from(body_bytes))
                .map_err(|e| AppError::Internal(format!("build 429 response: {}", e)))?;
            if let Some(RateLimitDecision::RequestBackoff(delay)) = rate_limit_decision {
                response
                    .extensions_mut()
                    .insert(RateLimitResponseDecision { delay });
            }
            return Ok(response);
        }

        // 处理认证失败：403 永久停用（但如果账号已处于 429 限流中则跳过，避免误判）
        if status_code == 403 {
            let is_rate_limited = account
                .rate_limit_reset_at
                .map(|reset| Utc::now() < reset)
                .unwrap_or(false);
            if is_rate_limited {
                warn!(
                    "account {} got 403 while rate-limited, skipping permanent disable",
                    account.id
                );
            } else if let Err(e) = self
                .account_svc
                .disable_account(account.id, AccountStatus::Disabled, "403 认证失败", None)
                .await
            {
                warn!("failed to disable account {} for 403: {}", account.id, e);
            } else {
                warn!("account {} permanently disabled for 403", account.id);
            }
        }

        // 被动采集：从上游响应头提取用量数据，异步合并到数据库。
        // 429 时跳过，因为 handle_rate_limit 已通过 API 获取了完整数据，异步写入会覆盖。
        if status_code != 429 {
            if let Some(usage_json) = extract_passive_usage(resp.headers()) {
                let svc = self.account_svc.clone();
                let aid = account.id;
                tokio::spawn(async move {
                    if let Err(e) = svc.update_passive_usage(aid, usage_json).await {
                        debug!("passive usage update failed for account {}: {}", aid, e);
                    }
                });
            }
        }

        if status_code >= 400 {
            let response_headers = resp.headers().clone();
            let body_bytes = resp.bytes().await.unwrap_or_default();
            warn!(
                "上游错误响应: account={} path={} status={} body={}",
                account.id,
                path,
                status_code,
                safe_upstream_error_log_body(&body_bytes, &response_headers)
            );

            let mut rb = Response::builder().status(
                StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            );
            for (k, v) in response_headers.iter() {
                if is_gateway_fingerprint_header(k.as_str()) {
                    continue;
                }
                rb = rb.header(k.clone(), v.clone());
            }
            return rb
                .body(Body::from(body_bytes))
                .map_err(|e| AppError::Internal(format!("build error response: {}", e)));
        }

        if path.starts_with("/api/claude_cli/bootstrap") {
            let config = self.bootstrap_profile_config.read().await.clone();
            if config.mode != BootstrapModelOptionsMode::Passthrough {
                let headers = resp.headers().clone();
                let body_bytes = resp.bytes().await.unwrap_or_default();
                return rewrite_bootstrap_response(
                    status_code,
                    &headers,
                    body_bytes,
                    query,
                    &config,
                );
            }
        }

        // 构建响应
        let mut response_builder = Response::builder()
            .status(StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));

        for (k, v) in resp.headers() {
            let name = k.as_str();
            // 过滤已知 AI Gateway / 代理指纹响应头，防止客户端检测并上报
            if is_gateway_fingerprint_header(name) {
                continue;
            }
            response_builder = response_builder.header(k.clone(), v.clone());
        }

        // 流式传输响应体，给 bytes_stream 加两次 chunk 间隔超时以检测卡死连接。
        // 该超时只限制"静默时长"，对健康长流无影响。
        // 诊断日志区分三种终止场景：上游错误 / idle 超时 / 正常结束（无日志）。
        // [临时诊断] 追踪 chunk 间隔：任何 >10s 的静默都打 info 日志；错误/超时时汇总最大间隔。
        use tokio_stream::StreamExt;
        let account_name = account.name.clone();
        let idle_secs = UPSTREAM_STREAM_IDLE_TIMEOUT.as_secs();
        let stream_start = std::time::Instant::now();
        let mut last_chunk_at: Option<std::time::Instant> = None;
        let mut max_gap_ms: u128 = 0;
        let mut chunk_count: u64 = 0;
        let body_stream = resp
            .bytes_stream()
            .timeout(UPSTREAM_STREAM_IDLE_TIMEOUT)
            .map(move |r| match r {
                Ok(Ok(bytes)) => {
                    let now = std::time::Instant::now();
                    chunk_count += 1;
                    if let Some(prev) = last_chunk_at {
                        let gap_ms = now.duration_since(prev).as_millis();
                        if gap_ms > max_gap_ms {
                            max_gap_ms = gap_ms;
                        }
                        if gap_ms > 10_000 {
                            info!(
                                "[chunk-gap] {}ms (account: {}, #{} chunk, {} bytes, 已过 {}ms)",
                                gap_ms,
                                account_name,
                                chunk_count,
                                bytes.len(),
                                now.duration_since(stream_start).as_millis()
                            );
                        }
                    }
                    last_chunk_at = Some(now);
                    Ok(bytes)
                }
                Ok(Err(e)) => {
                    warn!(
                        "上游流错误 (account: {}, 已收 {} chunks, 最大间隔 {}ms): {}",
                        account_name, chunk_count, max_gap_ms, e
                    );
                    Err(std::io::Error::other(e))
                }
                Err(_elapsed) => {
                    warn!(
                        "上游流 idle {}s 超时 (account: {}, 已收 {} chunks, 最大间隔 {}ms)",
                        idle_secs, account_name, chunk_count, max_gap_ms
                    );
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "upstream stream idle timeout",
                    ))
                }
            });
        let body = Body::from_stream(body_stream);

        response_builder
            .body(body)
            .map_err(|e| AppError::Internal(format!("build response: {}", e)))
    }
}

fn extract_headers(headers: &HeaderMap) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for (k, v) in headers {
        if let Ok(val) = v.to_str() {
            map.insert(k.to_string(), val.to_string());
        }
    }
    map
}

/// Claude Code 主动扫描响应头检测 AI Gateway/代理（src/services/api/logging.ts）。
/// 过滤这些指纹前缀以防止客户端上报 gateway 类型。
/// Claude Code 扫描的 AI Gateway 响应头前缀（来源: src/services/api/logging.ts）。
const GATEWAY_HEADER_PREFIXES: &[&str] = &[
    "x-litellm-",
    "helicone-",
    "x-portkey-",
    "cf-aig-",
    "x-kong-",
    "x-bt-",
];

fn is_gateway_fingerprint_header(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    GATEWAY_HEADER_PREFIXES.iter().any(|p| lower.starts_with(p))
}

/// 解析 429 响应的 `retry-after` 头（秒）。仅接受正整数秒;HTTP-date 形式忽略。
/// 上游给了明确退避时间时,据此精确冷却,优于自行猜测。
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<i64> {
    headers
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|n| *n > 0)
}

fn safe_body_summary(b: &[u8]) -> String {
    let digest = Sha256::digest(b);
    format!("{} bytes sha256:{}", b.len(), hex::encode(&digest[..8]))
}

fn safe_upstream_error_log_body(body: &[u8], headers: &HeaderMap) -> String {
    let decoded = decode_upstream_error_body(body, headers);
    let raw = if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&decoded) {
        serde_json::to_string(&value)
            .unwrap_or_else(|_| String::from_utf8_lossy(&decoded).into_owned())
    } else {
        String::from_utf8_lossy(&decoded).into_owned()
    };

    let mut out = String::new();
    let mut truncated = false;
    for (i, ch) in raw.chars().enumerate() {
        if i >= UPSTREAM_ERROR_LOG_BODY_LIMIT {
            truncated = true;
            break;
        }
        if ch.is_control() && ch != '\n' && ch != '\r' && ch != '\t' {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    if truncated {
        out.push_str("...<truncated>");
    }
    out
}

fn format_rate_limit_request_capture(
    path: &str,
    account: &Account,
    headers: &std::collections::HashMap<String, String>,
    body: &[u8],
    config: RateLimitRequestLogConfig,
) -> String {
    let body_json = serde_json::from_slice::<serde_json::Value>(body).ok();
    let model = body_json
        .as_ref()
        .and_then(|value| value.get("model"))
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let stream = body_json
        .as_ref()
        .and_then(|value| value.get("stream"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let capture = serde_json::json!({
        "account_id": account.id,
        "path": path,
        "model": model,
        "stream": stream,
        "body_summary": safe_body_summary(body),
        "request_headers": redact_request_headers(headers),
        "request_body": redacted_request_body_for_log(body, config.body_limit),
    });
    serde_json::to_string(&capture).unwrap_or_else(|_| "{}".into())
}

fn redact_request_headers(
    headers: &std::collections::HashMap<String, String>,
) -> serde_json::Value {
    let mut redacted = serde_json::Map::new();
    for (key, value) in headers {
        let safe_value = if is_sensitive_log_key(key) {
            "***REDACTED***".to_string()
        } else {
            redact_sensitive_text(value)
        };
        redacted.insert(key.clone(), serde_json::Value::String(safe_value));
    }
    serde_json::Value::Object(redacted)
}

fn redacted_request_body_for_log(body: &[u8], limit: usize) -> String {
    let raw = if let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(body) {
        redact_sensitive_json_value(&mut value);
        serde_json::to_string(&value).unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned())
    } else {
        String::from_utf8_lossy(body).into_owned()
    };
    truncate_log_text(&redact_sensitive_text(&raw), limit)
}

fn redact_sensitive_json_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if is_sensitive_log_key(key) {
                    *child = serde_json::Value::String("***REDACTED***".into());
                } else {
                    redact_sensitive_json_value(child);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                redact_sensitive_json_value(item);
            }
        }
        serde_json::Value::String(text) => {
            *text = redact_sensitive_text(text);
        }
        _ => {}
    }
}

fn is_sensitive_log_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_')
        .flat_map(|ch| ch.to_lowercase())
        .collect::<String>();
    matches!(
        normalized.as_str(),
        "authorization"
            | "proxyauthorization"
            | "cookie"
            | "setcookie"
            | "xapikey"
            | "anthropicapikey"
            | "apikey"
            | "key"
            | "token"
            | "accesstoken"
            | "refreshtoken"
            | "setuptoken"
            | "password"
            | "secret"
            | "clientsecret"
    )
}

fn redact_sensitive_text(input: &str) -> String {
    let mut out = Vec::with_capacity(input.len());
    let mut redact_next = false;
    for part in input.split_whitespace() {
        if redact_next || looks_like_sensitive_token_part(part) {
            out.push("***REDACTED***");
            redact_next = token_prefix_requires_next_redaction(part);
        } else {
            out.push(part);
            redact_next = false;
        }
    }
    out.join(" ")
}

fn looks_like_sensitive_token_part(part: &str) -> bool {
    let lower = part.to_ascii_lowercase();
    lower.starts_with("bearer")
        || lower.starts_with("basic")
        || lower.starts_with("sk-")
        || lower.contains("access_token=")
        || lower.contains("refresh_token=")
        || lower.contains("authorization=")
        || lower.contains("api_key=")
        || lower.contains("token=")
        || lower.contains("password=")
        || lower.contains("secret=")
        || lower.contains("authorization:")
        || lower.contains("api_key:")
        || lower.contains("token:")
        || lower.contains("password:")
        || lower.contains("secret:")
}

fn token_prefix_requires_next_redaction(part: &str) -> bool {
    let lower = part.to_ascii_lowercase();
    lower == "bearer"
        || lower == "basic"
        || lower.ends_with("=bearer")
        || lower.ends_with(":bearer")
        || lower.ends_with("token:")
        || lower.ends_with("password:")
        || lower.ends_with("secret:")
        || lower.ends_with("authorization:")
        || lower.ends_with("api_key:")
}

fn truncate_log_text(raw: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    let mut out = String::new();
    let mut truncated = false;
    for (idx, ch) in raw.chars().enumerate() {
        if idx >= limit {
            truncated = true;
            break;
        }
        if ch.is_control() && ch != '\n' && ch != '\r' && ch != '\t' {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    if truncated {
        out.push_str("...<truncated>");
    }
    out
}

fn decode_upstream_error_body(body: &[u8], headers: &HeaderMap) -> Vec<u8> {
    let codings = response_content_codings(headers);
    decode_response_body_with_codings(body, &codings)
}

fn decode_response_body_with_codings(body: &[u8], codings: &[String]) -> Vec<u8> {
    let mut decoded = body.to_vec();
    for coding in codings.iter().rev() {
        decoded = match decode_single_content_encoding(&coding, &decoded) {
            Ok(next) => next,
            Err(e) => {
                return format!(
                    "<decode {} failed: {}; raw {}>",
                    coding,
                    e,
                    safe_body_summary(body)
                )
                .into_bytes();
            }
        };
    }
    decoded
}

fn response_content_codings(headers: &HeaderMap) -> Vec<String> {
    headers
        .get_all(CONTENT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|encoding| {
            encoding
                .split(',')
                .map(|v| v.trim().to_ascii_lowercase())
                .filter(|v| !v.is_empty() && v != "identity")
        })
        .collect()
}

fn decode_single_content_encoding(coding: &str, body: &[u8]) -> std::io::Result<Vec<u8>> {
    match coding {
        "gzip" | "x-gzip" => {
            let mut decoder = flate2::read::GzDecoder::new(body);
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
        "deflate" => decode_deflate_body(body),
        "br" => {
            let mut decoder = brotli::Decompressor::new(body, 4096);
            let mut out = Vec::new();
            decoder.read_to_end(&mut out)?;
            Ok(out)
        }
        "zstd" => zstd::stream::decode_all(body),
        _ => Ok(body.to_vec()),
    }
}

fn decode_deflate_body(body: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut zlib_decoder = flate2::read::ZlibDecoder::new(body);
    let mut out = Vec::new();
    match zlib_decoder.read_to_end(&mut out) {
        Ok(_) => Ok(out),
        Err(_) => {
            let mut deflate_decoder = flate2::read::DeflateDecoder::new(body);
            let mut out = Vec::new();
            deflate_decoder.read_to_end(&mut out)?;
            Ok(out)
        }
    }
}

fn rewrite_bootstrap_response(
    status_code: u16,
    headers: &HeaderMap,
    body_bytes: Bytes,
    query: &str,
    config: &BootstrapProfileConfig,
) -> Result<Response, AppError> {
    let codings = response_content_codings(headers);
    let decoded = decode_response_body_with_codings(&body_bytes, &codings);
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&decoded) else {
        return rebuild_upstream_response(status_code, headers, body_bytes);
    };
    if !patch_bootstrap_json(&mut value, query, config) {
        return rebuild_upstream_response(status_code, headers, body_bytes);
    }

    let body = serde_json::to_vec(&value)
        .map_err(|e| AppError::Internal(format!("serialize bootstrap response: {}", e)))?;
    let mut rb = Response::builder()
        .status(StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));
    for (k, v) in headers.iter() {
        if should_skip_rewritten_bootstrap_response_header(k.as_str()) {
            continue;
        }
        rb = rb.header(k.clone(), v.clone());
    }
    rb = rb.header(CONTENT_TYPE, "application/json");
    rb.body(Body::from(body))
        .map_err(|e| AppError::Internal(format!("build bootstrap response: {}", e)))
}

fn rebuild_upstream_response(
    status_code: u16,
    headers: &HeaderMap,
    body_bytes: Bytes,
) -> Result<Response, AppError> {
    let mut rb = Response::builder()
        .status(StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR));
    for (k, v) in headers.iter() {
        if is_gateway_fingerprint_header(k.as_str()) {
            continue;
        }
        rb = rb.header(k.clone(), v.clone());
    }
    rb.body(Body::from(body_bytes))
        .map_err(|e| AppError::Internal(format!("build upstream response: {}", e)))
}

fn should_skip_rewritten_bootstrap_response_header(name: &str) -> bool {
    is_gateway_fingerprint_header(name)
        || name.eq_ignore_ascii_case(CONTENT_ENCODING.as_str())
        || name.eq_ignore_ascii_case(CONTENT_LENGTH.as_str())
        || name.eq_ignore_ascii_case(TRANSFER_ENCODING.as_str())
        || name.eq_ignore_ascii_case(CONTENT_TYPE.as_str())
}

fn patch_bootstrap_json(
    value: &mut serde_json::Value,
    query: &str,
    config: &BootstrapProfileConfig,
) -> bool {
    match config.mode {
        BootstrapModelOptionsMode::Passthrough => false,
        BootstrapModelOptionsMode::Configured => {
            inject_bootstrap_fable_profile(value, query, config);
            true
        }
        BootstrapModelOptionsMode::HideFable => {
            hide_bootstrap_fable_profile(value);
            true
        }
    }
}

fn inject_bootstrap_fable_profile(
    value: &mut serde_json::Value,
    query: &str,
    config: &BootstrapProfileConfig,
) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let client_data = obj
        .entry("client_data")
        .or_insert_with(|| serde_json::json!({}));
    if !client_data.is_object() {
        *client_data = serde_json::json!({});
    }
    if let Some(client_data_obj) = client_data.as_object_mut() {
        let cedar_lagoon = client_data_obj
            .entry("cedar_lagoon")
            .or_insert_with(|| serde_json::json!({}));
        if !cedar_lagoon.is_object() {
            *cedar_lagoon = serde_json::json!({});
        }
        if let Some(cedar_obj) = cedar_lagoon.as_object_mut() {
            cedar_obj.insert("claude-fable".into(), serde_json::Value::Bool(true));
            cedar_obj.insert("claude-mythos".into(), serde_json::Value::Bool(true));
        }
    }
    obj.insert(
        "additional_model_options".into(),
        serde_json::Value::Array(config.additional_model_options.clone()),
    );
    if query_model_is_fable(query) {
        obj.insert("cwk_cfg_key".into(), serde_json::json!("marigold"));
    }
}

fn hide_bootstrap_fable_profile(value: &mut serde_json::Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let client_data = obj
        .entry("client_data")
        .or_insert_with(|| serde_json::json!({}));
    if !client_data.is_object() {
        *client_data = serde_json::json!({});
    }
    if let Some(client_data_obj) = client_data.as_object_mut() {
        let cedar_lagoon = client_data_obj
            .entry("cedar_lagoon")
            .or_insert_with(|| serde_json::json!({}));
        if !cedar_lagoon.is_object() {
            *cedar_lagoon = serde_json::json!({});
        }
        if let Some(cedar_obj) = cedar_lagoon.as_object_mut() {
            cedar_obj.insert("claude-fable".into(), serde_json::Value::Bool(false));
        }
    }
    if let Some(options) = obj
        .get_mut("additional_model_options")
        .and_then(|options| options.as_array_mut())
    {
        options.retain(|item| {
            !item
                .get("model")
                .and_then(|model| model.as_str())
                .map(is_bootstrap_fable_model_option)
                .unwrap_or(false)
        });
    }
    if obj.get("cwk_cfg_key").and_then(|value| value.as_str()) == Some("marigold") {
        obj.insert("cwk_cfg_key".into(), serde_json::Value::Null);
    }
}

fn query_model_is_fable(query: &str) -> bool {
    query.split('&').any(|part| {
        part.strip_prefix("model=")
            .map(|model| model.starts_with("claude-fable-5"))
            .unwrap_or(false)
    })
}

fn is_bootstrap_fable_model_option(model: &str) -> bool {
    model.starts_with("claude-fable-")
}

/// 解析 bootstrap response 中要暴露的额外模型选项 JSON。
///
/// @param raw settings 中保存的 JSON 数组字符串。
/// @return 解析后的模型选项数组。
pub(crate) fn parse_bootstrap_additional_model_options(
    raw: &str,
) -> Result<Vec<serde_json::Value>, AppError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let parsed: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
        AppError::BadRequest(format!(
            "'bootstrap_additional_model_options' 必须是 JSON 数组: {}",
            e
        ))
    })?;
    let Some(items) = parsed.as_array() else {
        return Err(AppError::BadRequest(
            "'bootstrap_additional_model_options' 必须是 JSON 数组".into(),
        ));
    };
    for item in items {
        let Some(obj) = item.as_object() else {
            return Err(AppError::BadRequest(
                "'bootstrap_additional_model_options' 每一项必须是对象".into(),
            ));
        };
        let Some(model) = obj.get("model").and_then(|model| model.as_str()) else {
            return Err(AppError::BadRequest(
                "'bootstrap_additional_model_options' 每一项必须包含 model 字符串".into(),
            ));
        };
        validate_bootstrap_model_option_id(model)?;
    }
    Ok(items.clone())
}

fn validate_bootstrap_model_option_id(model: &str) -> Result<(), AppError> {
    if model.trim().is_empty()
        || !model
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '[' | ']'))
    {
        return Err(AppError::BadRequest(format!(
            "'bootstrap_additional_model_options' 包含非法模型 ID: {}",
            model
        )));
    }
    Ok(())
}

fn is_signature_related_error_response_body(body: &[u8], headers: &HeaderMap) -> bool {
    let decoded = decode_upstream_error_body(body, headers);
    is_signature_related_error_body(&decoded)
}

fn safe_header_log_value(name: &str, value: &str) -> String {
    if name.eq_ignore_ascii_case("authorization") {
        return "***".into();
    }
    value.to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignatureRetryStage {
    ThinkingOnly,
    ThinkingAndTools,
}

impl SignatureRetryStage {
    fn ordered() -> [Self; 2] {
        [Self::ThinkingOnly, Self::ThinkingAndTools]
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ThinkingOnly => "thinking-only",
            Self::ThinkingAndTools => "thinking+tools",
        }
    }

    fn telemetry_suffix(self) -> &'static str {
        match self {
            Self::ThinkingOnly => "thinking_only",
            Self::ThinkingAndTools => "thinking_tools",
        }
    }
}

async fn buffer_response_body(resp: Response) -> Result<(Parts, Bytes), AppError> {
    let (parts, body) = resp.into_parts();
    let body_bytes = axum::body::to_bytes(body, SIGNATURE_ERROR_BODY_LIMIT)
        .await
        .map_err(|e| AppError::Internal(format!("buffer upstream response: {}", e)))?;
    Ok((parts, body_bytes))
}

fn response_from_buffered(parts: Parts, body: Bytes) -> Response {
    Response::from_parts(parts, Body::from(body))
}

fn detect_warmup_intercept(
    body: &serde_json::Value,
    client_type: ClientType,
    config: WarmupInterceptConfig,
) -> Option<WarmupInterceptType> {
    if config.haiku_probe_enabled && is_haiku_probe_request(body, client_type) {
        return Some(WarmupInterceptType::HaikuProbe);
    }
    if config.suggestion_enabled && is_suggestion_mode_request(body) {
        return Some(WarmupInterceptType::Suggestion);
    }
    if config.title_enabled {
        if is_json_title_request(body) {
            return Some(WarmupInterceptType::JsonTitle);
        }
        if is_text_title_or_warmup_request(body) {
            return Some(WarmupInterceptType::TextTitle);
        }
    }
    None
}

fn is_haiku_probe_request(body: &serde_json::Value, client_type: ClientType) -> bool {
    client_type == ClientType::ClaudeCode
        && !is_streaming_messages_request(body)
        && body
            .get("model")
            .and_then(|model| model.as_str())
            .map(|model| model.to_ascii_lowercase().contains("haiku"))
            .unwrap_or(false)
        && body.get("max_tokens").and_then(|tokens| tokens.as_u64()) == Some(1)
}

fn is_suggestion_mode_request(body: &serde_json::Value) -> bool {
    let Some(messages) = body
        .get("messages")
        .and_then(|messages| messages.as_array())
    else {
        return false;
    };
    let Some(last) = messages.last() else {
        return false;
    };
    if last.get("role").and_then(|role| role.as_str()) != Some("user") {
        return false;
    }
    first_text_from_content(last.get("content"))
        .map(|text| text.starts_with("[SUGGESTION MODE:"))
        .unwrap_or(false)
}

fn is_json_title_request(body: &serde_json::Value) -> bool {
    system_text_items(body).any(|text| {
        text.contains(
            "Generate a concise, sentence-case title (3-7 words) that captures the main topic or goal of this coding session",
        )
    })
}

fn is_text_title_or_warmup_request(body: &serde_json::Value) -> bool {
    request_text_items(body).any(|text| {
        text.contains("Please write a 5-10 word title for the following conversation:")
            || text == "Warmup"
    }) || system_text_items(body).any(|text| {
        text.contains(
            "nalyze if this message indicates a new conversation topic. If it does, extract a 2-3 word title",
        )
    })
}

fn is_streaming_messages_request(body: &serde_json::Value) -> bool {
    body.get("stream")
        .and_then(|stream| stream.as_bool())
        .unwrap_or(false)
}

fn should_intercept_assistant_prefill(
    body: &serde_json::Value,
    config: &AssistantPrefillInterceptConfig,
) -> bool {
    config.enabled
        && assistant_prefill_model_matches(
            body.get("model")
                .and_then(|model| model.as_str())
                .unwrap_or(""),
            &config.models,
        )
        && messages_end_with_assistant(body)
}

fn assistant_prefill_model_matches(model: &str, configured_models: &[String]) -> bool {
    configured_models
        .iter()
        .any(|configured| configured.as_str() == model)
}

fn messages_end_with_assistant(body: &serde_json::Value) -> bool {
    body.get("messages")
        .and_then(|messages| messages.as_array())
        .and_then(|messages| messages.last())
        .and_then(|message| message.get("role"))
        .and_then(|role| role.as_str())
        == Some("assistant")
}

fn assistant_prefill_intercept_response(model: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(assistant_prefill_intercept_body(model)),
    )
        .into_response()
}

fn assistant_prefill_intercept_body(model: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "message": "This model does not support assistant message prefill. The conversation must end with a user message.",
            "code": "assistant_prefill_intercepted",
        },
        "model": model,
    })
}

fn request_text_items<'a>(body: &'a serde_json::Value) -> impl Iterator<Item = &'a str> {
    system_text_items(body).chain(message_text_items(body))
}

fn system_text_items<'a>(body: &'a serde_json::Value) -> impl Iterator<Item = &'a str> {
    content_text_items(body.get("system"))
}

fn message_text_items<'a>(body: &'a serde_json::Value) -> impl Iterator<Item = &'a str> {
    body.get("messages")
        .and_then(|messages| messages.as_array())
        .into_iter()
        .flatten()
        .flat_map(|message| content_text_items(message.get("content")))
}

fn first_text_from_content(content: Option<&serde_json::Value>) -> Option<&str> {
    content_text_items(content).next()
}

fn content_text_items(content: Option<&serde_json::Value>) -> Box<dyn Iterator<Item = &str> + '_> {
    match content {
        Some(serde_json::Value::String(text)) => Box::new(std::iter::once(text.as_str())),
        Some(serde_json::Value::Array(items)) => Box::new(items.iter().filter_map(text_from_block)),
        Some(serde_json::Value::Object(_)) => {
            Box::new(content.and_then(text_from_block).into_iter())
        }
        _ => Box::new(std::iter::empty()),
    }
}

fn text_from_block(value: &serde_json::Value) -> Option<&str> {
    match value {
        serde_json::Value::String(text) => Some(text.as_str()),
        serde_json::Value::Object(map) => map
            .get("text")
            .and_then(|text| text.as_str())
            .or_else(|| map.get("content").and_then(|content| content.as_str())),
        _ => None,
    }
}

fn mock_warmup_intercept_response(
    intercept_type: WarmupInterceptType,
    request_body: &serde_json::Value,
) -> Result<Response, AppError> {
    if is_streaming_messages_request(request_body) {
        mock_warmup_intercept_stream_response(intercept_type, request_body)
    } else {
        mock_warmup_intercept_json_response(intercept_type, request_body)
    }
}

fn mock_warmup_intercept_json_response(
    intercept_type: WarmupInterceptType,
    request_body: &serde_json::Value,
) -> Result<Response, AppError> {
    let body = serde_json::json!({
        "id": intercept_type.message_id(),
        "type": "message",
        "role": "assistant",
        "model": request_body.get("model").and_then(|model| model.as_str()).unwrap_or("claude-mock"),
        "content": [{
            "type": "text",
            "text": intercept_type.mock_text(),
        }],
        "stop_reason": intercept_type.stop_reason(),
        "stop_sequence": serde_json::Value::Null,
        "usage": {
            "input_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "output_tokens": mock_output_tokens(intercept_type.mock_text()),
        }
    });
    let body_bytes = serde_json::to_vec(&body)
        .map_err(|e| AppError::Internal(format!("serialize warmup mock response: {}", e)))?;
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body_bytes))
        .map_err(|e| AppError::Internal(format!("build warmup mock response: {}", e)))
}

fn mock_warmup_intercept_stream_response(
    intercept_type: WarmupInterceptType,
    request_body: &serde_json::Value,
) -> Result<Response, AppError> {
    let body = build_warmup_intercept_sse(intercept_type, request_body)?;
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(Body::from(body))
        .map_err(|e| AppError::Internal(format!("build warmup mock stream response: {}", e)))
}

fn build_warmup_intercept_sse(
    intercept_type: WarmupInterceptType,
    request_body: &serde_json::Value,
) -> Result<String, AppError> {
    let message = serde_json::json!({
        "id": intercept_type.message_id(),
        "type": "message",
        "role": "assistant",
        "model": request_body.get("model").and_then(|model| model.as_str()).unwrap_or("claude-mock"),
        "content": [],
        "stop_reason": serde_json::Value::Null,
        "stop_sequence": serde_json::Value::Null,
        "usage": {
            "input_tokens": 0,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
            "output_tokens": 0,
        }
    });
    let events = [
        serde_json::json!({"type": "message_start", "message": message}),
        serde_json::json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""},
        }),
        serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": intercept_type.mock_text()},
        }),
        serde_json::json!({"type": "content_block_stop", "index": 0}),
        serde_json::json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": intercept_type.stop_reason(),
                "stop_sequence": serde_json::Value::Null,
            },
            "usage": {"output_tokens": mock_output_tokens(intercept_type.mock_text())},
        }),
        serde_json::json!({"type": "message_stop"}),
    ];

    let mut out = String::new();
    for event in events {
        let event_type = event
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("message_delta");
        out.push_str("event: ");
        out.push_str(event_type);
        out.push('\n');
        out.push_str("data: ");
        out.push_str(&serde_json::to_string(&event).map_err(|e| {
            AppError::Internal(format!("serialize warmup mock stream event: {}", e))
        })?);
        out.push_str("\n\n");
    }
    Ok(out)
}

fn mock_output_tokens(text: &str) -> u64 {
    if text.is_empty() {
        0
    } else {
        text.split_whitespace().count().max(1) as u64
    }
}

fn headers_without_content_length(
    headers: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    headers
        .iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case("content-length"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn is_signature_related_error_body(body: &[u8]) -> bool {
    let mut text = String::new();
    if let Some(message) = extract_upstream_error_message(body) {
        text.push_str(&message);
        text.push('\n');
    }
    text.push_str(&String::from_utf8_lossy(body));

    let lower = text.to_ascii_lowercase();
    if lower.contains("thought_signature") || lower.contains("signature") {
        return true;
    }
    lower.contains("expected")
        && (lower.contains("thinking") || lower.contains("redacted_thinking"))
}

fn extract_upstream_error_message(body: &[u8]) -> Option<String> {
    let parsed = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    parsed
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .filter(|m| !m.trim().is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            parsed
                .get("message")
                .and_then(|m| m.as_str())
                .filter(|m| !m.trim().is_empty())
                .map(ToString::to_string)
        })
}

fn strip_thinking_from_messages_request(body: &[u8]) -> Option<Vec<u8>> {
    strip_messages_request(body, false)
}

fn strip_signature_sensitive_blocks_from_messages_request(body: &[u8]) -> Option<Vec<u8>> {
    strip_messages_request(body, true)
}

fn signature_retry_body_for_stage(body: &[u8], stage: SignatureRetryStage) -> Option<Vec<u8>> {
    match stage {
        SignatureRetryStage::ThinkingOnly => strip_thinking_from_messages_request(body),
        SignatureRetryStage::ThinkingAndTools => {
            strip_signature_sensitive_blocks_from_messages_request(body)
        }
    }
}

fn strip_messages_request(body: &[u8], strip_tools: bool) -> Option<Vec<u8>> {
    let mut parsed = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    let mut changed = false;

    if let Some(obj) = parsed.as_object_mut() {
        if obj.remove("thinking").is_some() {
            changed = true;
            if remove_thinking_dependent_context_strategies(obj) {
                changed = true;
            }
        }
    }

    if let Some(messages) = parsed.get_mut("messages").and_then(|m| m.as_array_mut()) {
        for message in messages.iter_mut() {
            if sanitize_message_content(message, strip_tools) {
                changed = true;
            }
        }
    }

    if !changed {
        return None;
    }

    serde_json::to_vec(&parsed).ok()
}

fn remove_thinking_dependent_context_strategies(
    obj: &mut serde_json::Map<String, serde_json::Value>,
) -> bool {
    let Some(context_management) = obj
        .get_mut("context_management")
        .and_then(|v| v.as_object_mut())
    else {
        return false;
    };
    let Some(edits) = context_management
        .get_mut("edits")
        .and_then(|v| v.as_array_mut())
    else {
        return false;
    };

    let original_len = edits.len();
    edits.retain(|edit| {
        edit.get("type").and_then(|v| v.as_str()) != Some("clear_thinking_20251015")
    });
    if edits.len() == original_len {
        return false;
    }

    if edits.is_empty() {
        context_management.remove("edits");
    }
    true
}

fn sanitize_message_content(message: &mut serde_json::Value, strip_tools: bool) -> bool {
    let content = match message.get_mut("content") {
        Some(content) => content,
        None => return false,
    };
    let blocks = match content.as_array_mut() {
        Some(blocks) => blocks,
        None => return false,
    };

    let original = std::mem::take(blocks);
    let mut sanitized = Vec::with_capacity(original.len());
    let mut changed = false;

    for block in original {
        match sanitize_content_block(block, strip_tools) {
            BlockSanitizeResult::Keep(block) => sanitized.push(block),
            BlockSanitizeResult::Replace(block) => {
                sanitized.push(block);
                changed = true;
            }
            BlockSanitizeResult::Remove => {
                changed = true;
            }
        }
    }

    if changed && sanitized.is_empty() {
        sanitized.push(serde_json::json!({
            "type": "text",
            "text": "(content removed)"
        }));
    }

    *blocks = sanitized;
    changed
}

enum BlockSanitizeResult {
    Keep(serde_json::Value),
    Replace(serde_json::Value),
    Remove,
}

fn sanitize_content_block(block: serde_json::Value, strip_tools: bool) -> BlockSanitizeResult {
    let obj = match block {
        serde_json::Value::Object(obj) => obj,
        other => return BlockSanitizeResult::Keep(other),
    };
    let block_type = obj
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    match block_type.as_str() {
        "thinking" => {
            let thinking = obj
                .get("thinking")
                .and_then(|t| t.as_str())
                .unwrap_or_default();
            if thinking.is_empty() {
                BlockSanitizeResult::Remove
            } else {
                BlockSanitizeResult::Replace(text_block(thinking.to_string()))
            }
        }
        "redacted_thinking" => BlockSanitizeResult::Remove,
        "tool_use" if strip_tools => BlockSanitizeResult::Replace(tool_use_text_block(&obj)),
        "tool_result" if strip_tools => BlockSanitizeResult::Replace(tool_result_text_block(&obj)),
        "" => {
            if let Some(thinking) = obj.get("thinking").and_then(|t| t.as_str()) {
                if thinking.is_empty() {
                    BlockSanitizeResult::Remove
                } else {
                    BlockSanitizeResult::Replace(text_block(thinking.to_string()))
                }
            } else {
                BlockSanitizeResult::Keep(serde_json::Value::Object(obj))
            }
        }
        _ => BlockSanitizeResult::Keep(serde_json::Value::Object(obj)),
    }
}

fn text_block(text: String) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": text
    })
}

fn tool_use_text_block(obj: &serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    let mut text = "(tool_use)".to_string();
    if let Some(name) = obj.get("name").and_then(|v| v.as_str()) {
        if !name.is_empty() {
            text.push_str(" name=");
            text.push_str(name);
        }
    }
    if let Some(id) = obj.get("id").and_then(|v| v.as_str()) {
        if !id.is_empty() {
            text.push_str(" id=");
            text.push_str(id);
        }
    }
    if let Some(input) = obj.get("input") {
        if !input.is_null() {
            text.push_str(" input=");
            text.push_str(&serde_json::to_string(input).unwrap_or_else(|_| "null".into()));
        }
    }
    text_block(text)
}

fn tool_result_text_block(obj: &serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    let mut text = "(tool_result)".to_string();
    if let Some(tool_use_id) = obj.get("tool_use_id").and_then(|v| v.as_str()) {
        if !tool_use_id.is_empty() {
            text.push_str(" tool_use_id=");
            text.push_str(tool_use_id);
        }
    }
    if obj
        .get("is_error")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        text.push_str(" is_error=true");
    }
    if let Some(content) = obj.get("content") {
        if !content.is_null() {
            text.push('\n');
            text.push_str(&serde_json::to_string(content).unwrap_or_else(|_| "null".into()));
        }
    }
    text_block(text)
}

fn has_system_role_message(body: &serde_json::Value) -> bool {
    body.get("messages")
        .and_then(|m| m.as_array())
        .map(|messages| {
            messages
                .iter()
                .any(|message| message.get("role").and_then(|role| role.as_str()) == Some("system"))
        })
        .unwrap_or(false)
}

fn parse_system_role_model_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// 将设置项字符串解析为开关布尔值,仅 "true" 视为开启。
fn parse_setting_flag(raw: &str) -> bool {
    raw == "true"
}

/// 将系统提示词环境透传设置解析为布尔值。
fn parse_passthrough_flag(raw: &str) -> bool {
    parse_setting_flag(raw)
}

/// 构造系统提示词环境透传开关的内存初始值(reload 之前的兜底,与设置默认值保持一致)。
fn default_env_passthrough() -> EnvPassthrough {
    EnvPassthrough {
        shell: parse_passthrough_flag(DEFAULT_PASSTHROUGH_SHELL),
        os_version: parse_passthrough_flag(DEFAULT_PASSTHROUGH_OS_VERSION),
        working_dir: parse_passthrough_flag(DEFAULT_PASSTHROUGH_WORKING_DIR),
    }
}

/// 构造 cache_control TTL 改写模式初始值(reload 之前的兜底,与设置默认值保持一致)。
fn default_cache_control_ttl_rewrite() -> CacheControlTtlRewrite {
    CacheControlTtlRewrite::parse(DEFAULT_CACHE_CONTROL_TTL_REWRITE)
        .expect("默认 cache_control TTL 改写模式必须合法")
}

/// 构造 messages cache_control 改写模式初始值(reload 之前的兜底,与设置默认值保持一致)。
fn default_message_cache_control_rewrite() -> MessageCacheControlRewrite {
    MessageCacheControlRewrite::parse(DEFAULT_MESSAGE_CACHE_CONTROL_REWRITE)
        .expect("默认 message cache_control 改写模式必须合法")
}

/// 构造预热请求拦截配置初始值(reload 之前的兜底,与设置默认值保持一致)。
fn default_warmup_intercept_config() -> WarmupInterceptConfig {
    WarmupInterceptConfig {
        title_enabled: parse_setting_flag(DEFAULT_INTERCEPT_WARMUP_TITLE_ENABLED),
        suggestion_enabled: parse_setting_flag(DEFAULT_INTERCEPT_WARMUP_SUGGESTION_ENABLED),
        haiku_probe_enabled: parse_setting_flag(DEFAULT_INTERCEPT_WARMUP_HAIKU_PROBE_ENABLED),
    }
}

/// 构造 `thinking.type=disabled` 兼容改写的内存初始值。
fn default_disabled_thinking_rewrite() -> DisabledThinkingRewrite {
    DisabledThinkingRewrite {
        enabled: parse_setting_flag(DEFAULT_REWRITE_DISABLED_THINKING_ENABLED),
        models: parse_system_role_model_list(DEFAULT_REWRITE_DISABLED_THINKING_MODELS),
    }
}

/// 构造 assistant prefill 拦截配置初始值。
fn default_assistant_prefill_intercept_config() -> AssistantPrefillInterceptConfig {
    AssistantPrefillInterceptConfig {
        enabled: parse_setting_flag(DEFAULT_INTERCEPT_ASSISTANT_PREFILL_ENABLED),
        models: parse_system_role_model_list(DEFAULT_INTERCEPT_ASSISTANT_PREFILL_MODELS),
    }
}

/// 构造 429 请求观测日志配置初始值。
fn default_rate_limit_request_log_config() -> RateLimitRequestLogConfig {
    RateLimitRequestLogConfig {
        enabled: parse_setting_flag(DEFAULT_LOG_429_REQUEST_ENABLED),
        body_limit: parse_rate_limit_request_body_limit(DEFAULT_LOG_429_REQUEST_BODY_LIMIT),
    }
}

/// 构造 bootstrap response 模型选项改写配置初始值。
fn default_bootstrap_profile_config() -> BootstrapProfileConfig {
    BootstrapProfileConfig {
        mode: BootstrapModelOptionsMode::parse(DEFAULT_BOOTSTRAP_MODEL_OPTIONS_MODE)
            .expect("默认 bootstrap 模型选项模式必须合法"),
        additional_model_options: parse_bootstrap_additional_model_options(
            DEFAULT_BOOTSTRAP_ADDITIONAL_MODEL_OPTIONS,
        )
        .expect("默认 bootstrap 额外模型选项必须合法"),
    }
}

/// 解析 429 请求体日志字符上限。非法值回落到默认值。
fn parse_rate_limit_request_body_limit(raw: &str) -> usize {
    raw.trim().parse::<usize>().ok().unwrap_or_else(|| {
        DEFAULT_LOG_429_REQUEST_BODY_LIMIT
            .parse::<usize>()
            .expect("默认 429 请求体日志上限必须是合法 usize")
    })
}

fn is_system_role_model_allowed(model: &str, allowed_models: &[String]) -> bool {
    allowed_models.iter().any(|allowed| allowed == model)
}

fn system_role_model_error_body(model: &str, allowed_models: &[String]) -> serde_json::Value {
    let message = "messages[].role=system is not allowed for this model";
    serde_json::json!({
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "message": message,
            "code": "system_role_model_not_allowed",
        },
        "model": model,
        "allowed_system_role_models": allowed_models,
    })
}

fn system_role_model_error_response(model: &str, allowed_models: &[String]) -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(system_role_model_error_body(model, allowed_models)),
    )
        .into_response()
}

fn build_message_telemetry_context(
    original_body: &serde_json::Value,
    rewritten_body: &serde_json::Value,
    request_body_bytes: usize,
    rewritten_body_bytes: usize,
    client_type: ClientType,
    attempt: usize,
    betas: String,
) -> MessageTelemetryContext {
    MessageTelemetryContext {
        model: original_body
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .to_string(),
        session_id: extract_message_session_id(rewritten_body)
            .or_else(|| crate::service::rewriter::extract_session_id_from_body(rewritten_body)),
        request_body_bytes,
        rewritten_body_bytes,
        stream: original_body
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        tool_count: original_body
            .get("tools")
            .and_then(|v| v.as_array())
            .map(|tools| tools.len())
            .unwrap_or(0),
        attachment_count: count_attachment_blocks(original_body),
        system_prompt_block_count: count_system_prompt_blocks(rewritten_body),
        client_type: match client_type {
            ClientType::ClaudeCode => "claude_code",
            ClientType::API => "api",
        }
        .into(),
        attempt,
        betas,
    }
}

fn final_beta_header(headers: &std::collections::HashMap<String, String>) -> String {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("anthropic-beta"))
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

fn extract_message_session_id(body: &serde_json::Value) -> Option<String> {
    let user_id = body
        .get("metadata")
        .and_then(|m| m.get("user_id"))
        .and_then(|u| u.as_str())?;
    let parsed = serde_json::from_str::<serde_json::Value>(user_id).ok()?;
    parsed
        .get("session_id")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

fn count_messages(body: &serde_json::Value) -> usize {
    body.get("messages")
        .and_then(|messages| messages.as_array())
        .map(|messages| messages.len())
        .unwrap_or(0)
}

fn count_message_content_blocks(body: &serde_json::Value) -> usize {
    body.get("messages")
        .and_then(|messages| messages.as_array())
        .map(|messages| {
            messages
                .iter()
                .map(|message| match message.get("content") {
                    Some(serde_json::Value::Array(content)) => content.len(),
                    Some(serde_json::Value::String(_)) => 1,
                    _ => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}

fn count_cache_breakpoints_in_array_field(body: &serde_json::Value, field: &str) -> usize {
    body.get(field)
        .and_then(|items| items.as_array())
        .map(|items| {
            items
                .iter()
                .filter(|item| item.get("cache_control").is_some())
                .count()
        })
        .unwrap_or(0)
}

fn count_message_cache_breakpoints(body: &serde_json::Value) -> usize {
    body.get("messages")
        .and_then(|messages| messages.as_array())
        .map(|messages| {
            messages
                .iter()
                .filter_map(|message| {
                    message
                        .get("content")
                        .and_then(|content| content.as_array())
                })
                .flat_map(|content| content.iter())
                .filter(|block| block.get("cache_control").is_some())
                .count()
        })
        .unwrap_or(0)
}

fn stable_body_hash_for_log(body: &serde_json::Value) -> String {
    match serde_json::to_vec(body) {
        Ok(bytes) => short_hash_for_log(&bytes),
        Err(_) => "serialize_error".into(),
    }
}

fn short_hash_for_log(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(&digest[..6])
}

fn count_system_prompt_blocks(body: &serde_json::Value) -> usize {
    match body.get("system") {
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter(|item| item.get("type").and_then(|t| t.as_str()) == Some("text"))
            .count(),
        Some(serde_json::Value::String(s)) if !s.is_empty() => 1,
        _ => 0,
    }
}

fn count_attachment_blocks(body: &serde_json::Value) -> usize {
    let mut count = 0;
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for message in messages {
            count +=
                count_attachment_value(message.get("content").unwrap_or(&serde_json::Value::Null));
        }
    }
    count
}

fn count_attachment_value(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(items) => items.iter().map(count_attachment_value).sum(),
        serde_json::Value::Object(map) => {
            let kind = map.get("type").and_then(|t| t.as_str()).unwrap_or("");
            let self_count = matches!(
                kind,
                "image" | "image_url" | "document" | "file" | "input_image" | "input_file"
            ) as usize;
            self_count
                + map
                    .get("content")
                    .map(count_attachment_value)
                    .unwrap_or_default()
        }
        _ => 0,
    }
}

/// 从上游响应头中提取 ratelimit 用量信息，构建与 OAuth usage API 格式一致的 JSON。
/// 仅保留 utilization 和 resets_at 都存在且可解析的完整窗口，避免不完整数据导致前端异常。
/// 没有任何完整窗口时返回 None。
pub(crate) fn extract_passive_usage(
    headers: &reqwest::header::HeaderMap,
) -> Option<serde_json::Value> {
    let get_str = |name: &str| -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };

    let mut usage = serde_json::json!({});
    let mut has_window = false;

    // 5 小时窗口：utilization 和 resets_at 必须同时存在且可解析
    if let (Some(util_str), Some(reset_raw)) = (
        get_str("anthropic-ratelimit-unified-5h-utilization"),
        get_str("anthropic-ratelimit-unified-5h-reset"),
    ) {
        if let (Ok(util), Some(reset)) = (
            util_str.parse::<f64>(),
            normalize_reset_timestamp(&reset_raw),
        ) {
            // 响应头返回 0~1 的比例，乘以 100 转为百分比，与 OAuth usage API 格式一致
            usage["five_hour"] =
                serde_json::json!({ "utilization": util * 100.0, "resets_at": reset });
            has_window = true;
        }
    }

    // 7 天窗口：同上
    if let (Some(util_str), Some(reset_raw)) = (
        get_str("anthropic-ratelimit-unified-7d-utilization"),
        get_str("anthropic-ratelimit-unified-7d-reset"),
    ) {
        if let (Ok(util), Some(reset)) = (
            util_str.parse::<f64>(),
            normalize_reset_timestamp(&reset_raw),
        ) {
            usage["seven_day"] =
                serde_json::json!({ "utilization": util * 100.0, "resets_at": reset });
            has_window = true;
        }
    }

    if has_window { Some(usage) } else { None }
}

/// 将响应头中的重置时间统一转为 RFC3339 格式。
/// 响应头可能是 Unix 时间戳（秒）或已经是 ISO8601/RFC3339 字符串。
fn normalize_reset_timestamp(raw: &str) -> Option<String> {
    // 尝试解析为 Unix 时间戳（秒）
    if let Ok(ts) = raw.parse::<i64>() {
        return chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339());
    }
    // 尝试解析为 RFC3339，验证合法性
    if chrono::DateTime::parse_from_rfc3339(raw).is_ok() {
        return Some(raw.to_string());
    }
    None
}

/// 并发槽位释放守卫：drop permit 时自动归还 semaphore，防止错误路径或 panic 导致泄漏。
/// 成功包装 stream 后调用 `defuse()` 把 permit 交给 `SlotGuardBody` 管理。
struct SlotReleaseGuard {
    permit: Option<OwnedSemaphorePermit>,
}

impl SlotReleaseGuard {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            permit: Some(permit),
        }
    }

    /// 解除守卫并取出 permit，调用后本 guard drop 不再释放。
    fn defuse(&mut self) -> OwnedSemaphorePermit {
        self.permit
            .take()
            .expect("SlotReleaseGuard already defused")
    }
}

impl Drop for SlotReleaseGuard {
    fn drop(&mut self) {
        // permit 的 Drop 本身就会归还 semaphore,无需额外动作
        let _ = self.permit.take();
    }
}

/// 包装响应体 Body，在 body 传输完成或被丢弃时自动归还并发槽位。
///
/// 解决 Axum 流式响应中 handler 提前返回导致槽位过早释放的问题：
/// 将 permit 的生命周期绑定到 body 上，确保槽位覆盖整个传输过程。
struct SlotGuardBody {
    inner: Body,
    /// `Some(permit)` 表示槽位尚未归还；`Drop` 时 take 让 permit 自动归还 semaphore。
    permit: Option<OwnedSemaphorePermit>,
    /// stateful 缓存状态只在上游响应体读到 EOF 后提交。
    stateful_cache_completion: Option<StatefulCacheCompletion>,
    /// 上游 Anthropic 响应中已观察到的 prompt cache 使用量。
    stateful_cache_usage: StatefulCacheUsage,
    /// 跨 frame 保留未完成的 SSE 行。
    stateful_cache_usage_buffer: String,
    /// 压缩响应的旁路采样副本。原始 frame 仍直接透传给下游。
    stateful_cache_usage_side_tap: Vec<u8>,
    /// 旁路采样是否因超过上限被截断。
    stateful_cache_usage_side_tap_truncated: bool,
    /// 上游响应的 content-encoding 列表,用于 EOF 后解压旁路副本。
    response_content_codings: Vec<String>,
    /// 请求改写器,用于提交 stateful 缓存状态。
    rewriter: Arc<Rewriter>,
    /// 请求开始时间，用于计算首字耗时。
    req_start: std::time::Instant,
    /// 账号名称，用于日志输出。
    account_name: String,
    /// prompt cache usage 诊断上下文。
    cache_usage_context: Option<CacheUsageLogContext>,
    /// 是否已收到第一个 frame。
    first_frame_logged: bool,
}

/// prompt cache usage 日志上下文,只保存脱敏后的请求结构信息。
#[derive(Clone)]
struct CacheUsageLogContext {
    client_type: ClientType,
    model: String,
    message_cache_mode: MessageCacheControlRewrite,
    ttl_mode: CacheControlTtlRewrite,
    account_name: String,
    session_hash: String,
    request_hash: String,
    message_count: usize,
    message_content_blocks: usize,
    system_breakpoints: usize,
    tool_breakpoints: usize,
    message_breakpoints: usize,
}

impl CacheUsageLogContext {
    /// 从原始请求和最终上游请求构造脱敏 usage 诊断上下文。
    fn from_request(
        original_body: &serde_json::Value,
        rewritten_body: &serde_json::Value,
        client_type: ClientType,
        ttl_mode: CacheControlTtlRewrite,
        message_cache_mode: MessageCacheControlRewrite,
        account_name: &str,
    ) -> Self {
        let model = original_body
            .get("model")
            .and_then(|m| m.as_str())
            .unwrap_or_default()
            .to_string();
        let session_id = extract_message_session_id(rewritten_body)
            .or_else(|| crate::service::rewriter::extract_session_id_from_body(rewritten_body))
            .unwrap_or_default();
        Self {
            client_type,
            model,
            message_cache_mode,
            ttl_mode,
            account_name: account_name.to_string(),
            session_hash: short_hash_for_log(session_id.as_bytes()),
            request_hash: stable_body_hash_for_log(rewritten_body),
            message_count: count_messages(rewritten_body),
            message_content_blocks: count_message_content_blocks(rewritten_body),
            system_breakpoints: count_cache_breakpoints_in_array_field(rewritten_body, "system"),
            tool_breakpoints: count_cache_breakpoints_in_array_field(rewritten_body, "tools"),
            message_breakpoints: count_message_cache_breakpoints(rewritten_body),
        }
    }

    /// 生成人类可读的请求摘要。
    fn human_summary(&self) -> String {
        format!(
            "客户端={} 模型={} 账号={} 模式={} ttl={} session={} req={} 消息={}/blocks={} 断点msg/system/tool={}/{}/{}",
            self.client_type.as_str(),
            display_or_dash(&self.model),
            self.account_name,
            self.message_cache_mode.as_str(),
            self.ttl_mode.as_str(),
            display_or_dash(&self.session_hash),
            self.request_hash,
            self.message_count,
            self.message_content_blocks,
            self.message_breakpoints,
            self.system_breakpoints,
            self.tool_breakpoints
        )
    }
}

impl SlotGuardBody {
    fn new(
        inner: Body,
        permit: OwnedSemaphorePermit,
        req_start: std::time::Instant,
        account_name: String,
        rewriter: Arc<Rewriter>,
        stateful_cache_completion: Option<StatefulCacheCompletion>,
        response_content_codings: Vec<String>,
        cache_usage_context: Option<CacheUsageLogContext>,
    ) -> Self {
        Self {
            inner,
            permit: Some(permit),
            stateful_cache_completion,
            stateful_cache_usage: StatefulCacheUsage::default(),
            stateful_cache_usage_buffer: String::new(),
            stateful_cache_usage_side_tap: Vec::new(),
            stateful_cache_usage_side_tap_truncated: false,
            response_content_codings,
            rewriter,
            req_start,
            account_name,
            cache_usage_context,
            first_frame_logged: false,
        }
    }
}

impl http_body::Body for SlotGuardBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let result = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        if !self.first_frame_logged {
            if let std::task::Poll::Ready(Some(Ok(_))) = &result {
                self.first_frame_logged = true;
                info!(
                    "[耗时] 首字到达: {:.0}ms → {}",
                    self.req_start.elapsed().as_millis(),
                    self.account_name
                );
            }
        }
        if let std::task::Poll::Ready(Some(Ok(frame))) = &result {
            let mut usage = self.stateful_cache_usage;
            let mut buffer = std::mem::take(&mut self.stateful_cache_usage_buffer);
            update_stateful_cache_usage_from_frame(&mut usage, &mut buffer, frame);
            self.stateful_cache_usage = usage;
            self.stateful_cache_usage_buffer = buffer;
            if !self.response_content_codings.is_empty() {
                if let Some(bytes) = frame.data_ref() {
                    let mut side_tap = std::mem::take(&mut self.stateful_cache_usage_side_tap);
                    let mut side_tap_truncated = self.stateful_cache_usage_side_tap_truncated;
                    append_stateful_usage_side_tap(&mut side_tap, &mut side_tap_truncated, bytes);
                    self.stateful_cache_usage_side_tap = side_tap;
                    self.stateful_cache_usage_side_tap_truncated = side_tap_truncated;
                }
            }
        }
        if let std::task::Poll::Ready(None) = &result {
            let mut usage = self.stateful_cache_usage;
            let mut buffer = std::mem::take(&mut self.stateful_cache_usage_buffer);
            flush_stateful_cache_usage_buffer(&mut usage, &mut buffer);
            merge_stateful_cache_usage_from_compressed_side_tap(
                &mut usage,
                &self.response_content_codings,
                &self.stateful_cache_usage_side_tap,
                self.stateful_cache_usage_side_tap_truncated,
            );
            self.stateful_cache_usage = usage;
            self.stateful_cache_usage_buffer = buffer;
            let completion = self.stateful_cache_completion.take();
            let usage = observed_stateful_cache_usage(self.stateful_cache_usage);
            log_prompt_cache_usage(self.cache_usage_context.as_ref(), usage);
            self.rewriter
                .complete_stateful_cache_with_usage(completion, usage);
        } else if let std::task::Poll::Ready(Some(Err(_))) = &result {
            // 上游 body 读失败时 Anthropic 的缓存写入状态未知,代理侧不能把该断点当成可复用锚点。
            log_prompt_cache_usage_read_error(self.cache_usage_context.as_ref());
            let _ = self.stateful_cache_completion.take();
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// 返回已经观察到的 Anthropic prompt cache usage。
fn observed_stateful_cache_usage(usage: StatefulCacheUsage) -> Option<StatefulCacheUsage> {
    if usage.cache_read_input_tokens == 0
        && usage.cache_creation_input_tokens == 0
        && usage.cache_creation_ephemeral_5m_input_tokens == 0
        && usage.cache_creation_ephemeral_1h_input_tokens == 0
    {
        None
    } else {
        Some(usage)
    }
}

/// 输出上游实际返回的 prompt cache usage。
fn log_prompt_cache_usage(
    context: Option<&CacheUsageLogContext>,
    usage: Option<StatefulCacheUsage>,
) {
    let Some(context) = context else {
        return;
    };
    let usage = usage.unwrap_or_default();
    info!(
        target: "cc2api::cache",
        "[缓存诊断] usage: 读={} 写={} (5m={},1h={}) 命中率≈{} {}",
        format_tokens(usage.cache_read_input_tokens),
        format_tokens(usage.cache_creation_input_tokens),
        format_tokens(usage.cache_creation_ephemeral_5m_input_tokens),
        format_tokens(usage.cache_creation_ephemeral_1h_input_tokens),
        cache_hit_ratio_label(&usage),
        context.human_summary()
    );
}

/// 输出响应体读取失败时的 prompt cache usage 诊断占位。
fn log_prompt_cache_usage_read_error(context: Option<&CacheUsageLogContext>) {
    let Some(context) = context else {
        return;
    };
    warn!(
        target: "cc2api::cache",
        "[缓存诊断] usage: 响应流读取失败,无法解析缓存读写 {}",
        context.human_summary()
    );
}

/// 格式化 token 数,便于人工阅读。
fn format_tokens(value: u64) -> String {
    let s = value.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (idx, ch) in s.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

/// 粗略显示 prompt cache 命中读占比。
fn cache_hit_ratio_label(usage: &StatefulCacheUsage) -> String {
    let total = usage
        .cache_read_input_tokens
        .saturating_add(usage.cache_creation_input_tokens);
    if total == 0 {
        return "0%".into();
    }
    format!(
        "{}%",
        usage.cache_read_input_tokens.saturating_mul(100) / total
    )
}

/// 空字符串在日志里显示为 `-`。
fn display_or_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn append_stateful_usage_side_tap(buffer: &mut Vec<u8>, truncated: &mut bool, bytes: &[u8]) {
    if *truncated {
        return;
    }
    let remaining = STATEFUL_USAGE_SIDE_TAP_LIMIT.saturating_sub(buffer.len());
    if bytes.len() <= remaining {
        buffer.extend_from_slice(bytes);
    } else {
        buffer.extend_from_slice(&bytes[..remaining]);
        *truncated = true;
    }
}

fn merge_stateful_cache_usage_from_compressed_side_tap(
    usage: &mut StatefulCacheUsage,
    codings: &[String],
    compressed: &[u8],
    truncated: bool,
) {
    if codings.is_empty() || compressed.is_empty() || truncated {
        return;
    }
    let decoded = decode_response_body_with_codings(compressed, codings);
    let mut buffer = String::new();
    update_stateful_cache_usage_from_bytes(usage, &mut buffer, &decoded);
    flush_stateful_cache_usage_buffer(usage, &mut buffer);
}

/// 从响应 frame 中提取 prompt cache usage。
fn update_stateful_cache_usage_from_frame(
    usage: &mut StatefulCacheUsage,
    buffer: &mut String,
    frame: &http_body::Frame<bytes::Bytes>,
) {
    if let Some(bytes) = frame.data_ref() {
        update_stateful_cache_usage_from_bytes(usage, buffer, bytes);
    }
}

/// 从 SSE 或非流式 JSON 响应片段中提取 prompt cache usage。
fn update_stateful_cache_usage_from_bytes(
    usage: &mut StatefulCacheUsage,
    buffer: &mut String,
    bytes: &[u8],
) {
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) {
        merge_stateful_cache_usage_from_value(usage, &value);
    }

    buffer.push_str(&String::from_utf8_lossy(bytes));
    if buffer.len() > STATEFUL_USAGE_BUFFER_LIMIT {
        let keep_from = next_char_boundary(buffer, buffer.len() - STATEFUL_USAGE_BUFFER_LIMIT);
        buffer.drain(..keep_from);
        if let Some(newline_idx) = buffer.find('\n') {
            buffer.drain(..=newline_idx);
        }
    }
    let complete_len = match buffer.rfind('\n') {
        Some(index) => index + 1,
        None => return,
    };
    let complete = buffer[..complete_len].to_string();
    buffer.drain(..complete_len);

    merge_stateful_cache_usage_from_lines(usage, complete.lines());
}

fn next_char_boundary(value: &str, mut index: usize) -> usize {
    while index < value.len() && !value.is_char_boundary(index) {
        index += 1;
    }
    index
}

/// 在流结束时解析最后一个没有换行结尾的 SSE data 行。
fn flush_stateful_cache_usage_buffer(usage: &mut StatefulCacheUsage, buffer: &mut String) {
    if buffer.is_empty() {
        return;
    }
    let remaining = std::mem::take(buffer);
    merge_stateful_cache_usage_from_lines(usage, remaining.lines());
}

/// 从 SSE 行集合中合并 prompt cache usage。
fn merge_stateful_cache_usage_from_lines<'a>(
    usage: &mut StatefulCacheUsage,
    lines: impl Iterator<Item = &'a str>,
) {
    for line in lines {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(data) {
            merge_stateful_cache_usage_from_value(usage, &value);
        }
    }
}

/// 合并 Anthropic usage 字段,同一响应多次 delta 时取最大值。
fn merge_stateful_cache_usage_from_value(
    usage: &mut StatefulCacheUsage,
    value: &serde_json::Value,
) {
    merge_stateful_cache_usage_from_usage_value(usage, value.get("usage"));
    merge_stateful_cache_usage_from_usage_value(usage, value.pointer("/message/usage"));
    merge_stateful_cache_usage_from_usage_value(usage, value.pointer("/delta/usage"));
}

/// 合并单个 Anthropic usage 对象。
fn merge_stateful_cache_usage_from_usage_value(
    usage: &mut StatefulCacheUsage,
    cache_usage: Option<&serde_json::Value>,
) {
    let Some(cache_usage) = cache_usage else {
        return;
    };
    if let Some(cache_read_input_tokens) = cache_usage
        .get("cache_read_input_tokens")
        .and_then(|tokens| tokens.as_u64())
    {
        usage.cache_read_input_tokens = usage.cache_read_input_tokens.max(cache_read_input_tokens);
    }
    if let Some(cache_creation_input_tokens) = cache_usage
        .get("cache_creation_input_tokens")
        .and_then(|tokens| tokens.as_u64())
    {
        usage.cache_creation_input_tokens = usage
            .cache_creation_input_tokens
            .max(cache_creation_input_tokens);
    }
    if let Some(cache_creation_ephemeral_5m_input_tokens) = cache_usage
        .get("cache_creation")
        .and_then(|cache_creation| cache_creation.get("ephemeral_5m_input_tokens"))
        .and_then(|tokens| tokens.as_u64())
    {
        usage.cache_creation_ephemeral_5m_input_tokens = usage
            .cache_creation_ephemeral_5m_input_tokens
            .max(cache_creation_ephemeral_5m_input_tokens);
    }
    if let Some(cache_creation_ephemeral_1h_input_tokens) = cache_usage
        .get("cache_creation")
        .and_then(|cache_creation| cache_creation.get("ephemeral_1h_input_tokens"))
        .and_then(|tokens| tokens.as_u64())
    {
        usage.cache_creation_ephemeral_1h_input_tokens = usage
            .cache_creation_ephemeral_1h_input_tokens
            .max(cache_creation_ephemeral_1h_input_tokens);
    }
}

impl Drop for SlotGuardBody {
    fn drop(&mut self) {
        info!(
            "[耗时] 传输结束: {:.0}ms → {}",
            self.req_start.elapsed().as_millis(),
            self.account_name
        );
        // permit 的 Drop 负责归还 semaphore
        let _ = self.permit.take();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AssistantPrefillInterceptConfig, BootstrapModelOptionsMode, BootstrapProfileConfig,
        RateLimitRequestLogConfig, STATEFUL_USAGE_BUFFER_LIMIT, SignatureRetryStage,
        WarmupInterceptConfig, WarmupInterceptType, assistant_prefill_intercept_body,
        build_message_telemetry_context, build_warmup_intercept_sse, detect_warmup_intercept,
        extract_message_session_id, flush_stateful_cache_usage_buffer,
        format_rate_limit_request_capture, has_system_role_message,
        is_signature_related_error_body, is_signature_related_error_response_body,
        is_system_role_model_allowed, mock_warmup_intercept_json_response,
        parse_bootstrap_additional_model_options, parse_rate_limit_request_body_limit,
        parse_system_role_model_list, patch_bootstrap_json, redact_request_headers,
        redact_sensitive_text, redacted_request_body_for_log, rewrite_bootstrap_response,
        safe_body_summary, should_intercept_assistant_prefill, signature_retry_body_for_stage,
        strip_signature_sensitive_blocks_from_messages_request,
        strip_thinking_from_messages_request, system_role_model_error_body, truncate_log_text,
        update_stateful_cache_usage_from_bytes,
    };
    use crate::model::account::{Account, AccountAuthType, AccountStatus, BillingMode};
    use crate::service::rewriter::{ClientType, StatefulCacheUsage};
    use axum::body;
    use axum::http::{HeaderMap, StatusCode, header::CONTENT_ENCODING};
    use chrono::Utc;
    use serde_json::json;
    use std::collections::HashMap;
    use std::io::Write;

    fn all_warmup_intercepts_enabled() -> WarmupInterceptConfig {
        WarmupInterceptConfig {
            title_enabled: true,
            suggestion_enabled: true,
            haiku_probe_enabled: true,
        }
    }

    fn assistant_prefill_config(enabled: bool) -> AssistantPrefillInterceptConfig {
        AssistantPrefillInterceptConfig {
            enabled,
            models: parse_system_role_model_list("claude-fable-5,claude-opus-4-8,claude-opus-4-7"),
        }
    }

    fn bootstrap_config(mode: BootstrapModelOptionsMode) -> BootstrapProfileConfig {
        BootstrapProfileConfig {
            mode,
            additional_model_options: parse_bootstrap_additional_model_options(
                r#"[{"model":"claude-fable-5[1m]","name":"Fable","description":"Most capable for your hardest and longest-running tasks","disabled_reason":null}]"#,
            )
            .expect("bootstrap options"),
        }
    }

    fn bootstrap_body() -> serde_json::Value {
        json!({
            "client_data": null,
            "additional_model_options": null,
            "additional_model_costs": null,
            "oauth_account": {
                "account_uuid": "account-redacted",
                "account_email": "user@example.com",
                "organization_uuid": "org-redacted"
            },
            "cwk_cfg_key": null
        })
    }

    fn test_account() -> Account {
        Account {
            id: 42,
            name: "测试账号".into(),
            email: "user@example.com".into(),
            status: AccountStatus::Active,
            auth_type: AccountAuthType::SetupToken,
            setup_token: String::new(),
            access_token: String::new(),
            refresh_token: String::new(),
            expires_at: None,
            oauth_refreshed_at: None,
            auth_error: String::new(),
            proxy_url: String::new(),
            device_id: String::new(),
            canonical_env: json!({}),
            canonical_prompt: json!({}),
            canonical_process: json!({}),
            billing_mode: BillingMode::Strip,
            account_uuid: None,
            organization_uuid: None,
            subscription_type: None,
            concurrency: 3,
            priority: 50,
            rpm_limit: 0,
            rate_limited_at: None,
            rate_limit_reset_at: None,
            disable_reason: String::new(),
            auto_telemetry: false,
            auto_poll_usage: false,
            allow_1m_models: "opus".into(),
            telemetry_count: 0,
            usage_data: json!({}),
            usage_fetched_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn safe_body_summary_hides_raw_body() {
        let body = br#"{"private_text":"raw-prompt-marker","private_token":"raw-token-marker"}"#;
        let summary = safe_body_summary(body);

        assert!(summary.starts_with(&format!("{} bytes sha256:", body.len())));
        assert!(!summary.contains("raw-prompt-marker"));
        assert!(!summary.contains("raw-token-marker"));
    }

    #[test]
    fn assistant_prefill_intercept_matches_enabled_model_and_last_assistant() {
        let body = json!({
            "model": "claude-opus-4-8",
            "stream": false,
            "messages": [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "{"}
            ]
        });

        assert!(should_intercept_assistant_prefill(
            &body,
            &assistant_prefill_config(true)
        ));
    }

    #[test]
    fn assistant_prefill_intercept_ignores_disabled_unmatched_or_last_user() {
        let last_assistant = json!({
            "model": "claude-opus-4-8",
            "messages": [{"role": "assistant", "content": "prefill"}]
        });
        let unmatched_model = json!({
            "model": "claude-sonnet-4-5",
            "messages": [{"role": "assistant", "content": "prefill"}]
        });
        let last_user = json!({
            "model": "claude-opus-4-8",
            "messages": [{"role": "user", "content": "go"}]
        });

        assert!(!should_intercept_assistant_prefill(
            &last_assistant,
            &assistant_prefill_config(false)
        ));
        assert!(!should_intercept_assistant_prefill(
            &unmatched_model,
            &assistant_prefill_config(true)
        ));
        assert!(!should_intercept_assistant_prefill(
            &last_user,
            &assistant_prefill_config(true)
        ));
    }

    #[test]
    fn assistant_prefill_intercept_body_uses_stable_error_code() {
        let body = assistant_prefill_intercept_body("claude-opus-4-8");

        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert_eq!(body["error"]["code"], "assistant_prefill_intercepted");
        assert_eq!(body["model"], "claude-opus-4-8");
    }

    #[test]
    fn bootstrap_patch_passthrough_keeps_response_unchanged() {
        let mut body = bootstrap_body();

        let changed = patch_bootstrap_json(
            &mut body,
            "entrypoint=cli&model=claude-fable-5",
            &bootstrap_config(BootstrapModelOptionsMode::Passthrough),
        );

        assert!(!changed);
        assert!(body["client_data"].is_null());
        assert!(body["additional_model_options"].is_null());
        assert!(body["cwk_cfg_key"].is_null());
    }

    #[test]
    fn bootstrap_patch_configured_injects_fable_capture_shape() {
        let mut body = bootstrap_body();

        let changed = patch_bootstrap_json(
            &mut body,
            "entrypoint=cli&model=claude-fable-5",
            &bootstrap_config(BootstrapModelOptionsMode::Configured),
        );

        assert!(changed);
        assert_eq!(body["client_data"]["cedar_lagoon"]["claude-fable"], true);
        assert_eq!(body["client_data"]["cedar_lagoon"]["claude-mythos"], true);
        assert_eq!(
            body["additional_model_options"][0]["model"],
            "claude-fable-5[1m]"
        );
        assert_eq!(body["additional_model_options"][0]["name"], "Fable");
        assert_eq!(body["cwk_cfg_key"], "marigold");
        assert_eq!(body["oauth_account"]["account_uuid"], "account-redacted");
    }

    #[test]
    fn bootstrap_patch_configured_does_not_force_marigold_for_opus() {
        let mut body = bootstrap_body();

        patch_bootstrap_json(
            &mut body,
            "entrypoint=cli&model=claude-opus-4-8",
            &bootstrap_config(BootstrapModelOptionsMode::Configured),
        );

        assert!(body["cwk_cfg_key"].is_null());
        assert_eq!(body["client_data"]["cedar_lagoon"]["claude-fable"], true);
    }

    #[test]
    fn bootstrap_patch_hide_fable_removes_model_option_and_marigold() {
        let mut body = json!({
            "client_data": {"cedar_lagoon": {"claude-fable": true, "claude-mythos": true}},
            "additional_model_options": [
                {"model": "claude-fable-5[1m]", "name": "Fable"},
                {"model": "claude-opus-4-8", "name": "Opus"}
            ],
            "cwk_cfg_key": "marigold"
        });

        let changed = patch_bootstrap_json(
            &mut body,
            "entrypoint=cli&model=claude-fable-5",
            &bootstrap_config(BootstrapModelOptionsMode::HideFable),
        );

        assert!(changed);
        assert_eq!(body["client_data"]["cedar_lagoon"]["claude-fable"], false);
        assert_eq!(body["client_data"]["cedar_lagoon"]["claude-mythos"], true);
        assert_eq!(
            body["additional_model_options"],
            json!([{"model": "claude-opus-4-8", "name": "Opus"}])
        );
        assert!(body["cwk_cfg_key"].is_null());
    }

    #[tokio::test]
    async fn bootstrap_rewrite_decodes_gzip_and_returns_plain_json() {
        let body = serde_json::to_vec(&bootstrap_body()).expect("json");
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&body).expect("write gzip");
        let compressed = encoder.finish().expect("finish gzip");
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("transfer-encoding", "chunked".parse().unwrap());

        let response = rewrite_bootstrap_response(
            200,
            &headers,
            bytes::Bytes::from(compressed),
            "entrypoint=cli&model=claude-fable-5",
            &bootstrap_config(BootstrapModelOptionsMode::Configured),
        )
        .expect("rewrite");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(CONTENT_ENCODING).is_none());
        assert!(response.headers().get("transfer-encoding").is_none());
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );

        let body_bytes = body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("response body");
        let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json");
        assert_eq!(parsed["client_data"]["cedar_lagoon"]["claude-fable"], true);
        assert_eq!(parsed["cwk_cfg_key"], "marigold");
    }

    #[test]
    fn rate_limit_request_log_redacts_sensitive_headers() {
        let headers = HashMap::from([
            (
                "authorization".to_string(),
                "Bearer secret-token".to_string(),
            ),
            ("x-api-key".to_string(), "sk-ant-oat-secret".to_string()),
            ("user-agent".to_string(), "claude-code/2.1.169".to_string()),
        ]);

        let redacted = redact_request_headers(&headers);
        let serialized = serde_json::to_string(&redacted).expect("json");

        assert!(serialized.contains("***REDACTED***"));
        assert!(serialized.contains("claude-code/2.1.169"));
        assert!(!serialized.contains("secret-token"));
        assert!(!serialized.contains("sk-ant-oat-secret"));
    }

    #[test]
    fn rate_limit_request_log_redacts_sensitive_body_fields_and_truncates() {
        let body = br#"{
            "model":"claude-opus-4-8",
            "stream":false,
            "access_token":"raw-access-token",
            "messages":[{"role":"user","content":"hello"}],
            "metadata":{"refresh_token":"raw-refresh-token","password":"raw-password"}
        }"#;

        let redacted = redacted_request_body_for_log(body, 120);

        assert!(redacted.contains("***REDACTED***"));
        assert!(redacted.contains("...<truncated>"));
        assert!(!redacted.contains("raw-access-token"));
        assert!(!redacted.contains("raw-refresh-token"));
        assert!(!redacted.contains("raw-password"));
    }

    #[test]
    fn rate_limit_request_log_redacts_free_text_token_values() {
        let redacted = redact_sensitive_text(
            "authorization: Bearer raw-token token: raw-token-2 password=raw-password",
        );

        assert!(redacted.contains("***REDACTED***"));
        assert!(!redacted.contains("raw-token"));
        assert!(!redacted.contains("raw-token-2"));
        assert!(!redacted.contains("raw-password"));
    }

    #[test]
    fn rate_limit_request_capture_uses_actual_request_shape() {
        let headers = HashMap::from([
            (
                "authorization".to_string(),
                "Bearer secret-token".to_string(),
            ),
            ("anthropic-version".to_string(), "2023-06-01".to_string()),
        ]);
        let body = br#"{"model":"claude-opus-4-8","stream":false,"messages":[{"role":"user","content":"hello"}]}"#;

        let captured = format_rate_limit_request_capture(
            "/v1/messages",
            &test_account(),
            &headers,
            body,
            RateLimitRequestLogConfig {
                enabled: true,
                body_limit: 4096,
            },
        );

        assert!(captured.contains(r#""account_id":42"#));
        assert!(captured.contains(r#""model":"claude-opus-4-8""#));
        assert!(captured.contains(r#""stream":false"#));
        assert!(captured.contains("sha256:"));
        assert!(!captured.contains("secret-token"));
    }

    #[test]
    fn rate_limit_request_body_limit_parser_falls_back_for_invalid_values() {
        assert_eq!(parse_rate_limit_request_body_limit("32"), 32);
        assert_eq!(parse_rate_limit_request_body_limit("invalid"), 8192);
    }

    #[test]
    fn truncate_log_text_handles_zero_limit() {
        assert_eq!(truncate_log_text("hello", 0), "");
        assert_eq!(truncate_log_text("hello", 3), "hel...<truncated>");
    }

    #[test]
    fn warmup_intercept_detects_haiku_probe_from_flow_shape() {
        let body = json!({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "quota"}]
        });

        assert_eq!(
            detect_warmup_intercept(
                &body,
                ClientType::ClaudeCode,
                all_warmup_intercepts_enabled()
            ),
            Some(WarmupInterceptType::HaikuProbe)
        );
        assert_eq!(
            detect_warmup_intercept(&body, ClientType::API, all_warmup_intercepts_enabled()),
            None
        );
    }

    #[test]
    fn warmup_intercept_detects_suggestion_mode_last_user_message() {
        let body = json!({
            "model": "claude-opus-4-8",
            "stream": true,
            "messages": [
                {"role": "assistant", "content": "ok"},
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "[SUGGESTION MODE: Suggest what the user might naturally type next into Claude Code.]"}]
                }
            ]
        });

        assert_eq!(
            detect_warmup_intercept(
                &body,
                ClientType::ClaudeCode,
                all_warmup_intercepts_enabled()
            ),
            Some(WarmupInterceptType::Suggestion)
        );
    }

    #[test]
    fn warmup_intercept_detects_json_title_prompt_from_flow_shape() {
        let body = json!({
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 32000,
            "stream": true,
            "system": "You are Claude Code.\nGenerate a concise, sentence-case title (3-7 words) that captures the main topic or goal of this coding session. Return JSON with a single \"title\" field.",
            "output_config": {
                "format": {
                    "type": "json_schema",
                    "schema": {
                        "type": "object",
                        "properties": {"title": {"type": "string"}},
                        "required": ["title"]
                    }
                }
            },
            "messages": [{"role": "user", "content": [{"type": "text", "text": "<session>实现 MVP</session>"}]}]
        });

        assert_eq!(
            detect_warmup_intercept(
                &body,
                ClientType::ClaudeCode,
                all_warmup_intercepts_enabled()
            ),
            Some(WarmupInterceptType::JsonTitle)
        );
    }

    #[test]
    fn warmup_intercept_detects_legacy_title_and_warmup_prompts() {
        let legacy_title = json!({
            "model": "claude-haiku-4-5-20251001",
            "messages": [{
                "role": "user",
                "content": "Please write a 5-10 word title for the following conversation: hello"
            }]
        });
        let warmup = json!({
            "model": "claude-haiku-4-5-20251001",
            "messages": [{"role": "user", "content": "Warmup"}]
        });

        assert_eq!(
            detect_warmup_intercept(
                &legacy_title,
                ClientType::ClaudeCode,
                all_warmup_intercepts_enabled()
            ),
            Some(WarmupInterceptType::TextTitle)
        );
        assert_eq!(
            detect_warmup_intercept(
                &warmup,
                ClientType::ClaudeCode,
                all_warmup_intercepts_enabled()
            ),
            Some(WarmupInterceptType::TextTitle)
        );
    }

    #[test]
    fn warmup_intercept_does_not_match_generic_title_words() {
        let body = json!({
            "model": "claude-opus-4-8",
            "stream": true,
            "messages": [{
                "role": "user",
                "content": "请给 HTML 页面增加 <title> 和 meta description"
            }]
        });

        assert_eq!(
            detect_warmup_intercept(
                &body,
                ClientType::ClaudeCode,
                all_warmup_intercepts_enabled()
            ),
            None
        );
    }

    #[tokio::test]
    async fn warmup_json_title_mock_uses_json_text_content() {
        let request = json!({
            "model": "claude-haiku-4-5-20251001",
            "stream": false
        });

        let response =
            mock_warmup_intercept_json_response(WarmupInterceptType::JsonTitle, &request)
                .expect("mock response");
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("body");
        let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).expect("json");

        assert_eq!(body_json["id"], "msg_mock_title");
        assert_eq!(
            body_json["content"][0]["text"],
            "{\"title\":\"New Conversation\"}"
        );
        assert_eq!(body_json["stop_reason"], "end_turn");
    }

    #[test]
    fn warmup_stream_mock_contains_anthropic_sse_events() {
        let request = json!({
            "model": "claude-opus-4-8",
            "stream": true
        });

        let body = build_warmup_intercept_sse(WarmupInterceptType::Suggestion, &request)
            .expect("sse body");

        assert!(body.contains("event: message_start"));
        assert!(body.contains("event: content_block_delta"));
        assert!(body.contains(r#""text":"""#));
        assert!(body.contains(r#""stop_reason":"end_turn""#));
        assert!(body.contains("event: message_stop"));
    }

    #[test]
    fn stateful_cache_usage_parser_reads_sse_usage() {
        let mut usage = StatefulCacheUsage::default();
        let mut buffer = String::new();

        update_stateful_cache_usage_from_bytes(
            &mut usage,
            &mut buffer,
            br#"event: message_delta
data: {"type":"message_delta","usage":{"cache_read_input_tokens":70513,"cache_creation_input_tokens":148518,"cache_creation":{"ephemeral_5m_input_tokens":12,"ephemeral_1h_input_tokens":34}}}

"#,
        );
        update_stateful_cache_usage_from_bytes(
            &mut usage,
            &mut buffer,
            br#"data: {"type":"message_delta","usage":{"cache_read_input_tokens":65001,"cache_creation_input_tokens":160000,"cache_creation":{"ephemeral_5m_input_tokens":56,"ephemeral_1h_input_tokens":78}}}"#,
        );
        flush_stateful_cache_usage_buffer(&mut usage, &mut buffer);

        assert_eq!(usage.cache_read_input_tokens, 70_513);
        assert_eq!(usage.cache_creation_input_tokens, 160_000);
        assert_eq!(usage.cache_creation_ephemeral_5m_input_tokens, 56);
        assert_eq!(usage.cache_creation_ephemeral_1h_input_tokens, 78);
    }

    #[test]
    fn stateful_cache_usage_parser_reads_json_usage() {
        let mut usage = StatefulCacheUsage::default();
        let mut buffer = String::new();

        update_stateful_cache_usage_from_bytes(
            &mut usage,
            &mut buffer,
            br#"{"id":"msg_1","usage":{"cache_read_input_tokens":12,"cache_creation_input_tokens":34}}"#,
        );

        assert_eq!(usage.cache_read_input_tokens, 12);
        assert_eq!(usage.cache_creation_input_tokens, 34);
    }

    #[test]
    fn stateful_cache_usage_parser_reads_nested_stream_usage() {
        let mut usage = StatefulCacheUsage::default();
        let mut buffer = String::new();

        update_stateful_cache_usage_from_bytes(
            &mut usage,
            &mut buffer,
            br#"data: {"type":"message_start","message":{"usage":{"cache_read_input_tokens":73307,"cache_creation_input_tokens":160619}}}
data: {"type":"message_delta","delta":{"usage":{"cache_read_input_tokens":73308,"cache_creation_input_tokens":160700}}}
"#,
        );

        assert_eq!(usage.cache_read_input_tokens, 73_308);
        assert_eq!(usage.cache_creation_input_tokens, 160_700);
    }

    #[test]
    fn stateful_cache_usage_parser_reads_gzip_side_tap() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(
                br#"data: {"type":"message_delta","usage":{"cache_read_input_tokens":73310,"cache_creation_input_tokens":153804}}
"#,
            )
            .expect("write gzip");
        let compressed = encoder.finish().expect("finish gzip");
        let mut usage = StatefulCacheUsage::default();

        super::merge_stateful_cache_usage_from_compressed_side_tap(
            &mut usage,
            &["gzip".to_string()],
            &compressed,
            false,
        );

        assert_eq!(usage.cache_read_input_tokens, 73_310);
        assert_eq!(usage.cache_creation_input_tokens, 153_804);
    }

    #[test]
    fn stateful_cache_usage_parser_keeps_split_sse_line() {
        let mut usage = StatefulCacheUsage::default();
        let mut buffer = String::new();

        update_stateful_cache_usage_from_bytes(
            &mut usage,
            &mut buffer,
            br#"data: {"type":"message_delta","usage":{"cache_read_input_tokens":"#,
        );
        assert_eq!(usage.cache_read_input_tokens, 0);
        update_stateful_cache_usage_from_bytes(
            &mut usage,
            &mut buffer,
            br#"55,"cache_creation_input_tokens":89}}
"#,
        );

        assert_eq!(usage.cache_read_input_tokens, 55);
        assert_eq!(usage.cache_creation_input_tokens, 89);
        assert!(buffer.is_empty());
    }

    #[test]
    fn stateful_cache_usage_buffer_trims_on_utf8_boundary() {
        let mut usage = StatefulCacheUsage::default();
        let prefix_len = 1;
        let mut buffer = "a".repeat(prefix_len);
        buffer.push('你');
        buffer.push_str(&"b".repeat(STATEFUL_USAGE_BUFFER_LIMIT - 3));

        let raw_keep_from = buffer.len() + 1 - STATEFUL_USAGE_BUFFER_LIMIT;
        assert!(!buffer.is_char_boundary(raw_keep_from));

        update_stateful_cache_usage_from_bytes(&mut usage, &mut buffer, b"b");

        assert!(buffer.is_char_boundary(0));
        assert!(buffer.len() <= STATEFUL_USAGE_BUFFER_LIMIT);
    }

    #[test]
    fn signature_error_detector_matches_anthropic_and_structural_errors() {
        assert!(is_signature_related_error_body(
            br#"{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid `signature` in `thinking` block"}}"#
        ));
        assert!(is_signature_related_error_body(
            br#"{"error":{"message":"Corrupted thought_signature."}}"#
        ));
        assert!(is_signature_related_error_body(
            br#"{"error":{"message":"Expected `thinking` or `redacted_thinking`, but found `text`"}}"#
        ));
        assert!(is_signature_related_error_body(
            br#"{"error":{"message":"invalid request","status":"INVALID_ARGUMENT","details":[{"reason":"Corrupted thought_signature."}]}}"#
        ));
        assert!(!is_signature_related_error_body(
            br#"{"error":{"message":"model is overloaded"}}"#
        ));
    }

    #[test]
    fn signature_error_detector_matches_compressed_upstream_body() {
        let body = br#"{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid `signature` in `thinking` block"}}"#;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());

        assert!(is_signature_related_error_response_body(
            &compressed,
            &headers
        ));
    }

    #[test]
    fn strip_thinking_retry_converts_thinking_and_keeps_tools() {
        let body = br#"{
            "model":"claude-opus-4-8",
            "thinking":{"type":"enabled","budget_tokens":1024},
            "messages":[
                {
                    "role":"assistant",
                    "content":[
                        {"type":"thinking","thinking":"secret plan","signature":"bad"},
                        {"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}
                    ]
                },
                {
                    "role":"user",
                    "content":[
                        {"type":"redacted_thinking","data":"opaque"},
                        {"type":"text","text":"ok"}
                    ]
                }
            ]
        }"#;

        let stripped = strip_thinking_from_messages_request(body).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&stripped).unwrap();

        assert!(value.get("thinking").is_none());
        let assistant_blocks = value["messages"][0]["content"].as_array().unwrap();
        assert_eq!(
            assistant_blocks[0],
            json!({"type":"text","text":"secret plan"})
        );
        assert_eq!(assistant_blocks[1]["type"], "tool_use");
        let user_blocks = value["messages"][1]["content"].as_array().unwrap();
        assert_eq!(user_blocks, &vec![json!({"type":"text","text":"ok"})]);
    }

    #[test]
    fn strip_thinking_retry_removes_thinking_dependent_context_strategy() {
        let body = br#"{
            "model":"claude-opus-4-8",
            "thinking":{"type":"enabled","budget_tokens":1024},
            "context_management":{
                "edits":[
                    {"type":"clear_thinking_20251015","keep":"all"},
                    {"type":"other_strategy","keep":"all"}
                ]
            },
            "messages":[
                {"role":"assistant","content":[{"type":"thinking","thinking":"secret","signature":"bad"}]}
            ]
        }"#;

        let stripped = strip_thinking_from_messages_request(body).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&stripped).unwrap();

        assert!(value.get("thinking").is_none());
        let edits = value["context_management"]["edits"].as_array().unwrap();
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0]["type"], "other_strategy");
    }

    #[test]
    fn strip_signature_sensitive_retry_converts_tools_and_fills_empty_content() {
        let body = br#"{
            "model":"claude-opus-4-8",
            "messages":[
                {
                    "role":"assistant",
                    "content":[
                        {"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}},
                        {"type":"redacted_thinking","data":"opaque"}
                    ]
                },
                {
                    "role":"user",
                    "content":[
                        {"type":"tool_result","tool_use_id":"t1","content":"ok","is_error":false}
                    ]
                },
                {
                    "role":"assistant",
                    "content":[
                        {"type":"redacted_thinking","data":"only"}
                    ]
                }
            ]
        }"#;

        let stripped = strip_signature_sensitive_blocks_from_messages_request(body).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&stripped).unwrap();

        let assistant_blocks = value["messages"][0]["content"].as_array().unwrap();
        assert_eq!(assistant_blocks.len(), 1);
        assert_eq!(assistant_blocks[0]["type"], "text");
        assert!(
            assistant_blocks[0]["text"]
                .as_str()
                .unwrap()
                .contains("(tool_use)")
        );
        assert!(
            assistant_blocks[0]["text"]
                .as_str()
                .unwrap()
                .contains("name=Bash")
        );

        let user_blocks = value["messages"][1]["content"].as_array().unwrap();
        assert_eq!(user_blocks.len(), 1);
        assert_eq!(user_blocks[0]["type"], "text");
        assert!(
            user_blocks[0]["text"]
                .as_str()
                .unwrap()
                .contains("(tool_result)")
        );

        let empty_blocks = value["messages"][2]["content"].as_array().unwrap();
        assert_eq!(
            empty_blocks,
            &vec![json!({"type":"text","text":"(content removed)"})]
        );
    }

    #[test]
    fn signature_retry_stages_run_from_thinking_to_tools() {
        let body = br#"{
            "model":"claude-opus-4-8",
            "messages":[{
                "role":"assistant",
                "content":[
                    {"type":"thinking","thinking":"secret plan","signature":"bad"},
                    {"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}
                ]
            }]
        }"#;

        let stages = SignatureRetryStage::ordered();
        assert_eq!(
            stages,
            [
                SignatureRetryStage::ThinkingOnly,
                SignatureRetryStage::ThinkingAndTools
            ]
        );

        let thinking_only: serde_json::Value =
            serde_json::from_slice(&signature_retry_body_for_stage(body, stages[0]).unwrap())
                .unwrap();
        assert_eq!(
            thinking_only["messages"][0]["content"][0],
            json!({"type":"text","text":"secret plan"})
        );
        assert_eq!(
            thinking_only["messages"][0]["content"][1]["type"],
            "tool_use"
        );

        let thinking_and_tools: serde_json::Value =
            serde_json::from_slice(&signature_retry_body_for_stage(body, stages[1]).unwrap())
                .unwrap();
        assert_eq!(
            thinking_and_tools["messages"][0]["content"][0],
            json!({"type":"text","text":"secret plan"})
        );
        assert_eq!(
            thinking_and_tools["messages"][0]["content"][1]["type"],
            "text"
        );
        assert!(
            thinking_and_tools["messages"][0]["content"][1]["text"]
                .as_str()
                .unwrap()
                .contains("(tool_use)")
        );
    }

    #[test]
    fn message_context_extracts_safe_counts_and_session_id() {
        let original_body = json!({
            "model": "claude-sonnet-4-20250514",
            "stream": true,
            "tools": [{"name": "Read"}, {"name": "Edit"}],
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "raw-prompt-marker"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "redacted"}},
                    {"type": "input_file", "file_id": "file_123"}
                ]
            }]
        });
        let rewritten_body = json!({
            "metadata": {
                "user_id": "{\"session_id\":\"session-from-user-id\"}"
            },
            "system": [
                {"type": "text", "text": "system prompt body"}
            ]
        });

        let context = build_message_telemetry_context(
            &original_body,
            &rewritten_body,
            111,
            222,
            ClientType::ClaudeCode,
            1,
            "claude-code-20250219".into(),
        );

        assert_eq!(context.model, "claude-sonnet-4-20250514");
        assert_eq!(context.session_id.as_deref(), Some("session-from-user-id"));
        assert_eq!(context.request_body_bytes, 111);
        assert_eq!(context.rewritten_body_bytes, 222);
        assert!(context.stream);
        assert_eq!(context.tool_count, 2);
        assert_eq!(context.attachment_count, 2);
        assert_eq!(context.system_prompt_block_count, 1);
        assert_eq!(context.client_type, "claude_code");
        assert_eq!(context.attempt, 1);
        assert_eq!(context.betas, "claude-code-20250219");
    }

    #[test]
    fn message_context_ignores_removed_internal_session_marker() {
        let rewritten_body = json!({
            "metadata": {
                "_session_id": "fallback-session"
            },
            "system": "system prompt body"
        });

        assert_eq!(extract_message_session_id(&rewritten_body), None);

        let context = build_message_telemetry_context(
            &json!({}),
            &rewritten_body,
            10,
            20,
            ClientType::API,
            0,
            String::new(),
        );

        assert_eq!(context.session_id, None);
        assert_eq!(context.system_prompt_block_count, 1);
        assert_eq!(context.client_type, "api");
    }

    #[test]
    fn system_role_guard_detects_messages_role_system() {
        let body = json!({
            "model": "claude-opus-4-7",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "system", "content": "runtime reminder"}
            ]
        });

        assert!(has_system_role_message(&body));
        assert!(!has_system_role_message(&json!({
            "messages": [{"role": "assistant", "content": "ok"}]
        })));
        assert!(!has_system_role_message(
            &json!({"system": "top level only"})
        ));
    }

    #[test]
    fn system_role_model_list_uses_exact_trimmed_matches() {
        let allowed =
            parse_system_role_model_list(" claude-opus-4-8,claude-sonnet-4-6,,claude.test:model ");

        assert_eq!(
            allowed,
            vec![
                "claude-opus-4-8".to_string(),
                "claude-sonnet-4-6".to_string(),
                "claude.test:model".to_string()
            ]
        );
        assert!(is_system_role_model_allowed("claude-opus-4-8", &allowed));
        assert!(!is_system_role_model_allowed("opus", &allowed));
        assert!(!is_system_role_model_allowed("claude-opus-4-7", &allowed));
    }

    #[test]
    fn system_role_error_body_includes_model_and_allowed_list() {
        let allowed = vec!["claude-opus-4-8".to_string()];
        let body = system_role_model_error_body("claude-opus-4-7", &allowed);

        assert_eq!(
            body,
            json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "messages[].role=system is not allowed for this model",
                    "code": "system_role_model_not_allowed"
                },
                "model": "claude-opus-4-7",
                "allowed_system_role_models": ["claude-opus-4-8"]
            })
        );
    }
}
