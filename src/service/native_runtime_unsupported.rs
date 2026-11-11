//! 原有转发的跨平台构建保持可用；原生能力限定已交付的 Linux x64。

use axum::response::Response;
use std::fs::File;
use std::sync::Arc;
use tokio::time::Instant;

use crate::error::AppError;
use crate::service::native_contract::{
    NativePreparationControl, NativePreparationStatus, NativeReadinessStatus,
};
pub use crate::service::native_contract::{
    NativeSenderConfig, NativeSubmission, POOL_MEMORY_MIB, WORKER_MEMORY_MIB,
};
use crate::service::runtime_transport::SenderConfig;

/// 在能力协商阶段拒绝其它平台。@param config 部署配置。@return 受控 503。
pub(crate) fn verify_distribution(_config: &NativeSenderConfig) -> Result<File, AppError> {
    Err(AppError::ServiceUnavailable(
        "原生运行时需要 Linux x64 平台".into(),
    ))
}

/// 其它平台的明确失败入口，保持共享 sender 合同可编译。
#[derive(Default)]
pub struct NativePool;

impl NativePool {
    /// 在实时检查阶段明确拒绝不支持的平台。
    /// @param config sender 配置。
    /// @param identity 当前账号身份。
    /// @return 受控平台错误。
    pub(crate) async fn readiness(
        self: &Arc<Self>,
        _config: &SenderConfig,
        _identity: &NativeSubmission,
    ) -> Result<NativeReadinessStatus, AppError> {
        Err(AppError::ServiceUnavailable(
            "原生运行时需要 Linux x64 平台".into(),
        ))
    }

    /// 在能力查询时明确拒绝不支持的平台。
    /// @param config 部署配置。
    /// @return 受控平台错误。
    pub(crate) fn verify(&self, config: &NativeSenderConfig) -> Result<(), AppError> {
        verify_distribution(config).map(|_| ())
    }

    /// 在准备阶段明确拒绝不支持的平台。
    /// @param config sender 配置。
    /// @param identity 账号启动身份。
    /// @param deadline 准备截止。
    /// @return 受控平台错误。
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        _config: SenderConfig,
        _identity: NativeSubmission,
        _deadline: Instant,
    ) -> Result<NativePreparationStatus, AppError> {
        Err(AppError::ServiceUnavailable(
            "原生运行时需要 Linux x64 平台".into(),
        ))
    }

    /// 在切换预热阶段明确拒绝不支持的平台。
    /// @param config sender 配置。
    /// @param identity 当前账号绑定。
    /// @param deadline 准备截止。
    /// @return 受控平台错误。
    pub(crate) async fn prepare_switch(
        self: &Arc<Self>,
        config: SenderConfig,
        identity: NativeSubmission,
        deadline: Instant,
    ) -> Result<NativePreparationStatus, AppError> {
        self.prepare(config, identity, deadline).await
    }

    /// 其它平台没有可取消的原生准备。
    /// @param control 控制标识。
    /// @return false。
    pub(crate) fn cancel_preparation(&self, _control: &NativePreparationControl) -> bool {
        false
    }

    /// 在创建任何作业之前拒绝不支持的平台。
    /// @param config 当前 sender 配置。
    /// @param submission 受信任网关的冻结请求。
    /// @return 受控 503。
    pub async fn submit(
        self: &Arc<Self>,
        config: SenderConfig,
        submission: NativeSubmission,
    ) -> Result<Response, AppError> {
        self.submit_validated(config, submission, Instant::now())
            .await
    }

    /// 接收已核验请求时同样在创建作业前拒绝不支持的平台。
    /// @param config 当前 sender 配置。
    /// @param submission 已核验的冻结请求。
    /// @param deadline 接收边界冻结的单调截止。
    /// @return 受控 503。
    pub(crate) async fn submit_validated(
        self: &Arc<Self>,
        _config: SenderConfig,
        _submission: NativeSubmission,
        _deadline: Instant,
    ) -> Result<Response, AppError> {
        Err(AppError::ServiceUnavailable(
            "原生运行时需要 Linux x64 平台".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_platform_is_explicit_before_execution() {
        let config = NativeSenderConfig {
            kernel_generation: None,
            release: None,
            binary_path: "/missing".into(),
            work_root: "/missing".into(),
            memory_mib: 2432,
            max_workers: 6,
        };
        assert!(matches!(
            verify_distribution(&config),
            Err(AppError::ServiceUnavailable(_))
        ));
    }
}
