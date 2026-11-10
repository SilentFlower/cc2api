//! 原有转发的跨平台构建保持可用；原生能力限定已交付的 Linux x64。

use axum::response::Response;
use std::fs::File;
use std::sync::Arc;

use crate::error::AppError;
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
    /// 在创建任何作业之前拒绝不支持的平台。
    /// @param config 当前 sender 配置。
    /// @param submission 受信任网关的冻结请求。
    /// @return 受控 503。
    pub async fn submit(
        self: &Arc<Self>,
        _config: SenderConfig,
        _submission: NativeSubmission,
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
