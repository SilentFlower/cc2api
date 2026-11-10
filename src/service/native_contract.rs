//! 原生 sender 的跨平台私有合同；平台执行实现由 native_runtime 选择。

use crate::error::AppError;
use crate::service::runtime_transport::PreparedRequest;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;

/// 共享执行映像及 sender 的基础 RAM 份额，包含首次解压余量。
pub const POOL_MEMORY_MIB: u64 = 256;
/// 每个独立进程的堆和缓冲预算；共享代码计入池基础份额。
pub const WORKER_MEMORY_MIB: u64 = 192;

/// 部署侧冻结的原生池配置；预算必须与 sender 的 cgroup 上限一致。
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSenderConfig {
    pub binary_path: String,
    pub work_root: String,
    /// 显式零表示账号无限内存，正数沿用加权容量公式。
    pub memory_mib: u64,
    pub max_workers: usize,
}

/// 受信任网关冻结的原始请求及当前授权；此类型禁止派生 Debug。
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSubmission {
    pub request: PreparedRequest,
    pub input: Value,
    pub bearer: String,
    pub device_id: String,
    pub account_uuid: Option<String>,
    pub organization_uuid: Option<String>,
    pub email: String,
    pub concurrency: i32,
}

impl NativeSenderConfig {
    /// 校验当前平台、资源模式及已登记分发体。
    /// @return 保持打开的精确执行文件，或明确的准备错误。
    pub fn verify(&self) -> Result<File, AppError> {
        super::native_runtime::verify_distribution(self)
    }
}
