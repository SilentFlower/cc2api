//! v3 凭据长期归属及交接协议；回执只存元数据和摘要。

use serde::{Deserialize, Serialize};

/// 一个固定账号环境的受信任归属证明。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CredentialIdentity {
    pub deployment_id: String,
    pub environment_id: String,
    pub bench_account_id: i64,
    pub device_id: String,
    pub email: String,
    pub account_uuid: String,
    pub organization_uuid: Option<String>,
}

/// 交接中的凭据，仅允许在受信任请求及现有凭据存储中出现。
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialMaterial {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
}

/// 受信任管理端的单次交接操作，未知字段不能改变协议语义。
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialOwnershipInput {
    pub protocol: u8,
    pub operation_id: String,
    pub action: String,
    pub direction: String,
    pub identity: CredentialIdentity,
    pub expected_generation: Option<String>,
    pub source_refresh_sha256: Option<String>,
    pub lease_owner: Option<String>,
    pub credentials: Option<CredentialMaterial>,
}

/// 持久刷新权；phase 未就绪时不允许两端自行推断可用。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CredentialOwnership {
    pub protocol: u8,
    pub account_id: i64,
    pub identity: CredentialIdentity,
    pub owner: String,
    pub phase: String,
    pub generation: String,
    pub operation_id: String,
    pub direction: String,
    pub lease_owner: Option<String>,
}

impl CredentialOwnership {
    /// 判断网关是否持有已确认刷新权。@return 仅 gateway/ready 为真。
    pub fn gateway_ready(&self) -> bool {
        self.protocol == 1 && self.owner == "gateway" && self.phase == "ready"
    }
}

/// 列表和调度需要的无身份归属摘要；坏记录只隔离所属账号。
pub struct CredentialOwnershipSummary {
    pub protocol: u8,
    pub owner: String,
    pub phase: String,
}

impl CredentialOwnershipSummary {
    /// 判断摘要是否允许网关准入。@return 协议已知且 gateway/ready 时为真。
    pub fn gateway_ready(&self) -> bool {
        self.protocol == 1 && self.owner == "gateway" && self.phase == "ready"
    }
}

/// 不含令牌的幂等交接回执，原来源与请求摘要防止跨账号重试。
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CredentialHandoffReceipt {
    pub operation_id: String,
    pub account_id: i64,
    pub identity: CredentialIdentity,
    pub direction: String,
    pub request_hash: String,
    pub source_owner: String,
    pub source_phase: String,
    pub generation: String,
    pub stage: String,
    pub material_sha256: Option<String>,
    pub import_operation_id: Option<String>,
}

/// 同事务保存归属与回执的条件变更，不持有额外凭据副本。
pub struct CredentialOwnershipChange {
    pub expected: Option<CredentialOwnership>,
    pub next: CredentialOwnership,
    pub receipt: CredentialHandoffReceipt,
}
