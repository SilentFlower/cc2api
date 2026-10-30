use serde::{Deserialize, Serialize};

/// 已验证 TLS 模板的稳定标识；版本绑定和内部传输只引用注册过的值。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TlsProfileId {
    /// 原版 Claude Code 2.1.293 主请求，亦作为内置兼容默认值。
    #[default]
    #[serde(rename = "claude-code-2.1.293")]
    ClaudeCode293,
    /// 只用于验证选择和连接隔离的扰动画像，不编译进正式二进制。
    #[cfg(test)]
    #[serde(rename = "test-reordered")]
    TestReordered,
}

impl TlsProfileId {
    /// 返回内部传输和缓存使用的稳定标识。@return 注册表 ID。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClaudeCode293 => "claude-code-2.1.293",
            #[cfg(test)]
            Self::TestReordered => "test-reordered",
        }
    }
}
