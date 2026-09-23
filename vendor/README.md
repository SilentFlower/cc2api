# HTTP/1 请求头大小写补丁

当前 reqwest 0.12.4 会丢失原始头名大小写；hyper 1.9.0 的 HTTP/1 编码器已有 HeaderCaseMap，但不能从请求调用方构造。为复用现有 TLS、HTTP/SOCKS 代理、连接池、超时与流式响应，仅开放经过校验的大小写映射，并显式传递请求级选项。

## 来源与许可

源码来自本机 Cargo 缓存中的 crates.io 发布归档，保留各目录内原始许可证和 `.cargo_vcs_info.json`。不修改 Cargo 缓存。除对应 `.patch` 中的差异外，源码与发布归档一致；只省略 Cargo 缓存标记 `.cargo-ok` / `.cargo-checksum.json`。

- `hyper-1.9.0`：crates.io 原始归档 SHA-256 `6299f016b246a94207e63da54dbe807655bf9e00044f73ded42c3ac5305fbcca`；上游提交 `0d6c7d5469baa09e2fb127ee3758a79b3271a4f0`。修改文件：`src/ext/mod.rs`。
- `reqwest-0.12.4`：crates.io 原始归档 SHA-256 `566cafdd92868e0939d3fb961bd0dc25fcfaaed179291093b3d43e6b3150ea10`；上游提交 `de5dbb1ab849cc301dcefebaeabdf4ce2e0f1e53`。修改文件：`src/async_impl/client.rs`, `src/async_impl/request.rs`。

## 补丁边界

- Hyper：公开 HeaderCaseMap，新增 from_names；使用 HeaderName 校验名称，不能借大小写映射注入 CRLF 或改写 header 值。
- Reqwest：新增 RequestBuilder::http1_header_casing，仅显式调用的请求启用；初次发送、try_clone、HTTP Request 转换、内部重试和重定向均携带映射。映射只控制已有头的拼写，不能恢复被重定向策略删除的 Authorization。
- HTTP/2 沿用规范化 HeaderMap；未启用选项的请求保持上游默认行为。gateway 的业务转发与 count_tokens 启用，event_logging/eval 不启用。
- Content-Length 仅提供名称映射，值由原网络栈根据最终请求体计算。

## 升级与验证

根 Cargo.toml 精确锁定版本，并通过 patch.crates-io 引用本目录。Docker 的依赖缓存层同时复制 vendor。升级时先对新发布源码审查接口是否已原生支持，再重放最小补丁，更新归档摘要、Cargo.lock 和补丁文件；不替换 TLS 实现。

`cargo test --test http1_header_casing_test` 检查原始 TCP 字节，包括混合大小写及顺序、Content-Length、clone/转换、跨地址重定向的敏感头删除、HTTP 代理、默认行为隔离和非法名字拒绝。`cargo test` 继续覆盖 gateway 业务处理与现有 CCH/版本规则。
