# 请求级诊断

网关记录已通过 API Token 鉴权的精确路径 `/v1/messages`。每个请求有一个内部 UUID，每次实际提交有独立尝试 UUID；本地拒绝、模拟和缓存命中保留请求记录，不虚构上游尝试。诊断独立于自动遥测及费用表，不重复计费。

## 从 new-api 定位请求

响应 `X-CC2API-Request-Id` 始终是内部请求 UUID。已有 `X-Request-Id` 保留，缺少时补内部 UUID；new-api 可以沿用现有逻辑保存响应 ID。用该 ID 查询列表的 `correlation_id`，再按列表中的 `request_id` 读取详情。上游响应的 `request-id` / `x-request-id` 也可以反查。

内部请求 ID、尝试 ID、上游 ID 分开保存。v3 sender 的既有 observations 使用尝试 UUID；业务请求不会向上游增加诊断头或 ID。

## 管理员查询

两个接口沿用管理鉴权：`x-api-key: <管理员密码>` 或 `Authorization: Bearer <管理员密码>`。

| 接口 | 返回 |
| --- | --- |
| `GET /admin/request-diagnostics` | `items`、`total`、`page`、`page_size`、`health` |
| `GET /admin/request-diagnostics/:request_id` | `request` 和按序号排列的 `attempts` |

列表支持以下查询参数，时间按请求开始时间筛选。

| 参数 | 含义 |
| --- | --- |
| `page` / `page_size` | 从 1 开始；默认 1 / 50，每页上限 200 |
| `account_id` | 正整数；匹配请求选中账号或任一次尝试账号 |
| `correlation_id` | 内部请求 UUID、最终响应 ID 或上游 ID，最多 128 字节 |
| `outcome` | 下方结果枚举 |
| `since` / `until` | RFC3339 时间，左闭右开；含时区，转换为 UTC 毫秒 |

非法参数返回 400，不存在的详情返回 404；存储不可用返回错误，不伪装为空列表。列表 GET 只读，不触发清理或上游请求。

## 字段与判读

| 位置 | 内容 |
| --- | --- |
| `request` | 根 ID、开始/结束时间、下游状态、最终选中账号、响应关联 ID、结果、错误阶段、尝试次数 |
| `attempts[]` | 尝试 ID/序号、账号、冻结的实际画像与选择模式、时间、首字节/总耗时、实际上游状态及 ID |
| `parameters` | 最终请求模型、`max_tokens`、thinking 类型与预算、effort、stream、工具数量 |
| `sessions` | 真实与上游 session 的散列、既有映射动作、线程前驱消息散列 |
| 响应摘要 | 响应模型、响应消息散列、`stop_reason`、`tokens`、`partial`、结果、错误类型与重试原因 |

`tokens` 包含输入、输出、缓存读、缓存写总量以及 5m/1h 缓存写明细。重复 usage 采用每字段累计最大值，明细不能再次加入总量；未知字段为 `null`。画像和参数来自本次实际发送，不在结束时重读全局配置。

session 和消息使用带账号、标识类型域的 SHA256 摘要（取前 128 位）。同账号、同类型可关联，不能跨账号比较；线程前驱和响应消息使用同一消息域。真实 session 与上游 session 散列不同表示发生映射；目标散列随时间变化可用于定位重映射，但诊断不会额外解析或刷新池 TTL。

| `outcome` | 含义 |
| --- | --- |
| `in_progress` | 尚未终结，或终态未能落库；不能直接断定请求仍运行 |
| `success` | 正常完整传输，观察到必要用量，无流内错误 |
| `local_rejected` / `local_response` | 本地拒绝 / 其它本地响应 |
| `mock` / `cache_hit` | 模拟 / 缓存响应，无本次上游推理 |
| `upstream_rejected` | 实际上游 HTTP 400 及以上 |
| `upstream_error` | HTTP 200 内出现受控类型的错误事件 |
| `transport_error` | 发送边界失败；上游是否接收未知，HTTP 状态不补零 |
| `body_error` / `cancelled` | 读取错误 / 客户端或调用任务取消 |
| `incomplete` | 响应采样不完整、缺用量、SSE 未见结束或解析失败 |

根结果代表整个请求；中间 401/400 不覆盖后续尝试结果。根状态是下游状态，尝试状态是实际收到的上游状态。`partial=true` 表示用量或响应观察不完整，不能据此认定上游没有生成完整结果。`ttft_ms` 实际测量响应头到达时间，不代表首个模型文本 token。

重试原因包括 `oauth_recovery`、`credentials_rejected`、`signature_repair`、`transient_rate_limit` 和 `account_rate_limit`。错误阶段区分准入、请求、凭据、认证恢复、账号选择、Hello 探针、传输、上游响应头/响应体和下游响应体；不保存任意错误文本。采样错误类型为 `sampling_limit`、`decode_error` 或 `invalid_payload`；上游错误类型只保留白名单，未知类型归为 `unknown_upstream_error`。

## 留存、隐私与故障信号

SQLite/PostgreSQL 使用独立请求与尝试表，启动幂等建表；后台按开始时间清理八天前记录，每小时运行一次。已提交记录重启保留。回滚旧程序可保留追加表，不影响原费用记录。

写入队列容量 1024，入队不等待数据库；每个摘要上限 16 KiB，响应解析/压缩采样上限 1 MiB。进程退出、队列饱和或存储故障可能丢失尚未提交的记录，不能把诊断数量作为绝对完整的账单。

`health.dropped_records` 是丢弃的写入事件数（不是独立请求数），`write_failures` 是写入/清理失败次数，`observation_failures` 是采样超限、解码或解析失败的尝试数。三者都是当前进程计数，重启清零；相应日志仅包含固定文案、计数和内部请求 UUID。查询本身失败时，应查服务日志确认数据库状态。

不保存正文、工具参数、完整 headers、凭据、Cookie、邮箱、设备 ID、账号 UUID 或完整 session ID。标准 UUID 和 `req_` 字母数字响应 ID 可原样关联，其它头值只保存 SHA256；按原头值筛选时执行相同规范化。此功能从启用后采集，历史缺失字段不回填。
