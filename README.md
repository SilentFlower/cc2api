<div align="center">

# CC-Bridge

### Claude Code Anti-Detection Gateway & Account Pool Manager

[![Release](https://img.shields.io/github/v/release/MamoWorks/cc-bridge?style=flat-square&color=blue)](https://github.com/MamoWorks/cc-bridge/releases)
[![Build](https://img.shields.io/github/actions/workflow/status/MamoWorks/cc-bridge/release.yml?style=flat-square)](https://github.com/MamoWorks/cc-bridge/actions)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-green?style=flat-square)](./craftls/LICENSE)
[![Rust](https://img.shields.io/badge/rust-%E2%89%A51.82-orange?style=flat-square&logo=rust)](https://www.rust-lang.org/)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-blue?style=flat-square&logo=docker)](https://ghcr.io/mamoworks/claude-code-gateway)

<br/>

**基于 Rust 的高性能 Claude Code 反检测网关** — 将网关转发、账号调度、令牌鉴权、用量管理和 Web 管理后台整合到单一二进制文件中。

[快速开始](#-快速开始) &bull; [配置说明](#-配置说明) &bull; [API 文档](#-http-api) &bull; [部署指南](#-构建与部署)

</div>

---

## 目录

- [核心能力](#-核心能力)
- [快速开始](#-快速开始)
- [配置说明](#-配置说明)
- [构建与部署](#-构建与部署)
- [HTTP API](#-http-api)
- [OAuth 授权登录](#-oauth-授权登录)
- [自动遥测](#-自动遥测)
- [架构概览](#-架构概览)
- [项目结构](#-项目结构)
- [CI/CD](#-cicd)
- [限制与注意事项](#-限制与注意事项)
- [内部工作机制](#-网关内部工作机制)
- [数据库表结构](#-数据库表结构)
- [贡献者](#-贡献者)

---

## 核心能力

<table>
<tr>
<td width="50%">

**账号管理**
- 多账号池，Setup Token + OAuth 双认证
- 粘性会话 1h 绑定同一账号（活跃时自动续期）
- 负载感知调度：`eff_7d×0.5 + eff_5h×0.3 + 并发%×0.2`（含时间衰减）
- 每账号独立并发上限，满时排队等待（30s 超时，队列上限 = 并发数）
- 并发/排队满时自动降级到其他账号
- OAuth 按用量窗口智能限流 / 403 永久停用
- 被动采集用量（响应头）+ 可选主动轮询（`auto_poll_usage`）
- 前端实时展示调度评分、并发数、排队数
- 手动一键启停

</td>
<td width="50%">

**反检测引擎**
- UA / 系统提示 / 环境指纹改写
- TLS 指纹伪装（自定义 `craftls`，模拟 Node.js ClientHello）
- AI Gateway 响应头过滤（LiteLLM / Helicone / Portkey / Cloudflare / Kong / BrainTrust）
- 自动遥测代发，10min TTL 续期

</td>
</tr>
<tr>
<td width="50%">

**鉴权与安全**
- API Token 鉴权，不暴露真实凭证
- OAuth PKCE 内置授权流程
- 管理后台密码保护

</td>
<td width="50%">

**平台支持**
- Vue 3 Web 管理后台
- SQLite / PostgreSQL 双数据库
- Redis / 内存缓存
- Docker 多架构镜像
- Linux / Windows 单二进制分发

</td>
</tr>
</table>

---

## 快速开始

### 环境要求

| 依赖 | 版本 | 说明 |
|------|------|------|
| Rust | >= 1.82 | 后端编译 |
| Node.js | 22 | 前端构建 |
| npm | - | 随 Node.js 安装 |
| Redis | 可选 | 多实例部署需要 |
| PostgreSQL | 可选 | 默认使用 SQLite |
| Docker | 可选 | 容器化部署 |

### 三步启动

```bash
# 1. 克隆项目
git clone https://github.com/MamoWorks/cc-bridge.git
cd cc-bridge

# 2. 配置环境
cp .env.example .env

# 3. 启动服务
./scripts/dev.sh          # Linux / macOS
# scripts\dev.bat         # Windows
```

### 启动后入口

| 入口 | 地址 | 说明 |
|------|------|------|
| 管理后台 | `http://127.0.0.1:5674/` | Vue 3 Web 界面 |
| 登录页 | `http://127.0.0.1:5674/login` | 默认密码 `admin` |
| API 网关 | `http://127.0.0.1:5674/*` | 除保留路径外的所有请求 |

### 基本使用流程

```
1. 登录管理后台 → 2. 新建账号（手动 / OAuth 一键授权）→ 3. 创建 API Token → 4. 调用网关
```

> **建议**：创建账号时同时填写 `account_uuid`、`organization_uuid`、`subscription_type`

### 调用示例

```bash
curl http://127.0.0.1:5674/v1/messages \
  -H "Authorization: Bearer sk-your-gateway-token" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-sonnet-4-6",
    "max_tokens": 128,
    "messages": [{"role": "user", "content": "hello"}]
  }'
```

---

## 配置说明

通过 `.env` 文件或环境变量配置。优先级：**进程环境变量 > `.env` > 代码默认值**。

### 服务端

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `5674` | 监听端口 |
| `TLS_CERT_FILE` | - | 证书路径（需反代终止 TLS） |
| `TLS_KEY_FILE` | - | 私钥路径 |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `ADMIN_PASSWORD` | `admin` | 管理后台密码 |
| `USAGE_POLL_INTERVAL_SECS` | `300` | OAuth 账号用量后台自动刷新间隔（秒） |

### 数据库

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DATABASE_DRIVER` | `sqlite` | `sqlite` 或 `postgres` |
| `DATABASE_DSN` | `data/claude-code-gateway.db` | 完整 DSN，设置后优先使用 |
| `DATABASE_HOST` | `localhost` | PostgreSQL 主机 |
| `DATABASE_PORT` | `5432` | PostgreSQL 端口 |
| `DATABASE_USER` | `postgres` | PostgreSQL 用户名 |
| `DATABASE_PASSWORD` | - | PostgreSQL 密码 |
| `DATABASE_DBNAME` | `claude_code_gateway` | PostgreSQL 数据库名 |

> SQLite 自动创建目录并启用 WAL 模式。PostgreSQL 无 DSN 时自动拼接连接串。

### Redis（可选）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `REDIS_HOST` | - | 不设置则使用内存缓存 |
| `REDIS_PORT` | `6379` | 端口 |
| `REDIS_PASSWORD` | - | 密码 |
| `REDIS_DB` | `0` | 数据库编号 |

> 单实例无需 Redis，多实例部署请启用以共享会话粘性和并发计数。

### 最小配置

```env
SERVER_HOST=0.0.0.0
SERVER_PORT=5674
DATABASE_DRIVER=sqlite
DATABASE_DSN=data/claude-code-gateway.db
ADMIN_PASSWORD=change-me
LOG_LEVEL=info
```

---

## 构建与部署

### 开发模式

```bash
# 方式一：一键启动（自动检测前端变更）
./scripts/dev.sh

# 方式二：前后端分离
cd web && npm ci && npm run dev    # 终端 A：前端 :3000
cargo run                           # 终端 B：后端 :5674
```

### 生产构建

```bash
# 当前平台
./scripts/build.sh

# 交叉编译
./scripts/build.sh linux-amd64
./scripts/build.sh linux-arm64

# 手动构建
cd web && npm ci && npm run build && cd ..
cargo build --release
./target/release/claude-code-gateway
```

### Docker 部署

```bash
cp .env.example .env
cd docker && docker compose up -d
```

> SQLite 数据持久化到命名卷 `claude-code-gateway-data`。

### 生产建议

| 建议 | 说明 |
|------|------|
| TLS 终止 | 使用 Nginx / Caddy 等反代 |
| 强密码 | 设置强随机 `ADMIN_PASSWORD` |
| Redis | 多实例部署启用 |
| 网络隔离 | 管理后台路径做访问控制 |

---

## HTTP API

### 认证方式

| 类型 | Header |
|------|--------|
| 管理 API | `x-api-key: <ADMIN_PASSWORD>` 或 `Authorization: Bearer <ADMIN_PASSWORD>` |
| 网关 API | `x-api-key: <sk-...>` 或 `Authorization: Bearer <sk-...>` |

### 管理接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/admin/dashboard` | 仪表盘统计 |
| `GET` | `/admin/accounts` | 账号列表（`page`/`page_size`） |
| `POST` | `/admin/accounts` | 创建账号 |
| `PUT` | `/admin/accounts/:id` | 更新账号 |
| `DELETE` | `/admin/accounts/:id` | 删除账号 |
| `POST` | `/admin/accounts/:id/test` | 测试账号 Token |
| `POST` | `/admin/accounts/:id/usage` | 刷新用量 |
| `GET` | `/admin/tokens` | 令牌列表 |
| `POST` | `/admin/tokens` | 创建令牌 |
| `PUT` | `/admin/tokens/:id` | 更新令牌 |
| `DELETE` | `/admin/tokens/:id` | 删除令牌 |
| `POST` | `/admin/oauth/generate-auth-url` | 生成 OAuth 授权链接 |
| `POST` | `/admin/oauth/generate-setup-token-url` | 生成 Setup Token 授权链接 |
| `POST` | `/admin/oauth/exchange-code` | 交换 OAuth 授权码 |
| `POST` | `/admin/oauth/exchange-setup-token-code` | 交换 Setup Token 授权码 |

### 网关转发

所有未命中前端页面、`/assets/*`、`/admin/*` 的请求进入网关 fallback，经 API Token 鉴权后转发到 `https://api.anthropic.com`。

### 保留路径

`/`、`/login`、`/tokens`、`/favicon.svg`、`/assets/*`、`/admin/*` 不进入网关。

### 创建账号示例

<details>
<summary><b>Setup Token 模式</b></summary>

```bash
curl -X POST http://127.0.0.1:5674/admin/accounts \
  -H "Authorization: Bearer admin" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "account-01",
    "email": "user@example.com",
    "auth_type": "setup_token",
    "setup_token": "sk-ant-xxxx",
    "proxy_url": "socks5://127.0.0.1:1080",
    "billing_mode": "strip",
    "account_uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "organization_uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "subscription_type": "pro",
    "concurrency": 3,
    "priority": 50,
    "auto_telemetry": false
  }'
```

</details>

<details>
<summary><b>OAuth 模式</b></summary>

```bash
curl -X POST http://127.0.0.1:5674/admin/accounts \
  -H "Authorization: Bearer admin" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "account-02",
    "email": "user@example.com",
    "auth_type": "oauth",
    "access_token": "ant-oc_xxxx",
    "refresh_token": "ant-rt_xxxx",
    "expires_at": 1735689600000,
    "billing_mode": "rewrite",
    "account_uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "organization_uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "subscription_type": "max",
    "concurrency": 5,
    "priority": 10,
    "auto_telemetry": true
  }'
```

</details>

<details>
<summary><b>创建令牌</b></summary>

```bash
curl -X POST http://127.0.0.1:5674/admin/tokens \
  -H "Authorization: Bearer admin" \
  -H "Content-Type: application/json" \
  -d '{"name": "team-a", "allowed_accounts": "1,2", "blocked_accounts": ""}'
```

</details>

### 错误响应

统一格式 `{"error": "..."}`，常见状态码：`400` / `401` / `404` / `429` / `502` / `503` / `500`。

`/v1/messages` 如果携带 `messages[].role=system`，但请求体 `model` 未命中全局设置
`allow_system_role_models`，网关会本地返回 400，不请求上游。该错误会额外返回当前模型和允许列表：

```json
{
  "error": "messages[].role=system is not allowed for this model",
  "model": "claude-opus-4-7",
  "allowed_system_role_models": ["claude-opus-4-8"]
}
```

---

## OAuth 授权登录

管理后台内置 OAuth PKCE 授权流程：

1. 点击 **"授权登录"**，选择模式：
   - **OAuth（完整权限）**：获取 `access_token` + `refresh_token`
   - **Setup Token（仅推理）**：获取 365 天有效的 `access_token`
2. 可选填写代理地址
3. 复制授权链接到浏览器完成登录
4. 从回调 URL 复制 `code`，粘贴到管理后台交换
5. 系统自动获取凭证和 `account_uuid`、`organization_uuid`、`email` 等信息
6. 点击 **"应用到新账号"** 自动填入表单

> 授权会话有效期 30 分钟。

---

## 自动遥测

开启 `auto_telemetry` 后，网关代替客户端发送遥测：

| 功能 | 说明 |
|------|------|
| **拦截** | 客户端遥测请求返回 200，不转发上游 |
| **代发** | `/api/event_logging/v2/batch`（每 10s）、`/api/eval/sdk-*`（每 6h） |
| **触发** | 账号收到 `/v1/messages` 请求时激活遥测会话（10min TTL，自动续期） |
| **拦截路径** | `/api/event_logging/v2/batch`、`/api/event_logging/batch`、`/api/eval/*`、`/api/claude_code/metrics`、`/api/claude_code/organizations/metrics_enabled` |

> Datadog 遥测由客户端直连 `browser-intake-datadoghq.com`，无法通过网关拦截。建议在网络层屏蔽。

---

## 架构概览

```
                                    CC-Bridge
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   Client Request                                                    │
│        │                                                            │
│        v                                                            │
│   ┌─────────┐    ┌──────────┐    ┌───────────┐    ┌─────────────┐  │
│   │  Auth    │───>│ Account  │───>│ Rewriter  │───>│  craftls    │──│──> api.anthropic.com
│   │Middleware│    │Scheduler │    │  Engine   │    │ TLS Spoof   │  │
│   └─────────┘    └──────────┘    └───────────┘    └─────────────┘  │
│        │              │                                             │
│        v              v                                             │
│   ┌─────────┐    ┌──────────┐                                      │
│   │  Token   │    │ Session  │                                      │
│   │  Store   │    │ Sticky   │                                      │
│   └─────────┘    └──────────┘                                      │
│        │              │                                             │
│        v              v                                             │
│   ┌──────────────────────────────┐                                  │
│   │   SQLite / PostgreSQL        │                                  │
│   │   Redis / Memory Cache       │                                  │
│   └──────────────────────────────┘                                  │
│                                                                     │
│   ┌──────────────────────────────┐                                  │
│   │   Vue 3 Web Dashboard        │    :5674                        │
│   └──────────────────────────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 项目结构

```text
cc-bridge/
├── .github/workflows/       # GitHub Actions 发布流程
├── craftls/                 # 自定义 rustls 分支（TLS 指纹伪装）
├── docker/                  # Dockerfile & docker-compose.yml
├── scripts/                 # 开发与构建脚本
├── src/
│   ├── main.rs              # 程序入口
│   ├── config.rs            # 环境变量加载
│   ├── error.rs             # 统一错误类型
│   ├── handler/             # 路由与 HTTP handler
│   ├── middleware/          # 鉴权中间件
│   ├── model/               # Account / ApiToken / Identity 模型
│   ├── service/             # Gateway / Account / OAuth / Telemetry / Rewriter
│   ├── store/               # 数据库与缓存访问层
│   └── tlsfp/               # TLS 指纹客户端
├── web/                     # Vue 3 前端
│   ├── src/components/      # 页面组件（Dashboard / Accounts / Tokens / Login）
│   ├── src/api.ts           # API 封装
│   └── vite.config.ts       # Vite 配置
├── .env.example             # 配置模板
├── .version                 # 发布版本与镜像名
└── Cargo.toml               # Rust 项目清单
```

---

## CI/CD

通过 `.version` 文件控制发布版本。GitHub Actions 工作流：

| 触发方式 | 条件 |
|----------|------|
| 自动触发 | 推送到 `main` 且 `.version` 有变更 |
| 手动触发 | `workflow_dispatch` |

**产物：**
- Linux x86_64 / arm64 二进制
- Windows x86_64 二进制
- GHCR 多架构 Docker 镜像（`latest` / `<version>` / `v<version>`）

**发布步骤：** 修改 `.version` 中的 `version` → 合入 `main` → 等待自动构建。

---

## 限制与注意事项

| # | 限制 | 说明 |
|---|------|------|
| 1 | TLS 未接入 HTTPS 监听 | 需使用 Nginx / Caddy / Traefik 反代做 TLS 终止 |
| 2 | 无显式 `/_health` 和 `/v1/models` | 这些路径进入网关 fallback 转发到上游 |
| 3 | Token 明文存储 | 凭证以明文存储在数据库中，请保护数据库访问 |
| 4 | 单共享密码 | 无多用户/权限系统，建议强密码 + 可信网络 + 反代访问控制 |
| 5 | 多实例需 Redis | 否则会话粘性和并发计数无法跨实例共享 |
| 6 | 版本号硬编码 | identity 模块中的版本号为静态值，上游更新后需手动同步 |
| 7 | Datadog 遥测无法拦截 | 客户端直连发送，建议网络层屏蔽 |

---

<details>
<summary><h2>网关内部工作机制</h2></summary>

### 请求鉴权

网关请求经令牌鉴权中间件，令牌必须在 `api_tokens` 表中且状态为 `active`。

### 客户端类型识别

| 特征 | 模式 |
|------|------|
| `User-Agent` 以 `claude-code/` 或 `claude-cli/` 开头 | Claude Code |
| 请求体 `metadata.user_id` 存在 | Claude Code |
| 其余 | 纯 API |

### 客户端访问策略

网关通过全局 settings 控制客户端入口访问，校验发生在账号选择和上游请求之前：

- `allowed_claude_code_versions`：只作用于 `claude-code/` / `claude-cli/` UA，默认 `2.1.89-2.1.187`
- `blocked_claude_code_versions`：只作用于 `claude-code/` / `claude-cli/` UA，默认空；命中后优先拒绝
- `allowed_user_agents`：只作用于非 Claude Code / CLI UA，默认允许 `AI-Hub-Monitor*` 和 `python-httpx*`
- 版本规则支持精确版本、通配和闭区间，例如 `2.1.187`、`2.1.*`、`2.1.89-2.1.187`
- 禁止版本规则与允许版本规则语法一致；同一版本同时命中允许和禁止时，禁止规则优先生效
- UA 规则支持 `*` 通配，例如 `AI-Hub-Monitor*`、`python-httpx*`
- `allowed_claude_code_versions` 清空表示关闭允许范围限制，但 `blocked_claude_code_versions` 非空时仍会拦截命中的 Claude Code / CLI 版本
- `blocked_claude_code_versions` 清空表示关闭禁止版本限制；`allowed_user_agents` 清空表示关闭非 Claude Code UA 限制
- 未命中策略时本地返回 403，不请求上游

### 会话哈希

- **Claude Code**：从 `metadata.user_id` 解析 `session_id`
- **纯 API**：`sha256(UA + system/首条消息 + 小时窗口)`

### 账号过滤

每个 API Token 可配置 `allowed_accounts` 和 `blocked_accounts`（逗号分隔 ID）。

### 账号选择

1. 粘性绑定命中且可调度 → 复用（刷新 1h TTL）
2. 否则从可调度账号（active + 未限流 + 未排除）中按 `priority` 升序选最优组
3. 同优先级内按综合评分选择（越低越优先）：
   - `eff_7d = 7d_utilization × (剩余时间 / 7天)` — 权重 0.5
   - `eff_5h = 5h_utilization × (剩余时间 / 5小时)` — 权重 0.3
   - `concurrency_load% = 当前并发 / 最大并发 × 100` — 权重 0.2
   - 并发已满的账号优先排除；快重置的窗口衰减更大
4. 绑定粘性会话（1h TTL，活跃时续期）

### 并发控制

每账号 `concurrency` 上限，请求命中后抢占槽位。槽位满时进入等待队列（500ms 轮询，最多 30s），队列上限等于账号并发数。等待队列满或超时后自动降级到其他账号。所有账号都忙时返回 429。

### 限速与停用

网关根据账号类型对上游 `429` 采取不同策略，避免把仍有额度的账号长时间挂起。

**SetupToken 账号**（无法查询用量接口，保守处理）：

| 上游状态码 | 行为 | 持续时间 |
|-----------|------|---------|
| `429` | 暂停调度（状态保持 active） | 5 小时自动恢复 |
| `403` | 永久停用（标记 disabled） | 手动启用 |

**OAuth 账号**（收到 `429` 后立即调用 `/api/oauth/usage` 判断实际触发的限额窗口）：

| 触发场景 | 判定条件 | 恢复时间 |
|---------|---------|---------|
| 7 天限额撞墙 | `seven_day.utilization >= 97%` | 到 `seven_day.resets_at` |
| 5 小时限额撞墙 | `five_hour.utilization >= 97%` | 到 `five_hour.resets_at` |
| 纯速率限制 | 两个窗口都未达阈值 | 1 分钟短冷却 |
| 用量查询失败 | 网络 / 认证异常 | 5 小时保守回退 |
| `403` | 认证失败 | 永久停用（手动启用） |

同时命中两个窗口时优先 7 天（限流更久）。Sonnet 周限额（`seven_day_sonnet`）暂不纳入判断。阈值在 `src/service/account.rs` 的 `USAGE_HIT_THRESHOLD` 常量中可调整。

> 429 期间再收到 403 不会触发永久停用，避免误判。

### 自动换号重试

收到 429 后网关会把当前账号加入本次请求的排除列表，从可调度账号中重新选号继续尝试，直到成功或无可用账号。客户端只看到最终结果，中间切换对其透明。

### 用量数据

- **被动采集**（所有账号）：每次请求从上游响应头 `anthropic-ratelimit-unified-*` 提取 utilization 和 resets_at，merge 到 `usage_data`。零 API 开销，SetupToken 也能获取用量。
- **主动轮询**（可选）：账号开启 `auto_poll_usage` 后，后台每 `USAGE_POLL_INTERVAL_SECS`（默认 300 秒）调用 Anthropic `/api/oauth/usage` 获取完整用量。仅 OAuth 账号支持。默认关闭。
- **前端静默重载**：Accounts 页面每 60 秒重拉账号列表，展示实时调度评分、并发数、排队数。
- **手动刷新按钮**：点击"用量"按钮可立即刷新单个账号的用量数据。

### 请求头改写

- 默认 Claude Code 指纹为 `2.1.185`，新账号的 `version` / `version_base` / `build_time` 会按该版本生成；启动迁移会把已有账号的这三个版本字段升级到当前默认值
- `/v1/messages` 使用 `claude-cli/<version> (external, cli)`、`X-Stainless-Package-Version=0.94.0`、`X-Stainless-Runtime-Version=v24.3.0`
- `/api/event_logging/v2/batch` 使用 `claude-code/<version>`、`anthropic-beta=oauth-2025-04-20`、`x-service-name=claude-code`
- `/api/eval/*` 使用抓包中的 `Bun/1.4.0` UA
- `/v1/code/triggers` / `/v1/mcp_servers` 使用各自 endpoint beta token
- 注入/合并 `anthropic-beta`、固定必要 `anthropic-version`
- 强制使用账号真实 `Authorization`
- 追加 `beta=true` 查询参数
- 还原 header wire casing

### 请求体改写

| 路径 | 改写内容 |
|------|---------|
| `/v1/messages` | 系统提示词注入、`metadata.user_id`、环境/进程指纹、`cache_control`、billing 处理 |
| `/api/event_logging/v2/batch` / `/api/event_logging/batch` | `device_id`、`email`、`account_uuid`、`organization_uuid`、env/process 指纹、`user_attributes` JSON |
| `/api/eval/{clientKey}` | `id`、`deviceID`、`email`、`accountUUID`、`organizationUUID`、`subscriptionType`、`userType`、`rateLimitTier`、`entrypoint`、移除 `apiBaseUrlHost` |
| 其他路径 | 通用身份字段改写 |

### Thinking 签名错误重试

`/v1/messages` 如果上游返回 400 且错误体包含 `signature` /
`thought_signature` / thinking 结构错误，网关会对同一账号、同一 token、
同一套请求头做最多两阶段降级重试：

1. `thinking-only`：移除顶层 `thinking`，把历史 `thinking` block 转为普通
   `text`，删除 `redacted_thinking`。
2. `thinking+tools`：第一阶段仍然是签名相关 400 时，再把 `tool_use` /
   `tool_result` 转为普通 `text`。

该逻辑用于缓解从 Kiro / Antigravity 等渠道切回官方 Anthropic 后，历史
`thinking.signature` 无法通过官方校验的问题。网关不会验证、伪造或重算
Anthropic `thinking.signature`，也不会使用 Gemini `thoughtSignature` 的 dummy
值。

### 系统角色模型白名单

Claude Code 2.1.172 在 `claude-opus-4-8` 请求中可能把运行时提醒放入
`messages[].role=system`。网关通过全局 settings key
`allow_system_role_models` 控制哪些模型允许透传这种格式：

- 默认值：`claude-opus-4-8`
- 格式：逗号分隔的精确模型 ID 列表，例如 `claude-opus-4-8,claude-sonnet-4-6`
- 空字符串：不允许任何模型透传 `messages[].role=system`
- 未命中：本地返回 400，并在响应中返回 `allowed_system_role_models`

### Anthropic 缓存改写

网关通过全局 settings key `message_cache_control_rewrite` 控制 Claude Code
`/v1/messages` 的 messages 缓存断点策略：

- `off`：默认值，保持客户端原始 `messages[].content[].cache_control`
- `auto`：推荐的保守修复策略，先稳定化 tools / skills / deferred tools / 并行 tool blocks 顺序，再接管 4 个断点预算；尾部断点选择最新可缓存 block（可包含 `user tool_result`），窗口回退仍优先 text 边界，且不主动选择 `assistant tool_use`
- `stateful`：会话级防污染策略；在 `auto` 的 prefix 稳定化和安全断点规则上，按 `account_id + Claude Code session_id` 记录上一轮实际发出的断点指纹，正常主线优先复用旧断点，再补 tail/bridge；遇到 `76 -> 567 -> 78` 这类 block 数异常暴涨或并发 sibling 请求时只临时选点，不覆盖正常主线锚点
- `rolling`：更积极的滚动断点对照策略，同样稳定化 prefix；它也不会落到 `assistant tool_use`，窗口回退优先 text，只有当前窗口没有 text 时才兜底使用 `user tool_result`
- `sub2api`：通用稳定断点策略，先清理 `messages` 内已有断点，再只给最后一条 message 和倒数第二个 `role=user` message 的最后可缓存 block 打断点；不主动接管 system/tools 断点，适合 API 模式和 Claude Code 对照测试

API 模式只要该设置不是 `off`，都会使用 `sub2api` 风格策略，避免复用 Claude Code
并行 tool 专用的 rolling/lookback 算法导致长历史断点持续漂移。

历史配置值 `stable` / `anchored` 会兼容解析为 `auto`。

`auto` / `stateful` / `rolling` 会清理请求根级、`system`、`tools` 和 `messages` 内已有的
`cache_control`，再把最多 4 个断点集中用于 message history。这样避免
Claude Code 原始 system/tools 断点占掉 slot，导致并行 tool 长会话只能给
尾部历史留下 1-2 个断点。若尾部滚动断点未用满可用 slot，网关还会尝试在
Claude Code 首个 user message 内的 hooks / skills / CLAUDE.md /
deferred tools / MCP 自动注入块末尾补一个边界断点。
为避免并行 tool 场景下 prefix hash 在进入 messages 之前就变化，这些模式还会将
`tools[]` 按 `name` 排序，并对 Claude Code 自动注入的 skills / deferred tools
文本列表做确定性排序；连续的 `assistant tool_use` 与对应 `user tool_result`
也会按工具调用 ID 稳定排序。`thinking` / `redacted_thinking` 不会直接放置
`cache_control`，但会计入 19-block 步距。

`cache_control_ttl_rewrite=off|5m|1h` 只覆盖已有或上述策略新建的
ephemeral `cache_control.ttl`，不会额外新建断点。所有缓存断点和 TTL 改写都在
CCH attestation 重新计算之前完成。

### Billing / CCH 策略

`billing_mode=rewrite` 会按版本改写 `cc_version` / `cch`。Claude Code `2.1.185` 的 `cc_version` 后缀公式沿用 JS 字符串索引语义；`cch` 使用序列化后的 body（其中 `cch=00000` 保持占位）计算 `xxhash64` 低 20 bits。`2.1.156` / `2.1.169` 使用完整最终 body 与 seed `0x4D659218E32A3268`；`2.1.172` / `2.1.173` / `2.1.185` 继续使用同 seed，但计算前会把顶层 `model` 值替换为 `""`，并排除顶层 `max_tokens` / `fallbacks` 字段。旧版本继续使用旧 seed `0x6E52736AC806831E`。

### TLS 指纹

所有上游请求通过 `craftls` 发出，模拟 Node.js TLS 指纹。每账号可配代理（HTTP / SOCKS5）。

### AI Gateway 指纹过滤

过滤响应头前缀：`x-litellm-`、`helicone-`、`x-portkey-`、`cf-aig-`、`x-kong-`、`x-bt-`。

</details>

<details>
<summary><h2>数据库表结构</h2></summary>

### `accounts` 表

| 字段 | 说明 |
|------|------|
| `id` | 主键 |
| `name` / `email` | 账号标识（email 检查重复） |
| `status` | `active` / `error` / `disabled` |
| `auth_type` | `setup_token` / `oauth` |
| `token` | Setup Token |
| `access_token` / `refresh_token` / `oauth_expires_at` / `oauth_refreshed_at` | OAuth 凭证 |
| `auth_error` | 认证错误信息 |
| `proxy_url` | 账号专用代理 |
| `device_id` | 自动生成的设备 ID |
| `canonical_env` / `canonical_prompt_env` / `canonical_process` | 指纹 JSON |
| `billing_mode` | `strip` / `rewrite` |
| `account_uuid` / `organization_uuid` / `subscription_type` | 遥测改写用 |
| `concurrency` / `priority` | 调度参数 |
| `rate_limited_at` / `rate_limit_reset_at` / `disable_reason` | 限流/停用状态 |
| `usage_data` / `usage_fetched_at` | 用量缓存 |
| `auto_telemetry` / `telemetry_count` | 自动遥测 |
| `auto_poll_usage` | 是否开启后台自动轮询用量 |

### `api_tokens` 表

| 字段 | 说明 |
|------|------|
| `id` | 主键 |
| `name` | 令牌名称 |
| `token` | 自动生成的 `sk-...` 令牌 |
| `allowed_accounts` / `blocked_accounts` | 账号 ID 列表（逗号分隔） |
| `status` | `active` / `disabled` |

> 服务启动时自动执行内建 SQL 迁移，不依赖外部 migration 文件。

</details>

<details>
<summary><h2>账号字段参考</h2></summary>

| 字段 | 必填 | 说明 |
|------|------|------|
| `email` | 是 | 账号邮箱 |
| `auth_type` | 否 | `setup_token`（默认）或 `oauth` |
| `setup_token` / `token` | 条件 | Setup Token 模式必填 |
| `access_token` / `refresh_token` | 条件 | OAuth 模式必填 |
| `expires_at` | 否 | OAuth access_token 过期时间（ms 时间戳） |
| `name` | 否 | 显示名称 |
| `proxy_url` | 否 | 专用代理 |
| `billing_mode` | 否 | `strip` 或 `rewrite` |
| `account_uuid` | 否 | 推荐填写，用于遥测改写 |
| `organization_uuid` | 否 | 推荐填写，用于遥测改写 |
| `subscription_type` | 否 | `max` / `pro` / `team` / `enterprise`，推荐填写 |
| `concurrency` | 否 | 最大并发，默认 3 |
| `priority` | 否 | 数值越小优先级越高，默认 50 |
| `auto_telemetry` | 否 | 是否开启自动遥测，默认 false |
| `auto_poll_usage` | 否 | 是否开启后台自动轮询用量（仅 OAuth），默认 false |

> 创建时系统自动生成 `device_id`、`canonical_env`、`canonical_prompt_env`、`canonical_process`。

</details>

---

## 许可与依赖说明

项目包含自定义 `craftls` 目录（基于 [rustls](https://github.com/rustls/rustls) 分支）。详见 `craftls/` 下的许可证文件。

---

## 贡献者

<table>
<tr>
<td align="center">
<a href="https://github.com/Rfym21">
<img src="https://github.com/Rfym21.png" width="80px;" alt="Rfym21"/>
<br/>
<sub><b>Rfym21</b></sub>
</a>
</td>
<td align="center">
<a href="https://github.com/FF-crazy">
<img src="https://github.com/FF-crazy.png" width="80px;" alt="FF-crazy"/>
<br/>
<sub><b>FF-crazy</b></sub>
</a>
</td>
<td align="center">
<a href="https://github.com/kao0312">
<img src="https://github.com/kao0312.png" width="80px;" alt="kao0312"/>
<br/>
<sub><b>kao0312</b></sub>
</a>
</td>
<td align="center">
<a href="https://github.com/2830897438">
<img src="https://github.com/2830897438.png" width="80px;" alt="2830897438"/>
<br/>
<sub><b>2830897438</b></sub>
</a>
</td>
</tr>
</table>

---

<div align="center">

**[MamoWorks](https://github.com/MamoWorks)** &copy; 2025

[![Star History Chart](https://api.star-history.com/svg?repos=MamoWorks/cc-bridge&type=Date)](https://star-history.com/#MamoWorks/cc-bridge&Date)

</div>
