# LivePilot WSL2 环境配置指南生成提示词

```text
你是一名资深 Python、React、Docker、WSL2 和实时 AI 应用工程师。请为 LivePilot 项目生成一份详细、可执行的本地开发环境配置指南。

项目名称：LivePilot
项目类型：全双工实时语音旅行规划 Agent

## 开发环境

- 宿主机：Windows 11
- Linux 开发环境：WSL2 Ubuntu
- 容器环境：Docker Desktop，启用 WSL2 backend
- 浏览器：Windows 浏览器，用于访问前端、使用麦克风和播放声音
- 编辑器：VS Code + Remote - WSL
- 项目目录：WSL Linux 文件系统，例如 `~/projects/livepilot`
- 不要把项目放在 `/mnt/c/...` 下
- Python、Node.js、FastAPI、Worker 和 Docker Compose 命令默认在 WSL Ubuntu 中执行
- 只有安装 WSL、启动 Docker Desktop、打开浏览器等操作可以在 Windows 中执行

## 技术栈

- 前端：React + TypeScript + Vite
- 后端：Python 3.12+、FastAPI、asyncio
- Python 依赖管理：uv
- 异步任务：Redis + Arq 或 Redis Streams
- 数据库：PostgreSQL
- 实时音频：浏览器通过 WebRTC 接入实时语音模型 API
- 可观测性：OpenTelemetry、Prometheus、Grafana
- 容器：Docker Compose

## 业务场景

LivePilot 是一个语音旅行规划助手。

用户通过自然语音描述目的地、时间、预算和兴趣偏好。Agent 负责规划行程、查询天气、检索景点和餐厅，并根据用户后续反馈实时调整。

示例：

用户：
“我准备下周去上海玩三天，预算五千，想安排一些适合晚上的活动。”

Agent：
“我先帮你规划一个三天行程，再查询下周天气和适合晚上的活动。”

后台 Worker 异步执行天气查询、景点搜索、餐厅搜索、路线规划和预算计算。执行期间用户可以继续说话或打断 Agent。

用户：
“等等，我不想去人太多的地方，最好多安排一些博物馆。”

系统必须立即停止当前音频播放，更新用户偏好，并根据最新上下文重新规划。旧任务结果返回时，不能覆盖用户最新需求。

## 输出要求

使用中文输出 Markdown 格式。

每一步必须说明：

1. 配置目的
2. 执行位置
3. 具体命令
4. 预期输出
5. 验证方法
6. 常见错误和解决方法

每个代码块必须标明执行环境或文件路径，例如：

- `Windows PowerShell`
- `WSL Ubuntu`
- `frontend/.env.local`
- `backend/.env`
- `compose.yaml`

不要输出以下章节：

- 项目周期
- 项目边界
- 简历描述

## 必须包含的章节

### 1. 环境架构

解释 Windows、WSL2、Docker Desktop、浏览器和项目服务之间的关系。

使用 Mermaid 绘制架构图，至少包含：

- Windows 浏览器
- WSL2 Ubuntu
- React 前端
- FastAPI
- Python Worker
- Redis
- PostgreSQL
- Docker Desktop

明确说明：浏览器运行在 Windows 中，但前端开发服务、FastAPI 和 Worker 运行在 WSL 中。

### 2. Windows 前置环境

说明如何在 Windows 中安装和验证：

- WSL2
- Ubuntu
- Docker Desktop
- Docker Desktop 的 WSL Integration
- VS Code
- Remote - WSL 插件

必要的 PowerShell 命令必须单独标注为 Windows 命令。

### 3. WSL Ubuntu 基础配置

说明如何在 WSL 中：

- 更新系统软件包
- 安装 Git、curl、build-essential、libpq 等依赖
- 配置 Git
- 创建 `~/projects/livepilot`
- 验证项目不在 `/mnt/c/...`
- 验证 WSL 和 Linux 用户环境

### 4. Node.js 与 React 前端

使用 WSL 中的 nvm 安装 Node.js LTS。

必须包含：

- nvm 安装和验证
- Node.js、npm 版本验证
- React + TypeScript + Vite 初始化命令
- 前端依赖安装
- Vite 启动命令
- `--host 0.0.0.0` 配置
- Windows 浏览器访问地址
- 前端服务验证方式

说明前端只能配置公开变量，例如：

```text
VITE_API_BASE_URL
```

前端不得保存长期模型 API Key、数据库密码、Redis 地址或 JWT 私钥。

### 5. Python FastAPI 后端

使用 uv 管理 Python 环境。

必须包含：

- uv 安装和验证
- Python 3.12+ 安装或选择
- 创建虚拟环境
- 激活虚拟环境
- 安装 FastAPI、Uvicorn、Pydantic Settings、SQLAlchemy、Alembic、Redis 客户端和 OpenTelemetry
- 后端目录结构
- `.env.example`
- Pydantic Settings 配置方式
- 最小 `/health` 接口
- Uvicorn 启动命令
- `--host 0.0.0.0` 的作用
- 使用浏览器或 curl 验证接口

### 6. Docker Compose

提供完整、可运行的 `compose.yaml`，至少包含：

- PostgreSQL
- Redis
- 数据卷
- 端口
- 健康检查
- 环境变量引用
- Docker 网络

所有 Docker Compose 命令必须默认从 WSL Ubuntu 执行。

说明：

- Docker Desktop 必须保持运行
- WSL Ubuntu 通过 Docker Desktop 使用 Docker Engine
- 不要重复在 WSL 中安装另一套 Docker Desktop
- 给出启动、停止、查看日志和进入容器的命令

### 7. PostgreSQL

说明：

- PostgreSQL 用户、数据库和密码配置
- `DATABASE_URL` 的正确格式
- 如何启动、停止和查看日志
- 如何进入数据库容器
- 如何验证数据库连接
- Alembic 初始化、生成迁移、执行迁移和回滚

### 8. Redis 与异步 Worker

说明：

- `REDIS_URL` 的正确格式
- Redis 启动和验证
- Worker 为什么独立于 FastAPI
- Worker 启动命令
- 最小测试任务
- 任务成功、失败、超时和重试日志
- Worker 无法连接 Redis 时的排查方法

### 9. 实时语音 API

说明安全接入流程：

```text
Windows 浏览器
  -> FastAPI 获取短期实时连接凭证
  -> 浏览器通过 WebRTC 建立实时语音会话
  -> 实时模型返回音频轨道
  -> Windows 浏览器播放声音
```

必须说明：

- 长期 API Key 只保存在 WSL 后端 `.env`
- 前端不能读取长期 API Key
- FastAPI 负责短期凭证
- 浏览器负责麦克风采集和音频播放
- 用户打断时前端停止播放并通知实时会话
- 凭证过期、权限失败和 WebRTC 断开时如何处理

涉及具体模型名称、API 路径或参数时，必须先参考当前官方文档；无法确认时不要编造，应设计独立适配层并明确标注待替换位置。

### 10. 环境变量

使用表格列出：

- 变量名
- 所属服务
- 示例值
- 是否敏感
- 用途
- 是否允许出现在前端

至少包含：

```text
OPENAI_API_KEY
REALTIME_MODEL
DATABASE_URL
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_DB
REDIS_URL
BACKEND_HOST
BACKEND_PORT
FRONTEND_ORIGIN
JWT_SECRET
OTEL_SERVICE_NAME
LOG_LEVEL
```

提供：

- 根目录 `.env.example`
- `backend/.env.example`
- `frontend/.env.local.example`

不得输出任何真实密钥。

### 11. CORS、端口与访问方式

统一说明：

- 前端端口
- FastAPI 端口
- PostgreSQL 端口
- Redis 端口
- `FRONTEND_ORIGIN`
- FastAPI CORS 配置
- WSL 服务绑定 `0.0.0.0`
- Windows 浏览器访问 WSL 服务
- 端口冲突检查方法

### 12. 可观测性

说明如何配置：

- OpenTelemetry
- Prometheus
- Grafana
- FastAPI 请求追踪
- Worker 任务追踪
- Trace ID 传递
- 结构化日志

至少记录：

- 请求延迟
- 首段语音响应延迟
- 用户打断生效时间
- Redis 任务耗时
- 工具调用耗时
- Worker 错误率
- WebRTC 重连次数
- 数据库连接错误

### 13. 一键启动流程

给出完整 WSL Ubuntu 启动顺序：

1. 进入项目目录
2. 确认 Docker Desktop 已启动
3. 启动 PostgreSQL 和 Redis
4. 执行数据库迁移
5. 启动 FastAPI
6. 启动 Worker
7. 启动 React
8. 使用 Windows 浏览器访问前端
9. 验证 `/health`
10. 验证 PostgreSQL
11. 验证 Redis
12. 验证 Worker 测试任务
13. 验证麦克风和扬声器权限
14. 验证实时语音连接

每一步都提供命令和预期结果。

### 14. 常见问题排查

至少覆盖：

- WSL2 未启用
- Docker Desktop 未启动
- WSL Integration 未开启
- 项目放在 `/mnt/c`
- Node.js 或 Python 版本错误
- uv 虚拟环境未激活
- Docker 端口冲突
- PostgreSQL 连接拒绝
- Redis 连接失败
- FastAPI 无法访问
- 前端 CORS 错误
- 浏览器麦克风权限被拒绝
- WebRTC 连接失败
- `.env` 未加载
- Worker 无法连接 Redis
- Windows 防火墙或代理影响连接

每个问题必须给出：

- 检查命令
- 可能原因
- 修复方法
- 修复后的验证命令

### 15. 最终检查清单

最后输出一份按顺序排列的检查清单，确认：

- WSL2 正常
- Docker Compose 正常
- React 正常启动
- FastAPI 正常启动
- PostgreSQL 可连接
- Redis 可连接
- Worker 可以执行测试任务
- `.env` 未提交到 Git
- 前端没有暴露长期 API Key
- Windows 浏览器可以访问 WSL 服务
- 麦克风和扬声器权限正常
- 实时语音会话可以建立
- 用户可以打断 Agent
- 日志和 Trace 可以查看

不要输出项目周期、项目边界或简历描述。
