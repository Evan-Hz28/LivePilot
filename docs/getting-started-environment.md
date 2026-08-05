# LivePilot 入门环境配置与本地运行指南

本文依据 `LivePilotInstruction.md` 的目标架构编写，用于在 **Windows 11 + WSL2 Ubuntu 22.04 + WSL 原生 Docker Engine** 上搭建 LivePilot 的本地开发环境。

它面向“从目前的 Vite 前端骨架开始，逐步建立 FastAPI、PostgreSQL、Redis Streams、Worker 与基础可观测性”的开发过程。每一步只在所需文件出现后继续执行，避免在空目录中运行无效命令。

## 1. 当前状态与目标

### 1.1 已确认的本机状态

以下项目已就绪：

| 项目 | 当前状态 | 是否需重装 |
| --- | --- | --- |
| WSL | Ubuntu 22.04.5 | 否 |
| Docker | WSL 内原生 Docker Engine | 否 |
| Docker Compose | v5.0.2 | 否 |
| Node.js | v24.16.0 | 否 |
| npm | v11.13.0 | 否 |
| uv | v0.11.2 | 否 |
| 系统 Python | 3.10.16 | 保留，不作为本项目运行时 |
| 前端 | `frontend/` 已用 React + TypeScript + Vite 初始化 | 已完成 |

本项目目标使用 Python 3.12+。安装它不会替换系统 Python 3.10，也不会影响依赖 `/usr/bin/python3` 的系统程序。

### 1.2 最终本地架构

```mermaid
flowchart LR
    B[Windows 浏览器] --> FE[React + Vite :5173]
    B --> API[FastAPI :8000]
    FE --> API
    B <-->|WebRTC 音频与数据通道| RP[实时语音供应商]
    API --> PG[(PostgreSQL :5432)]
    API --> R[(Redis Streams :6379)]
    W[Python Worker] --> R
    W --> PG
    API --> OT[OTel Collector]
    W --> OT
    OT --> PR[Prometheus :9090]
    PR --> GF[Grafana :3000]
```

关键原则：

- 浏览器直接通过 WebRTC 与实时语音供应商传输音频；FastAPI 不代理音频。
- FastAPI 只处理鉴权、短期实时令牌、会话、偏好更新、打断、入队与事件推送。
- 天气、景点、餐厅、路线、预算等慢操作由独立 Worker 从 Redis Streams 消费。
- PostgreSQL 是权威状态；Redis 用于队列、取消标记、短期缓存和事件分发。
- 长期 API Key、数据库密码和 JWT 私钥只存在后端环境变量，绝不能进入 `frontend/`。

## 2. 目录结构与执行位置

完成 P0 后，仓库建议采用以下结构：

```text
LivePilot/
├── frontend/                     # 已创建的 React + Vite 项目
│   ├── package.json
│   ├── .env.local                # 只放 VITE_ 公开变量，不提交
│   └── src/
├── backend/                      # 待创建的 Python 项目
│   ├── pyproject.toml
│   ├── uv.lock
│   ├── .python-version
│   ├── .venv/                    # 本地虚拟环境，不提交
│   ├── .env                      # 后端密钥，不提交
│   ├── .env.example              # 变量名和假值，可提交
│   ├── app/
│   │   ├── main.py               # FastAPI 入口
│   │   ├── worker.py             # Redis Streams Worker 入口
│   │   └── ...
│   └── alembic/
├── compose.yaml                  # PostgreSQL、Redis 等本地基础服务
├── .env                          # 仅供 Compose 插值，不提交
├── .env.example
└── docs/
```

以下约定贯穿本文：

- `~/project/LivePilot` 是当前项目目录；请按实际目录调整。
- 无特别说明的命令都在 **WSL Ubuntu 终端** 中执行。
- `frontend` 命令在 `frontend/` 目录执行。
- `uv`、FastAPI、迁移与 Worker 命令在 `backend/` 目录执行。
- `docker compose` 命令在包含 `compose.yaml` 的仓库根目录执行。
- 可在 VS Code 的 WSL 终端中运行命令，但不要在 Windows PowerShell 中执行 Linux 项目命令。

## 3. 第一阶段：验证已安装工具

在仓库根目录执行：

```bash
cd ~/project/LivePilot

node --version
npm --version
uv --version
python3 --version
docker version
docker compose version
```

预期：

- `node` 为 v24.x，`npm` 为 11.x 或兼容版本。
- `uv` 能显示版本。
- `python3` 显示 3.10.x 没有问题，它仍供系统工具使用。
- `docker version` 同时显示 Client 和 Server。
- `docker compose version` 显示 Compose 版本。

确认 Docker 是 WSL 原生 Engine：

```bash
docker info --format 'OS: {{.OperatingSystem}} | Name: {{.Name}}'
systemctl is-active docker
```

预期操作系统为 Ubuntu，第二条通常返回 `active`。此环境不需要 Docker Desktop，也不要再启动 Docker Desktop 的第二套 daemon。

若 `docker version` 出现 socket 权限错误，先在自己的 WSL 终端检查：

```bash
id -nG
sudo usermod -aG docker $USER
exit
```

重新打开 WSL 终端后再次执行 `docker version`。不要通过长期使用 `sudo docker` 规避权限问题。

## 4. 第二阶段：前端环境

### 4.1 已完成的前端初始化

当前 `frontend/` 已由 Vite 创建，且依赖已安装。常用命令：

```bash
cd ~/project/LivePilot/frontend

npm run dev
npm run build
npm run lint
```

- `npm run dev`：启动开发服务器，默认地址为 `http://localhost:5173`。
- `npm run build`：运行 TypeScript 构建检查并生成 `dist/`。
- `npm run lint`：按 ESLint 配置检查源码。
- 若需要从局域网或非默认 WSL 转发访问，使用 `npm run dev -- --host 0.0.0.0`。

Windows 浏览器能访问 `http://localhost:5173` 即表示 Vite 与 WSL 端口转发正常。停止开发服务器按 `Ctrl+C`。

### 4.2 前端环境变量

创建 `frontend/.env.local`：

```dotenv
# frontend/.env.local
VITE_API_BASE_URL=http://localhost:8000
VITE_APP_ENV=development
```

Vite 只会暴露 `VITE_` 前缀变量给浏览器，因此该文件只能放公开信息。以下变量严禁放入前端：

- `REALTIME_PROVIDER_API_KEY` 或任何长期模型密钥
- `DATABASE_URL`、`POSTGRES_PASSWORD`、`REDIS_URL`
- `JWT_SECRET`、OAuth client secret
- 天气、地图、餐厅等第三方服务的服务端密钥

`frontend/.gitignore` 已忽略 `*.local`。修改环境变量后必须重启 Vite。

### 4.3 前端在后端未完成前的状态

在 FastAPI 未建立前，页面可正常开发 UI，但调用 `VITE_API_BASE_URL` 会失败，这是预期行为。不要为了消除该错误把真实密钥或数据库连接放入前端。

浏览器访问麦克风时，`localhost` 被视为安全上下文；局域网 IP 或生产环境必须使用 HTTPS，否则 `getUserMedia` 可能被拒绝。

## 5. 第三阶段：安装项目 Python 3.12

系统 Python 3.10 保持不变；通过 `uv` 下载项目专用解释器：

```bash
cd ~/project/LivePilot

uv python install 3.12
uv python find 3.12
```

预期第二条输出一个 Python 3.12 的路径。该解释器通常由 uv 保存在用户缓存目录，不在 `/usr/bin` 中。

验证“没有改动系统默认 Python”：

```bash
python3 --version
```

它仍可显示 Python 3.10.x。项目随后通过 `.venv` 或 `uv run` 使用 Python 3.12；不需要、也不应修改 `update-alternatives`。

## 6. 第四阶段：初始化 FastAPI 后端

### 6.1 创建 Python 项目骨架

在项目根目录执行一次：

```bash
cd ~/project/LivePilot
mkdir backend
cd backend
uv init --bare
uv python pin 3.12
```

作用如下：

- `uv init --bare` 创建最小 Python 项目，生成 `pyproject.toml`。
- `uv python pin 3.12` 写入 `.python-version`，使本项目默认选择 3.12。
- `pyproject.toml` 是 Python 项目的依赖与构建配置，作用类似前端的 `package.json`。

打开 `backend/pyproject.toml`，将 Python 最低版本固定为 3.12：

```toml
[project]
name = "livepilot-api"
version = "0.1.0"
description = "LivePilot FastAPI gateway and worker"
requires-python = ">=3.12"
dependencies = []
```

项目名可以不同，但 `requires-python` 不应低于 `3.12`。

### 6.2 安装 P0 开发依赖

仍在 `backend/` 执行：

```bash
uv add fastapi "uvicorn[standard]" pydantic-settings \
  sqlalchemy asyncpg alembic redis httpx \
  opentelemetry-api opentelemetry-sdk \
  opentelemetry-instrumentation-fastapi \
  opentelemetry-exporter-otlp-proto-http \
  prometheus-client
```

这些依赖对应项目说明书的 P0 基础：

| 依赖 | 用途 |
| --- | --- |
| `fastapi`、`uvicorn` | HTTP API、WebSocket、开发服务器 |
| `pydantic-settings` | 从 `.env` 安全读取配置 |
| `sqlalchemy`、`asyncpg`、`alembic` | PostgreSQL 异步访问与迁移 |
| `redis` | Redis Streams、取消标记和短期状态 |
| `httpx` | Worker 调用天气、地图等外部工具 |
| `opentelemetry-*`、`prometheus-client` | traces、metrics 与基础观测 |

`uv add` 会更新 `pyproject.toml` 和 `uv.lock`。锁文件应提交到 Git；`.venv` 不应提交。

创建并验证项目虚拟环境：

```bash
uv sync
uv run python --version
```

预期显示 Python 3.12.x。通常无需 `source .venv/bin/activate`；`uv run` 会自动使用这个项目环境。

如需临时激活，使用：

```bash
source .venv/bin/activate
python --version
deactivate
```

激活仅影响当前终端，不会更改系统 Python。

### 6.3 后端最小启动验证

实现 `backend/app/main.py` 和 `GET /health` 后，启动方式为：

```bash
cd ~/project/LivePilot/backend
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

新终端验证：

```bash
curl -i http://localhost:8000/health
```

预期 HTTP 状态为 `200`。此命令在 `app.main:app` 实际创建之前会失败，属于正常现状，不要提前执行。

## 7. 第五阶段：配置 PostgreSQL 与 Redis

### 7.1 先创建 Compose 专用变量文件

在仓库根目录创建本机私有 `.env`，不要提交：

```dotenv
# .env
POSTGRES_USER=livepilot
POSTGRES_PASSWORD=replace-with-a-long-local-password
POSTGRES_DB=livepilot
POSTGRES_PORT=5432
REDIS_PORT=6379
```

并创建可提交的 `.env.example`：

```dotenv
# .env.example
POSTGRES_USER=livepilot
POSTGRES_PASSWORD=change-me
POSTGRES_DB=livepilot
POSTGRES_PORT=5432
REDIS_PORT=6379
```

在根目录 `.gitignore` 添加：

```gitignore
.env
backend/.env
backend/.venv/
__pycache__/
.pytest_cache/
```

若仓库尚无根目录 `.gitignore`，创建它后再加入上述规则。不要依赖 `frontend/.gitignore` 保护后端密钥。

### 7.2 建立本地基础服务

在仓库根目录创建 `compose.yaml`：

```yaml
name: livepilot

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $$POSTGRES_USER -d $$POSTGRES_DB"]
      interval: 5s
      timeout: 3s
      retries: 20
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    ports:
      - "${REDIS_PORT}:6379"
    volumes:
      - redis_data:/data
    command: ["redis-server", "--appendonly", "yes"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 20
    restart: unless-stopped

volumes:
  postgres_data:
  redis_data:
```

镜像标签应在团队确定后固定并定期更新；不要在生产环境使用未经评估的浮动版本。

启动服务：

```bash
cd ~/project/LivePilot
docker compose up -d
docker compose ps
```

预期 `postgres` 和 `redis` 最终均为 `healthy`。首次拉取镜像需要网络连接。

验证两个服务：

```bash
docker compose exec postgres pg_isready -U livepilot -d livepilot
docker compose exec redis redis-cli ping
```

预期 PostgreSQL 返回“accepting connections”，Redis 返回 `PONG`。

常用维护命令：

```bash
docker compose logs -f postgres
docker compose logs -f redis
docker compose exec postgres psql -U livepilot -d livepilot
docker compose down
```

`docker compose down` 只停止服务，保留数据卷。仅在明确要删除本机开发数据时使用 `docker compose down -v`。

### 7.3 后端连接字符串

创建 `backend/.env`，不要提交：

```dotenv
# backend/.env
APP_ENV=development
LOG_LEVEL=INFO
BACKEND_HOST=0.0.0.0
BACKEND_PORT=8000
FRONTEND_ORIGIN=http://localhost:5173

DATABASE_URL=postgresql+asyncpg://livepilot:replace-with-a-long-local-password@localhost:5432/livepilot
REDIS_URL=redis://localhost:6379/0

OTEL_SERVICE_NAME=livepilot-api
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318

JWT_SECRET=replace-with-a-long-random-development-secret
REALTIME_PROVIDER_BASE_URL=https://provider.example
REALTIME_PROVIDER_API_KEY=replace-me
REALTIME_MODEL=replace-with-provider-model-id
WEATHER_API_KEY=replace-me
MAP_API_KEY=replace-me
```

同时创建 `backend/.env.example`，保留变量名但用 `change-me` 或空值替代敏感信息。

连接地址取决于运行位置：

| 后端运行位置 | `DATABASE_URL` 中的主机 | `REDIS_URL` 中的主机 |
| --- | --- | --- |
| 直接在 WSL 运行 `uv run ...` | `localhost` | `localhost` |
| 将 api/worker 也放进 Compose | `postgres` | `redis` |

不要把容器服务名 `postgres` 写入 WSL 原生启动的后端配置，也不要把 `localhost` 写入容器化 API 配置。

## 8. 第六阶段：数据库迁移

Alembic 初始化必须在 SQLAlchemy 模型、异步引擎和 `alembic.ini` 的连接配置确定后进行。首次初始化时，在 `backend/` 执行：

```bash
uv run alembic init alembic
```

之后的日常迁移流程：

```bash
# 修改 SQLAlchemy 模型后
uv run alembic revision --autogenerate -m "create sessions and tasks"

# 应用到本地 PostgreSQL
uv run alembic upgrade head

# 查看当前迁移版本
uv run alembic current

# 回滚一个迁移
uv run alembic downgrade -1
```

迁移文件应提交到 Git；不要在 Worker 或 FastAPI 进程启动时自动执行迁移。先在开发环境审查自动生成的 SQL，再执行 `upgrade`。

本项目的首批表应至少覆盖 `sessions`、`turns`、`transcripts`、`preferences`、`tasks`、`tool_calls`、`itineraries` 与 `event_outbox`。这些表的版本控制关系详见 `LivePilotInstruction.md`，重点是 `context_version` 与条件写入，不能被简化为“任务结束即覆盖结果”。

## 9. 第七阶段：Redis Streams 与 Worker

### 9.1 Redis 的职责

Redis 不是权威数据存储。它用于：

- 流 `travel.tasks`：网关入队、Worker 使用 Consumer Group 消费。
- 取消标记：例如 `cancel:task:{task_id}` 与 `cancel:turn:{turn_id}`。
- 短期去重、限流与事件分发。

任务、工具调用结果、偏好版本和最终行程必须写入 PostgreSQL。Worker 处理迟到结果时，必须检查其 `context_version` 是否仍等于会话当前版本；不一致则标记 `discarded`，不得覆盖行程。

### 9.2 首次开发时的 Worker 启动方式

在创建 `backend/app/worker.py` 后，建议定义一个明确的模块入口，例如：

```bash
cd ~/project/LivePilot/backend
uv run python -m app.worker
```

这个命令只有在 `app/worker.py` 存在且实现了 Redis Streams Consumer Group 后才有效。它应与 FastAPI 分开运行：

| 终端 | 命令 | 职责 |
| --- | --- | --- |
| 终端 1 | `docker compose up` | PostgreSQL 与 Redis |
| 终端 2 | `uv run uvicorn app.main:app ...` | HTTP、WebSocket、会话网关 |
| 终端 3 | `uv run python -m app.worker` | 外部工具任务、重试、条件落库 |
| 终端 4 | `npm run dev` | React 前端 |

不要在 FastAPI 的请求处理器内等待天气、景点或路线任务完成；请求只应创建 task、写 outbox、`XADD` 消息并快速返回。

### 9.3 最小 Redis 验证

基础服务运行时可检查流和消费者组：

```bash
docker compose exec redis redis-cli XADD travel.tasks '*' type smoke-test
docker compose exec redis redis-cli XRANGE travel.tasks - +
```

这只验证 Redis Streams 可用，不等于 Worker 已正确实现。Worker 就绪后的冒烟测试必须进一步验证：任务写入、Worker 消费、PostgreSQL 条件落库、事件回传和 `XACK`。

## 10. 第八阶段：CORS、WebRTC 与实时密钥边界

### 10.1 CORS

开发环境后端只允许 Vite 的确切来源：

```dotenv
FRONTEND_ORIGIN=http://localhost:5173
```

FastAPI CORS 配置应读取该变量，允许必要的方法、请求头和凭证。不要使用“允许所有来源”与 `allow_credentials=true` 的组合。

### 10.2 WebRTC 链路

正确链路：

```text
Windows 浏览器
  -> FastAPI：获取一次性、短有效期实时令牌
  -> 实时语音供应商：浏览器以 WebRTC 传输音频和数据通道事件
  -> 浏览器：播放远端音频轨
```

FastAPI 的职责是核验用户与会话、向供应商换取短期令牌、记录会话状态和发送控制事件。音频不应经过 FastAPI 或 Worker。

具体供应商的模型名、SDP 接口、事件名和取消事件必须由 `RealtimeProviderAdapter` 隔离。接入时以该供应商的当前官方文档为准；不要把猜测的模型 ID、URL 或长效 Key 写死在前端。

### 10.3 打断必须先本地执行

用户插话或点击停止时，前端顺序应是：

1. 同步暂停并解绑当前远端音频，立即停止听到旧回复。
2. 经实时模型的数据通道发送取消事件。
3. 非阻塞发送 `POST /interrupt` 或 WebSocket `agent.interrupt` 给 FastAPI。
4. 网关写入取消标记，Worker 检查该标记并对旧版本结果执行丢弃。

本地停播不能等待 HTTP、Redis 或 Worker 响应。

## 11. 第九阶段：基础可观测性

在应用能启动且 Redis/PostgreSQL 验证通过后，再加入 OTel Collector、Prometheus 与 Grafana。项目说明书要求追踪从浏览器到网关、Redis 任务、Worker 和工具调用的同一 `trace_id`。

第一步应在 API 和 Worker 中设置：

```dotenv
OTEL_SERVICE_NAME=livepilot-api
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

Worker 使用不同服务名，例如 `livepilot-worker`。基础指标至少包括：

- 首段语音响应与音频首包延迟。
- 用户打断生效时间和模型取消确认延迟。
- Redis 队列等待、任务完成、工具调用耗时。
- Worker、数据库和实时连接错误率。

不要为了“先有监控”而在前端、日志或指标标签中放完整转写内容、访问令牌、精确位置或第三方原始响应。

## 12. 完整本地启动顺序

后端、Worker 与 Compose 文件创建完成后，按以下顺序启动。

### 12.1 终端 1：基础服务

```bash
cd ~/project/LivePilot
docker compose up -d
docker compose ps
```

确认 PostgreSQL 和 Redis 为 `healthy` 后再继续。

### 12.2 终端 2：数据库迁移和 API

```bash
cd ~/project/LivePilot/backend
uv sync
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

验证：

```bash
curl -i http://localhost:8000/health
```

### 12.3 终端 3：Worker

```bash
cd ~/project/LivePilot/backend
uv run python -m app.worker
```

Worker 应在启动日志中显示 Redis 连接成功、Consumer Group 已创建或已加入。日志不得输出 `DATABASE_URL`、令牌或工具 API Key。

### 12.4 终端 4：前端

```bash
cd ~/project/LivePilot/frontend
npm run dev
```

打开 `http://localhost:5173`。前端应能调用 API 健康检查、创建会话、显示任务状态；只有后端短期令牌接口完成后才测试麦克风和 WebRTC。

## 13. 验收清单

按顺序完成下列检查：

- [ ] `docker version` 同时显示 Client 与 Server，且 Docker 使用 WSL 原生 Engine。
- [ ] `frontend/` 中 `npm run build` 与 `npm run lint` 成功。
- [ ] `uv python find 3.12` 找到项目 Python。
- [ ] `backend/pyproject.toml` 的 `requires-python` 为 `>=3.12`。
- [ ] `uv sync` 后 `uv run python --version` 为 3.12.x。
- [ ] `docker compose ps` 显示 PostgreSQL 和 Redis 健康。
- [ ] `pg_isready` 成功，`redis-cli ping` 返回 `PONG`。
- [ ] `backend/.env`、根 `.env` 和 `frontend/.env.local` 均未被 Git 跟踪。
- [ ] `/health` 返回 200。
- [ ] Alembic 已应用迁移，数据库包含必要的会话和任务表。
- [ ] API 与 Worker 分别在独立进程运行。
- [ ] 旧 `context_version` 的任务结果被标记 `discarded`，未覆盖最新偏好或行程。
- [ ] 长期 API Key 不存在于浏览器构建产物、前端变量、日志或接口响应。
- [ ] 浏览器在 `localhost` 下能请求麦克风；实时连接使用后端签发的短期令牌。
- [ ] 日志和 trace 可按 `trace_id` 关联 API、Worker 和工具调用。

## 14. 常见问题

### `uv python install 3.12` 后系统仍显示 3.10

这是正常的。`python3` 指向系统 Python；使用以下命令确认项目解释器：

```bash
cd ~/project/LivePilot/backend
uv run python --version
```

### `uv sync` 使用了错误的 Python

确认当前位置包含 `pyproject.toml` 和 `.python-version`：

```bash
pwd
cat .python-version
uv python find 3.12
```

必要时删除当前项目虚拟环境后重新创建：

```bash
rm -rf .venv
uv sync
```

仅删除 `backend/.venv`，不要删除系统 Python 或 uv 的全局缓存。

### Docker 容器无法启动

先查看状态和日志：

```bash
cd ~/project/LivePilot
docker compose ps
docker compose logs postgres
docker compose logs redis
```

常见原因是端口 5432/6379 已被其他服务占用，或根目录 `.env` 缺少 `POSTGRES_PASSWORD`。检查端口：

```bash
ss -ltnp '( sport = :5432 or sport = :6379 )'
```

### 前端请求 API 时出现 CORS 错误

确认三处值一致：

1. 浏览器实际地址，例如 `http://localhost:5173`。
2. `frontend/.env.local` 中的 `VITE_API_BASE_URL`。
3. `backend/.env` 中的 `FRONTEND_ORIGIN`。

修改后重启前端和后端。不要用宽松 CORS 配置掩盖地址不一致问题。

### 浏览器无法使用麦克风

先在浏览器站点权限中允许麦克风。`http://localhost` 可用于本地开发；通过局域网 IP 访问时应改用 HTTPS。WebRTC 连接断开或令牌过期时必须获取新短期令牌，不要重用旧令牌。

### Redis 连接失败

确认基础服务正常：

```bash
docker compose exec redis redis-cli ping
```

然后确认后端运行位置与 `REDIS_URL` 主机一致：WSL 原生后端用 `localhost`，容器化后端用 `redis`。

### 提交前发现密钥已被 Git 跟踪

先确认状态：

```bash
git status --short
git check-ignore -v .env backend/.env frontend/.env.local
```

如果密钥已经提交过，删除文件并不足够；应立即到对应供应商撤销并重建该密钥，再清理 Git 历史。
