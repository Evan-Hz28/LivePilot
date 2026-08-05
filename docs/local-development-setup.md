# LivePilot 本地开发环境准备

这是一份用于准备开发机的说明，不是项目实现清单。默认环境为 Windows 11 + WSL2 Ubuntu + Docker Desktop；所有项目命令均在 WSL 中运行，只有安装 WSL、启动 Docker Desktop 和使用浏览器在 Windows 中完成。

## 当前仓库状态

当前仓库只包含项目说明文档，尚未发现以下运行配置：

- `frontend/package.json`
- `backend/pyproject.toml` 或根目录 `pyproject.toml`
- `compose.yaml`
- 后端应用入口和 `/health` 接口

因此，本指南只安装通用开发依赖；后端、前端、数据库和 Worker 的启动命令必须等相应项目文件加入后才可运行。

## 1. Windows 与 WSL2

在 **Windows PowerShell（管理员）** 执行：

```powershell
wsl --install
wsl --update
wsl -l -v
```

Ubuntu 的 `VERSION` 必须为 `2`。如需转换发行版：

```powershell
wsl --set-version Ubuntu 2
```

安装并启动 Docker Desktop，然后在 **Settings > Resources > WSL Integration** 中启用 Ubuntu。安装 VS Code 和 **Remote - WSL** 扩展。

## 2. WSL Ubuntu 基础依赖

在 **WSL Ubuntu** 中执行：

```bash
sudo apt update
sudo apt install -y git curl build-essential libpq-dev ca-certificates
```

项目应放在 Linux 文件系统中，例如 `~/projects/LivePilot`，而不是 `/mnt/c/...`。检查当前位置：

```bash
pwd
```

确认 Docker Desktop 已连接到 WSL：

```bash
docker version
docker compose version
```

若 Docker 无法连接，先确认 Docker Desktop 正在运行，并重新检查 WSL Integration。不要在 WSL 内另装一套 Docker Engine。

## 3. Node.js

使用 `nvm` 安装 Node.js LTS：

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
source ~/.bashrc
nvm install --lts
nvm use --lts
node --version
npm --version
```

前端工程加入后，在 `frontend` 目录执行：

```bash
npm install
npm run dev -- --host 0.0.0.0
```

Windows 浏览器通常通过 `http://localhost:5173` 访问 Vite。前端只能保存公开变量，例如：

```dotenv
# frontend/.env.local
VITE_API_BASE_URL=http://localhost:8000
```

不要在前端变量中写入模型 API Key、数据库密码、Redis 地址或 JWT 私钥。

## 4. Python

使用 `uv` 管理 Python 3.12+：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.local/bin/env
uv python install 3.12
uv --version
python3 --version
```

Python 后端加入 `pyproject.toml` 后，在项目根目录执行：

```bash
uv sync
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

应用提供 `/health` 后，可验证：

```bash
curl -i http://localhost:8000/health
```

`--host 0.0.0.0` 使 Windows 浏览器能通过 `http://localhost:8000` 访问 WSL 内服务。

## 5. PostgreSQL 与 Redis

项目加入 `compose.yaml` 后，从仓库根目录启动本地基础服务：

```bash
docker compose up -d
docker compose ps
docker compose exec postgres pg_isready -U <postgres-user> -d <postgres-db>
docker compose exec redis redis-cli ping
```

预期 PostgreSQL 可用、Redis 返回 `PONG`。停止服务但保留数据：

```bash
docker compose down
```

只有需要删除本地开发数据时才执行 `docker compose down -v`。

本地连接串约定：

```dotenv
DATABASE_URL=postgresql+psycopg://<user>:<password>@localhost:5432/<database>
REDIS_URL=redis://localhost:6379/0
```

## 6. 环境变量与密钥

应提交不含真实值的 `.env.example`；实际 `.env`、`backend/.env` 和 `frontend/.env.local` 应加入 `.gitignore`。

| 变量 | 放置位置 | 是否敏感 | 说明 |
| --- | --- | --- | --- |
| `OPENAI_API_KEY` | 后端 `.env` | 是 | 长期 API Key，仅后端读取 |
| `REALTIME_MODEL` | 后端 `.env` | 否 | 实时模型标识 |
| `DATABASE_URL` | 后端 `.env` | 是 | PostgreSQL 连接串 |
| `REDIS_URL` | 后端 `.env` | 视部署而定 | Redis 连接串 |
| `FRONTEND_ORIGIN` | 后端 `.env` | 否 | 例如 `http://localhost:5173` |
| `JWT_SECRET` | 后端 `.env` | 是 | 高熵令牌签名密钥 |
| `VITE_API_BASE_URL` | 前端 `.env.local` | 否 | 后端公开访问地址 |

实时语音场景下，浏览器只能从后端获取短期凭证；长期 `OPENAI_API_KEY` 绝不能下发到浏览器。

## 7. 端口与排查

| 服务 | 默认端口 |
| --- | ---: |
| Vite | 5173 |
| FastAPI | 8000 |
| PostgreSQL | 5432 |
| Redis | 6379 |

检查端口占用：

```bash
ss -ltnp '( sport = :5173 or sport = :8000 or sport = :5432 or sport = :6379 )'
```

常见问题：

- Windows 无法访问前端或 API：确认进程使用 `--host 0.0.0.0`，然后检查端口占用。
- `uv` 不存在：执行 `source ~/.local/bin/env`，再打开新 WSL 终端。
- Docker daemon 无法连接：启动 Docker Desktop，并启用发行版的 WSL Integration。
- 连接 PostgreSQL 或 Redis 失败：执行 `docker compose ps` 与 `docker compose logs`，检查容器状态和连接串。
- 修改 `.env` 后配置未更新：重启对应的前端、后端或 Worker 进程。
