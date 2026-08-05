# LivePilot 系统流程图

本文以图示方式说明 LivePilot 的核心工作流、异常处理和基础设施关系。

> 当前已跑通：FastAPI 创建 `smoke_test` 任务、PostgreSQL 持久化、Redis Streams 入队、Worker 消费并写回结果。
>
> 其余流程是依据 `LivePilotInstruction.md` 规划的目标架构，需在后续阶段实现。

## 1. 系统总览

```mermaid
flowchart TB
    subgraph Browser[Windows 浏览器]
        FE[React 前端]
        RTC[WebRTC 连接与音频控制]
        FE --> RTC
    end

    subgraph Application[WSL 应用服务]
        API[FastAPI 会话网关]
        AG[Agent Worker]
        TW[工具 Worker]
        R[(Redis Streams)]
        DB[(PostgreSQL)]
    end

    subgraph Provider[实时语音供应商]
        RM[实时语音模型]
    end

    subgraph Tools[外部旅行工具]
        WX[天气]
        POI[景点 / 餐厅]
        MAP[路线 / 预算]
    end

    FE -->|HTTPS / WebSocket| API
    RTC <-->|WebRTC 音频 / 数据通道| RM
    API <--> DB
    API --> R
    R --> AG
    R --> TW
    AG <--> DB
    TW <--> DB
    TW --> WX
    TW --> POI
    TW --> MAP
```

媒体面与控制面分离：浏览器直接和实时语音供应商交换 WebRTC 音频；FastAPI 不代理音频，只处理会话、令牌、任务和控制事件。

## 2. 核心业务主流程

这是用户说出旅行需求后，完成异步查询并获得下一步语音回复的正常成功路径。

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器
    participant API as FastAPI
    participant R as Redis Streams
    participant AW as Agent Worker
    participant TW as 工具 Worker
    participant DB as PostgreSQL
    participant RM as 实时语音模型

    B->>RM: WebRTC 上行用户语音
    RM-->>B: 实时短确认语音
    B->>API: 最终转写 / 意图摘要
    API->>DB: 保存 turn、偏好、context_version
    API->>R: XADD agent.plan

    R->>AW: 消费 agent.plan
    AW->>DB: 读取最新会话、偏好、行程
    AW->>AW: 决定需要的工具
    AW->>DB: 创建 weather / attraction 等 tasks
    AW->>R: XADD 工具任务

    R->>TW: 消费工具任务
    TW->>DB: 条件写入有效工具结果
    TW->>R: XADD agent.compose

    R->>AW: 消费 agent.compose
    AW->>DB: 读取最新结果并生成下一步回复
    AW-->>API: 回复上下文或行程草案
    API-->>B: WebSocket 控制事件
    B->>RM: 更新上下文，请求语音回复
    RM-->>B: WebRTC 下行远端音频
```

## 3. 当前最小任务链路

这是当前本地已实现的 `smoke_test` 验证流程。它是未来 Agent 与旅行工具任务的最小基础。

```mermaid
sequenceDiagram
    autonumber
    participant C as curl / 前端
    participant API as FastAPI
    participant PG as PostgreSQL
    participant R as Redis Streams
    participant W as Python Worker

    C->>API: POST /demo/tasks/smoke-test
    API->>PG: INSERT sessions
    API->>PG: INSERT tasks(status=queued)
    PG-->>API: 事务提交

    API->>R: XADD travel.tasks(task_id, smoke_test)
    R-->>API: message_id
    API-->>C: 202 task_id, queued

    W->>R: XREADGROUP travel.tasks
    R-->>W: task_id
    W->>PG: UPDATE tasks status=running
    W->>W: 执行 smoke_test
    W->>PG: UPDATE tasks status=succeeded, result
    W->>R: XACK message_id

    C->>API: GET /demo/tasks/task_id
    API->>PG: SELECT task
    PG-->>API: succeeded, result
    API-->>C: 任务状态和结果
```

## 4. 用户打断与旧结果丢弃

打断的第一目标是立即停止浏览器本地音频，不等待网络。随后网关增加 `context_version` 并取消旧任务；迟到结果只保留审计信息，不能覆盖当前行程。

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器
    participant RM as 实时语音模型
    participant API as FastAPI
    participant R as Redis
    participant DB as PostgreSQL
    participant W as Worker

    RM-->>B: 正在播放 Agent 音频
    B->>B: pause / detach 远端音频轨
    B->>RM: response.cancel
    B->>API: agent.interrupt 或 preference.update

    API->>DB: context_version 原子递增
    API->>DB: 旧任务标记 cancel_requested
    API->>R: 写入 cancel:task 键
    API-->>B: interrupt.accepted

    W->>W: 旧工具调用晚到
    W->>DB: 条件更新，检查 context_version
    alt 版本仍有效
        DB-->>W: 允许写入当前结果
    else 版本已过期或已取消
        DB-->>W: 拒绝当前结果写入
        W->>DB: task = discarded，仅保留审计
    end
```

## 5. WebRTC 或网络断开后的恢复

浏览器内的部分转写和临时播放状态不可靠。重连时以 PostgreSQL 中的权威快照为基线，并使用新短期令牌重新建立 WebRTC。

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器
    participant API as FastAPI
    participant DB as PostgreSQL
    participant RM as 实时语音模型

    B--xRM: WebRTC 或网络断开
    B->>B: 停止播放，记录 last_event_seq
    B->>API: POST /sessions/id/resume

    API->>DB: 读取 session、偏好、任务、行程、事件
    DB-->>API: 权威快照和 missed events
    API-->>B: snapshot、新实时令牌

    B->>B: 丢弃旧推测态，合并权威快照
    B->>RM: 使用新令牌创建 RTCPeerConnection
    RM-->>B: connection.ready
    B-->>API: session.resumed
```

## 6. Worker 超时、失败与重试

Worker 只有在 PostgreSQL 成功提交任务终态后才确认 Redis 消息。这样即使进程崩溃，未确认消息也可以被其他 Worker 接管。

```mermaid
sequenceDiagram
    autonumber
    participant R as Redis Streams
    participant W1 as Worker 1
    participant DB as PostgreSQL
    participant Tool as 外部工具
    participant W2 as Worker 2

    R->>W1: XREADGROUP 领取任务
    W1->>DB: task = running
    W1->>Tool: 调用天气 / 地图 / 景点

    alt 工具成功
        Tool-->>W1: 结果
        W1->>DB: 保存结果，task = succeeded
        W1->>R: XACK
    else 可重试错误 / 超时
        Tool-->>W1: timeout / 429 / 5xx
        W1->>DB: attempt 加一，task = retry_wait
        W1->>R: 延迟后重新入队或重领
    else Worker 崩溃
        W1--xR: 未执行 XACK
        W2->>R: XAUTOCLAIM pending 消息
        W2->>DB: 检查任务状态和幂等键
        W2->>Tool: 安全重试或结束任务
    end
```

不可重试的业务错误，例如无效城市或无效日期，直接标记为 `failed`，不进行指数退避。

## 7. 创建会话与获取短期实时令牌

长期实时模型 API Key 只存在后端。浏览器只获得绑定会话、有效期很短的一次性令牌。

```mermaid
sequenceDiagram
    autonumber
    participant B as 浏览器
    participant API as FastAPI
    participant DB as PostgreSQL
    participant RM as 实时语音供应商

    B->>API: POST /v1/sessions
    API->>DB: 创建 session 与初始 preference
    DB-->>API: session_id, context_version
    API-->>B: session_id

    B->>API: POST /sessions/id/realtime-token
    API->>API: 校验用户、会话、设备
    API->>RM: 使用长期 API Key 换取短期令牌
    RM-->>API: token, expires_at, provider_config
    API-->>B: 短期令牌和公开连接配置

    B->>RM: 使用短期令牌建立 WebRTC
    RM-->>B: 音频轨与数据通道
```

## 8. 部署与可观测性链路

开发期可以先运行 PostgreSQL、Redis、API、Worker 和前端。完整部署增加 OTel Collector、Prometheus 与 Grafana，使用同一个 `trace_id` 串联请求和任务。

```mermaid
flowchart TB
    B[Windows 浏览器] --> FE[React / Vite :5173]
    FE --> API[FastAPI :8000]
    FE <-->|WebRTC| RM[实时语音供应商]

    API --> PG[(PostgreSQL :5432)]
    API --> R[(Redis :6379)]
    R --> AW[Agent Worker]
    R --> TW[工具 Worker]
    AW --> PG
    TW --> PG

    FE -. traceparent .-> OT[OTel Collector :4318]
    API -. OTLP .-> OT
    AW -. OTLP .-> OT
    TW -. OTLP .-> OT
    OT --> PR[Prometheus :9090]
    PR --> GF[Grafana :3000]
```

需要追踪的重点包括：首段音频响应延迟、用户打断生效时间、队列等待时间、工具调用耗时、任务失败率、WebRTC 重连次数，以及旧任务被 `discarded` 的比例。
