# LivePilot 项目说明书

> 项目类型：全双工实时语音旅行规划 Agent  
> 技术栈：React + TypeScript + Vite、Python + FastAPI + asyncio、WebRTC、Redis Streams、PostgreSQL、OpenTelemetry、Prometheus、Grafana、Docker Compose  
> 文档用途：产品设计、系统设计、接口实现、联调、测试与演示的共同基线。

## 1. 项目背景

旅行规划通常需要用户在天气、景点、餐厅、地图、预算和时间冲突之间反复切换。传统文本助手可以给出建议，但无法提供自然、可打断的连续对话，也难以在用户改变需求时可靠地取消过期查询并保持行程数据一致。

LivePilot 是一个面向个人用户的语音旅行规划助手。用户以自然语音描述目的地、日期、预算、同行人员与偏好，系统通过实时语音模型保持双向语音对话；涉及外部信息和较长计算的工作交给异步 Worker 执行。用户在等待期间可以继续交谈、修改偏好或打断 Agent，系统必须优先响应最新意图。

### 1.1 目标用户

| 用户 | 典型需求 | 主要困难 |
| --- | --- | --- |
| 自由行规划者 | 快速获得可执行的多日行程 | 多个信息源分散，方案调整成本高 |
| 临时出行者 | 在短时间内确认天气、路线和夜间活动 | 无法在多任务查询期间持续对话 |
| 有明确限制的出行者 | 控制预算、避开拥挤、照顾同行人员 | 限制条件容易被后续修改遗漏 |
| 语音优先用户 | 在走路、开车前准备等场景规划行程 | 文本输入不便，等待回复时无法自然插话 |

### 1.2 用户痛点与核心价值

| 痛点 | LivePilot 的处理方式 | 可验证结果 |
| --- | --- | --- |
| 等待信息查询时对话被卡住 | 实时音频和异步任务解耦，查询在 Worker 执行 | 用户可在任务运行中继续说话 |
| Agent 说话时难以打断 | 客户端立即停止远端音频，并向模型和网关发送中断 | 打断后不再听到旧回复 |
| 用户修改条件后旧查询仍影响结果 | 偏好、上下文和任务均携带版本号，迟到结果做条件提交 | 旧结果不会覆盖最新需求 |
| 长会话与断网后上下文丢失 | PostgreSQL 保存权威会话状态，Redis 保存短期控制状态 | 重连后能恢复最近轮次、偏好和任务状态 |
| 难以定位语音链路慢在哪里 | Trace ID 贯穿浏览器、网关、队列、Worker 与工具 | 可按会话和轮次定位端到端延迟 |

### 1.3 项目目标

1. 提供连续全双工语音交互：浏览器采集麦克风音频，经 WebRTC 连接实时语音模型，并播放模型返回的远端音频轨道。
2. 支持用户随时打断：停止本地播放、取消当前模型回复并以最新用户语音建立新的 turn。
3. 将工具调用、复杂推理、数据库持久化从实时音频关键路径移出，避免阻塞音频上下行。
4. 以显式会话状态、版本号、幂等键和条件写入维护多轮对话与异步任务的一致性。
5. 支持会话恢复、任务续接、旧结果丢弃和端到端可观测性。

## 2. 设计假设

以下假设用于固定实现细节。若所选实时模型供应商的事件名不同，由 `RealtimeProviderAdapter` 负责映射，不改变领域事件与状态机。

1. 实时语音模型 API 支持浏览器 WebRTC 连接、短期连接令牌、上行麦克风音频、下行远端音频轨道、数据通道控制事件，以及取消当前回复的控制语义。
2. 浏览器通过 HTTPS 访问前端与 FastAPI；生产环境由反向代理终止 TLS，WebRTC 使用供应商支持的 STUN/TURN 配置。
3. 天气、景点、餐厅和路线由已有第三方服务提供；本项目只封装工具适配器，不自研 ASR、TTS、语音模型或媒体服务器。
4. 初版使用 Redis Streams Consumer Group 和 Python asyncio Worker。若团队已有 Arq 或 Celery 基础设施，可保留本文的任务契约并替换调度实现。
5. PostgreSQL 使用 UUID 主键与 `timestamptz`；所有时间以 UTC 落库，前端按用户时区展示。
6. `context_version` 是会话中用户意图和行程事实的单调递增版本。任何会改变规划含义的偏好更新、最终转写确认或行程确认都会递增该版本。
7. 实时模型可在工具等待期间给出简短确认，但不能直接写入权威行程数据；最终事实只能经网关和 Worker 的持久化流程提交。

## 3. 核心用户流程

1. 用户打开 React 页面，完成登录并创建会话。
2. 前端向 FastAPI 会话网关请求一次性、短有效期的实时连接令牌。
3. 浏览器请求麦克风权限，创建 `RTCPeerConnection`，用令牌与实时语音模型建立 WebRTC 会话。音频不经过 FastAPI。
4. 用户说出旅行需求。模型完成实时理解并返回语音确认，例如“我先规划三天行程，再查询天气和晚间活动”。
5. 前端或模型的数据通道把结构化意图通知会话网关。网关创建 turn、写入轻量控制状态，并向 Redis Streams 投递天气、景点、餐厅、路线、预算和冲突检测任务。
6. Worker 消费任务，调用工具服务，写入可验证的工具结果和任务状态到 PostgreSQL，再通过会话事件通道通知前端和 Agent 编排器。
7. Agent 编排器基于最新上下文生成行程草案和后续语音回复请求。实时模型继续输出远端音频，浏览器播放。
8. 用户可以在任意时刻插话。前端立刻停止当前播放，发送 `agent.interrupt`，同时在实时模型会话上发送取消回复事件。
9. 若用户修改“避开人多、增加博物馆”等偏好，网关增加 `context_version`，取消旧任务或将其标为过期。迟到结果即使成功返回，也只能记录审计信息，不能写入当前行程。
10. 网络中断时，浏览器保存恢复游标；重连后向网关获取会话快照、未完成任务和最新行程，再领取新实时令牌重新建立 WebRTC。

### 3.1 从语音输入到语音输出的完整链路

```mermaid
flowchart LR
    U[用户说话] --> M[浏览器麦克风]
    M --> MS[getUserMedia MediaStream]
    MS --> PC[RTCPeerConnection 本地音频轨]
    PC -->|WebRTC 上行音频| RM[实时语音模型]
    RM -->|实时转写与控制事件| DC[数据通道]
    DC --> FE[React 会话控制器]
    FE -->|REST 或 WebSocket 控制面| GW[FastAPI 会话网关]
    RM -->|远端音频轨| RPC[RTCPeerConnection]
    RPC --> AP[Audio 元素或 Web Audio]
    AP --> S[扬声器或耳机]
    GW -->|任务消息| RS[Redis Streams]
    RS --> W[Python Worker]
    W --> TS[工具服务]
    W --> DB[(PostgreSQL)]
    W -->|任务和工具事件| GW
    GW -->|会话状态事件| FE
```

## 4. 系统架构

```mermaid
flowchart TB
    subgraph Browser[浏览器]
        R[React + TypeScript + Vite]
        RTC[WebRTC PeerConnection]
        AC[音频播放控制器]
        R --> RTC
        RTC --> AC
    end

    subgraph Realtime[实时语音服务]
        RM[实时语音模型 API]
    end

    subgraph Application[应用服务]
        GW[FastAPI 会话网关]
        AG[Agent 编排器\n显式状态机或 Python Agent SDK]
        RW[Python asyncio Worker]
        RS[(Redis Streams)]
        DB[(PostgreSQL)]
    end

    subgraph External[外部工具服务]
        WT[天气查询]
        POI[景点搜索]
        REST[餐厅搜索]
        MAP[路线规划]
        BUD[预算计算]
    end

    subgraph Observability[可观测性]
        OT[OpenTelemetry Collector]
        PR[Prometheus]
        GF[Grafana]
        OT --> PR --> GF
    end

    R <-->|HTTPS REST / WebSocket| GW
    RTC <-->|WebRTC 音频与数据通道| RM
    GW <--> AG
    GW --> RS
    RS --> RW
    RW <--> AG
    GW <--> DB
    RW <--> DB
    RW --> WT
    RW --> POI
    RW --> REST
    RW --> MAP
    RW --> BUD
    R -. Trace Context .-> OT
    GW -. OTLP .-> OT
    RW -. OTLP .-> OT
```

### 4.1 架构原则

1. **媒体面与控制面分离**：浏览器与实时语音模型直连 WebRTC；FastAPI 仅负责令牌、会话、任务与状态事件，不代理 PCM/Opus 音频。
2. **实时与异步分离**：实时路径只做连接、轻量事件转发、内存状态更新和快速取消。工具调用、复杂 Agent 推理、数据库写入和行程计算走 Worker。
3. **版本优先于完成时间**：任务结果是否有效由其请求时的 `context_version`、目标实体版本和取消标记决定，而不是由先后返回顺序决定。
4. **权威状态可重建**：Redis 用于队列、短期锁、取消信号和事件分发；PostgreSQL 是会话、任务、工具结果和最终行程的权威来源。
5. **可观测性内建**：每次会话、turn、任务和工具调用均有可关联的 Trace ID、结构化日志与延迟指标。

### 4.2 组件职责、输入、输出与依赖

| 组件 | 职责 | 输入 | 输出 | 依赖 |
| --- | --- | --- | --- | --- |
| React 前端 | 登录、会话 UI、麦克风控制、WebRTC、音频播放、打断、会话恢复、显示任务和行程 | 用户交互、REST 响应、WebSocket 事件、模型数据通道事件、远端音频轨 | API 请求、实时模型 SDP/ICE 交互、控制事件、遥测事件 | 浏览器 MediaDevices/WebRTC、FastAPI、实时模型 API |
| WebRTC `RTCPeerConnection` | 承载浏览器到实时模型的上行麦克风轨、下行远端音频轨与可选数据通道 | 本地 `MediaStreamTrack`、SDP、ICE、短期令牌 | 加密媒体包、`track` 事件、连接状态 | 实时模型 API、STUN/TURN、浏览器网络 |
| 实时语音模型 API | 实时理解上行语音、生成下行语音、提供转写与控制事件 | WebRTC 媒体、数据通道控制、实时令牌、上下文指令 | 远端音频轨、部分或最终转写、回复/取消状态 | 模型供应商服务 |
| FastAPI 会话网关 | 鉴权、创建会话、签发实时令牌、会话快照、偏好更新、打断登记、任务派发、控制事件推送 | HTTPS/WS 请求、Worker 回传、Agent 计划 | JSON API、会话事件、Redis Streams 消息 | PostgreSQL、Redis、令牌服务、Agent 编排器 |
| Agent 编排器 | 将最终转写、偏好、行程摘要和工具结果转为下一步行动；决定回复、工具任务、行程草案 | 会话上下文、工具结果、用户意图、任务状态 | `AgentDecision`、任务定义、回复指令、行程草案 | Python Agent SDK 或状态机、上下文构建器 |
| Redis Streams | 可靠传递异步任务；提供 Consumer Group、短期取消标记、幂等去重辅助和事件扇出 | 任务消息、取消消息、Worker ACK | 待消费任务、消费者确认、临时状态 | Redis 高可用实例 |
| Python Worker | 消费任务、执行工具、超时与重试、条件落库、结果回传、生成后续编排事件 | Redis 消息、任务记录、取消标记 | 工具调用、任务状态、权威结果、会话事件 | Redis、PostgreSQL、工具服务、Agent 编排器 |
| 工具服务 | 返回旅行领域外部数据与计算结果 | 标准化工具请求 | 标准化成功或错误结果 | 天气/地图/POI/餐厅等第三方 API |
| PostgreSQL | 保存权威会话、turn、转写、偏好、任务、工具调用、行程和追踪关联 | 网关与 Worker 的事务写入 | 事务结果、会话快照、审计记录 | PostgreSQL、迁移工具 |
| OpenTelemetry | 收集 traces、metrics 与 logs 的关联上下文 | 浏览器、网关、Worker 的遥测 | OTLP spans、metrics、log correlation | OTel Collector、Prometheus、Grafana |

## 5. WebRTC 音频链路设计

### 5.1 建连与采集

1. 前端调用 `POST /v1/sessions/{session_id}/realtime-token`，网关完成用户、会话与设备校验后返回供应商连接参数和有效期不超过 60 秒的一次性令牌。
2. 用户点击开始语音后，前端调用：

```ts
const stream = await navigator.mediaDevices.getUserMedia({
  audio: {
    echoCancellation: true,
    noiseSuppression: true,
    autoGainControl: true,
    channelCount: 1,
  },
  video: false,
});
```

3. 前端创建 `RTCPeerConnection`，将 `stream.getAudioTracks()[0]` 作为上行轨添加。浏览器负责采集、编码和 SRTP 加密；应用不读取、转存或转发原始音频。
4. 前端用实时令牌向模型供应商完成 offer/answer 和 ICE 协商。令牌过期、会话失效或 ICE 失败均不得静默重试旧令牌。

### 5.2 下行播放与状态

1. `pc.ontrack` 收到模型的远端音频轨后，绑定到稳定的 `<audio autoplay playsinline>` 元素，或连接到 `AudioContext` 的 `MediaStreamAudioSourceNode`。
2. 播放控制器维护 `playbackId`、`turnId`、`startedAt`、`state`。只有当前活跃的 `turnId` 可以驱动 UI 播放状态。
3. 页面必须监听 `connectionState`、`iceConnectionState`、`track.muted`、`audio.play()` 失败等事件，分别展示重连、无音频和浏览器自动播放受限状态。
4. 模型音频首包到达时写入前端指标 `realtime_audio_first_packet_ms`；第一个实际可听帧播放时写入 `speech_first_playout_ms`。

### 5.3 用户打断时停止播放

打断处理不能等待 HTTP 请求完成。前端接收到用户开始说话、显式点击停止或本地 VAD 判定插话时，按如下顺序执行：

1. 同步停止本地播放器：`audio.pause()`、清空 `srcObject` 中当前远端轨或断开 Web Audio 节点，并将该 `playbackId` 加入本地 `cancelledPlaybackIds`。
2. 通过实时模型数据通道发送 `response.cancel`（名称由适配器映射）；若数据通道不可用，立即发起非阻塞 HTTP `POST /interrupt` 作为控制面补偿。
3. 通过 WebSocket 或 HTTP 向网关发送 `agent.interrupt`，包含 `session_id`、`turn_id`、`playback_id`、`client_event_id` 和 `occurred_at`。
4. 网关在 Redis 写入短期取消标记，并把当前 turn 状态更新为 `interrupted` 或 `superseded`。该写入应使用极短事务，不调用工具或进行复杂推理。
5. 新用户语音形成新 turn 后，编排器基于最新偏好和上下文决定是否创建替代任务。

### 5.4 禁止事项

- FastAPI 不直接生成、编码、转发或播放语音。
- FastAPI 不在 WebRTC 音频关键路径同步执行 LLM 推理、外部工具调用、PostgreSQL 大查询或行程计算。
- Worker 不持有浏览器音频轨，也不把任务完成与音频播放生命周期绑定。
- 客户端不因等待工具结果暂停上行麦克风、事件循环或 WebRTC 连接。

## 6. 关键交互时序

### 6.1 普通语音问答

```mermaid
sequenceDiagram
    autonumber
    participant U as 用户
    participant FE as React 浏览器
    participant RM as 实时语音模型
    participant GW as FastAPI 网关
    participant DB as PostgreSQL

    U->>FE: 说出普通问题
    FE->>RM: WebRTC 上行麦克风音频
    RM-->>FE: partial_transcript / final_transcript
    FE->>GW: turn.finalize（轻量元数据）
    GW->>DB: 写入最终 turn 和 transcript
    DB-->>GW: commit
    RM-->>FE: 远端音频轨
    FE->>FE: 播放器绑定轨道并播放
    FE-->>GW: playback.started（异步遥测）
```

### 6.2 用户打断 Agent

```mermaid
sequenceDiagram
    autonumber
    participant U as 用户
    participant FE as React 浏览器
    participant RM as 实时语音模型
    participant GW as FastAPI 网关
    participant R as Redis

    RM-->>FE: 正在输出远端语音
    U->>FE: 插话或点击停止
    FE->>FE: 同步 pause / detach，停止本地播放
    FE->>RM: response.cancel（数据通道）
    FE->>GW: agent.interrupt
    GW->>R: SET cancel:turn:{id} TTL 10min
    GW-->>FE: interrupt.accepted
    RM-->>FE: response.cancelled
    U->>FE: 继续说新的需求
```

### 6.3 异步查询天气和景点

```mermaid
sequenceDiagram
    autonumber
    participant FE as React 浏览器
    participant RM as 实时语音模型
    participant GW as FastAPI 网关
    participant RS as Redis Streams
    participant W as Python Worker
    participant T as 天气/景点工具
    participant DB as PostgreSQL

    RM-->>FE: "我先查询天气和景点"
    FE->>GW: turn.finalize + 意图摘要
    GW->>DB: 创建 turn、task、tool_call（queued）
    GW->>RS: XADD travel.tasks
    GW-->>FE: task.queued
    RS->>W: XREADGROUP 获取任务
    W->>DB: task=running
    par 天气查询
        W->>T: weather.get
        T-->>W: 天气结果
    and 景点查询
        W->>T: attraction.search
        T-->>W: 景点结果
    end
    W->>DB: 条件写入结果和 task=succeeded
    W-->>GW: task.result.ready
    GW-->>FE: task.succeeded
```

### 6.4 工具结果返回并继续语音

```mermaid
sequenceDiagram
    autonumber
    participant W as Python Worker
    participant DB as PostgreSQL
    participant AG as Agent 编排器
    participant GW as FastAPI 网关
    participant FE as React 浏览器
    participant RM as 实时语音模型

    W->>DB: 保存已验证工具结果
    W->>AG: ToolResultReady(session, context_version)
    AG->>DB: 读取最新偏好、行程和摘要
    AG->>AG: 生成下一步决策与回复上下文
    AG->>GW: agent.reply.request
    GW-->>FE: agent.reply.available（控制事件）
    FE->>RM: 发送更新后的会话上下文/回复请求
    RM-->>FE: 远端音频轨
    FE->>FE: 播放后续语音回复
```

### 6.5 网络断开后的会话恢复

```mermaid
sequenceDiagram
    autonumber
    participant FE as React 浏览器
    participant GW as FastAPI 网关
    participant DB as PostgreSQL
    participant RM as 实时语音模型

    FE--xRM: WebRTC 连接中断
    FE->>FE: 停止播放，记录 last_event_seq 和 active_turn
    FE->>GW: POST /sessions/{id}/resume（恢复游标）
    GW->>DB: 读取权威快照、任务、行程和事件序号
    DB-->>GW: session snapshot
    GW-->>FE: snapshot + missed_events + 新实时令牌
    FE->>FE: 合并快照，丢弃旧推测态
    FE->>RM: 使用新令牌重新建立 WebRTC
    RM-->>FE: connection.ready
    FE-->>GW: session.resumed
```

### 6.6 修改偏好后旧任务结果迟到

```mermaid
sequenceDiagram
    autonumber
    participant U as 用户
    participant FE as React 浏览器
    participant GW as FastAPI 网关
    participant DB as PostgreSQL
    participant W as Python Worker
    participant T as 工具服务

    W->>T: 按 context_version=12 查询热门夜游
    U->>FE: "不要人多，多安排博物馆"
    FE->>GW: PATCH preference，base_context_version=12
    GW->>DB: context_version 原子递增至 13
    GW->>DB: 将 version=12 的未完成任务标记 cancel_requested
    W->>T: 旧查询仍返回成功
    W->>DB: 条件更新 WHERE task.context_version = sessions.context_version
    DB-->>W: 0 rows affected
    W->>DB: task=discarded，保留审计结果
    W-->>GW: task.result.discarded
    GW-->>FE: 不更新当前行程，仅显示已按新偏好重规划
```

## 7. 实时关键路径与异步路径

### 7.1 路径划分

| 路径 | 允许操作 | 禁止或应迁移的操作 | 目标 |
| --- | --- | --- | --- |
| 浏览器 WebRTC 音频路径 | 麦克风采集、轨道发送、接收、解码、播放、本地打断 | 同步等待网关、工具或数据库响应 | 让音频持续、可打断 |
| 实时模型控制路径 | 发送会话指令、最终转写、取消当前回复、极小上下文更新 | 等待多个工具完成、做行程计算、写复杂业务事务 | 快速形成短语音响应 |
| FastAPI 快速控制路径 | 鉴权、令牌签发、短事务状态更新、入队、取消标记、事件推送 | 外部 API 调用、长 LLM 推理、大量聚合查询、等待 Worker | P95 在数百毫秒内完成 |
| Worker 异步路径 | 工具调用、路线和预算计算、冲突检测、上下文压缩、行程版本写入、重试 | 直接控制客户端音频轨 | 可恢复、可重试、可审计 |

### 7.2 不能阻塞实时音频的操作

下列操作不得放入浏览器音频回调、WebRTC 协商处理、实时模型事件处理器或 FastAPI 的实时请求处理器中：

- 天气、景点、餐厅、地图等第三方 HTTP 调用。
- Agent 的复杂任务分解、多轮工具选择、长上下文压缩与大模型推理。
- PostgreSQL 的行程聚合、历史检索、大批量写入与事务重试。
- Redis Streams 消费、等待任务完成、重试退避和取消清理。
- 导出行程、图片生成或任何可能超过单个事件循环节拍的 CPU 计算。

FastAPI 可在控制面做一次短小、可超时的事务，例如创建 task 记录并 `XADD` 任务消息。若 Redis 入队未成功，使用 Outbox 记录，由后台投递器重试，HTTP 请求仍应及时返回明确状态。

## 8. 会话状态模型

### 8.1 领域实体与关键字段

| 实体 | 作用 | 关键字段 |
| --- | --- | --- |
| `session` | 用户一次连续旅行规划会话的容器 | `id`、`user_id`、`status`、`context_version`、`last_event_seq`、`realtime_connection_epoch`、`active_turn_id` |
| `turn` | 一次用户输入到 Agent 回复或工具等待的工作单元 | `id`、`session_id`、`sequence_no`、`kind`、`status`、`context_version`、`parent_turn_id`、`interrupt_reason` |
| `transcript` | 语音转写的部分和最终版本 | `id`、`turn_id`、`source`、`text`、`is_final`、`revision`、`audio_started_at`、`audio_ended_at` |
| `preference` | 对目的地、日期、预算、同行人员与兴趣的结构化偏好版本 | `id`、`session_id`、`version`、`status`、`payload`、`source_turn_id` |
| `task` | 可独立调度的异步业务任务 | `id`、`session_id`、`turn_id`、`context_version`、`type`、`status`、`idempotency_key`、`attempt`、`deadline_at` |
| `tool_call` | 一次外部工具调用的审计与结果引用 | `id`、`task_id`、`tool_name`、`status`、`request_hash`、`input`、`output`、`error_code` |
| `itinerary` | 行程的版本化权威结果 | `id`、`session_id`、`version`、`status`、`context_version`、`content`、`budget_summary` |
| `trace` | 跨服务关联与诊断索引 | `trace_id`、`session_id`、`turn_id`、`task_id`、`span_name`、`started_at`、`ended_at`、`status` |

### 8.2 状态枚举

| 实体 | 枚举值 | 说明 |
| --- | --- | --- |
| `session.status` | `created`、`active`、`reconnecting`、`suspended`、`closed`、`failed` | `suspended` 表示实时连接不存在但会话可恢复 |
| `turn.status` | `open`、`capturing`、`user_final`、`agent_generating`、`waiting_tools`、`agent_speaking`、`completed`、`interrupted`、`superseded`、`failed` | 新偏好产生后旧 turn 使用 `superseded` |
| `transcript.status` | `partial`、`final`、`retracted` | 最终转写只能追加 revision，不可原地改写历史 |
| `preference.status` | `active`、`superseded` | 每个会话只有一个 `active` 偏好版本 |
| `task.status` | `queued`、`running`、`retry_wait`、`succeeded`、`failed`、`timed_out`、`cancel_requested`、`cancelled`、`discarded` | `discarded` 表示外部结果到达但不再可应用 |
| `tool_call.status` | `pending`、`running`、`succeeded`、`failed`、`timed_out`、`cancelled`、`discarded` | 与 task 状态独立，便于子工具并行 |
| `itinerary.status` | `draft`、`confirmed`、`superseded`、`archived` | `confirmed` 是当前可展示的权威版本 |

### 8.3 核心状态迁移

```mermaid
stateDiagram-v2
    [*] --> created
    created --> active: 创建会话并获取令牌
    active --> reconnecting: WebRTC 或网络断开
    reconnecting --> active: 恢复成功
    reconnecting --> suspended: 超过短期重连窗口
    suspended --> active: 用快照恢复
    active --> closed: 用户结束或超时清理
    active --> failed: 不可恢复的鉴权/数据错误

    state active {
        [*] --> open
        open --> capturing
        capturing --> user_final
        user_final --> agent_generating
        agent_generating --> agent_speaking
        agent_generating --> waiting_tools
        waiting_tools --> agent_generating: 当前版本工具结果就绪
        agent_speaking --> completed
        agent_speaking --> interrupted: 用户打断
        waiting_tools --> superseded: 偏好版本变化
    }
```

任务状态迁移：`queued -> running -> succeeded`；可重试错误走 `running -> retry_wait -> queued`；超时走 `running -> timed_out`；收到取消走 `queued/running -> cancel_requested -> cancelled`；若任务完成时版本失效，则任一非终态可转为 `discarded`。终态不可回退。

### 8.4 版本与并发控制规则

1. `session.context_version` 由 PostgreSQL 原子递增：`UPDATE sessions SET context_version = context_version + 1 ... RETURNING context_version`。
2. 创建 task 时固定 `task.context_version`、`task.target_preference_version` 和 `task.target_itinerary_version`，不允许 Worker 修改。
3. Worker 在保存成功结果前执行条件更新：任务仍为 `running`、未取消，且其 `context_version` 等于会话当前版本。条件不成立时结果标为 `discarded`。
4. 行程写入使用乐观锁：`WHERE itinerary.version = :expected_version AND session.context_version = :context_version`。失败时必须重新读取上下文并重新编排，不得覆盖。
5. 客户端所有可重放控制请求携带 `client_event_id`，网关以 `(session_id, client_event_id)` 去重。

## 9. PostgreSQL 权威数据设计

### 9.1 表关系

```mermaid
erDiagram
    sessions ||--o{ turns : contains
    turns ||--o{ transcripts : has
    sessions ||--o{ preferences : versions
    sessions ||--o{ tasks : owns
    turns ||--o{ tasks : initiates
    tasks ||--o{ tool_calls : executes
    sessions ||--o{ itineraries : versions
    sessions ||--o{ traces : correlates
    turns ||--o{ traces : correlates
    tasks ||--o{ traces : correlates
```

### 9.2 核心 DDL

以下 DDL 展示关键字段；`jsonb` 字段采用版本化 schema，并在应用层以 Pydantic 模型校验。

```sql
create type session_status as enum (
  'created', 'active', 'reconnecting', 'suspended', 'closed', 'failed'
);
create type turn_status as enum (
  'open', 'capturing', 'user_final', 'agent_generating', 'waiting_tools',
  'agent_speaking', 'completed', 'interrupted', 'superseded', 'failed'
);
create type task_status as enum (
  'queued', 'running', 'retry_wait', 'succeeded', 'failed', 'timed_out',
  'cancel_requested', 'cancelled', 'discarded'
);
create type itinerary_status as enum ('draft', 'confirmed', 'superseded', 'archived');

create table sessions (
  id uuid primary key,
  user_id uuid not null,
  status session_status not null default 'created',
  context_version integer not null default 0,
  last_event_seq bigint not null default 0,
  active_turn_id uuid,
  realtime_connection_epoch integer not null default 0,
  locale varchar(16) not null default 'zh-CN',
  timezone varchar(64) not null default 'Asia/Shanghai',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  closed_at timestamptz
);
create index idx_sessions_user_updated on sessions (user_id, updated_at desc);

create table turns (
  id uuid primary key,
  session_id uuid not null references sessions(id),
  sequence_no integer not null,
  parent_turn_id uuid references turns(id),
  kind varchar(24) not null check (kind in ('user_voice', 'user_text', 'agent_reply', 'system')),
  status turn_status not null,
  context_version integer not null,
  interrupt_reason varchar(64),
  started_at timestamptz not null default now(),
  finalized_at timestamptz,
  completed_at timestamptz,
  unique (session_id, sequence_no)
);
create index idx_turns_session_sequence on turns (session_id, sequence_no desc);

create table transcripts (
  id uuid primary key,
  turn_id uuid not null references turns(id),
  source varchar(16) not null check (source in ('realtime_model', 'user_edit', 'system')),
  status varchar(16) not null check (status in ('partial', 'final', 'retracted')),
  revision integer not null default 1,
  text text not null,
  language varchar(16) not null default 'zh-CN',
  audio_started_at timestamptz,
  audio_ended_at timestamptz,
  provider_event_id varchar(128),
  created_at timestamptz not null default now(),
  unique (turn_id, revision)
);
create index idx_transcripts_turn_final on transcripts (turn_id, status, revision desc);

create table preferences (
  id uuid primary key,
  session_id uuid not null references sessions(id),
  version integer not null,
  status varchar(16) not null check (status in ('active', 'superseded')),
  source_turn_id uuid references turns(id),
  payload jsonb not null,
  created_at timestamptz not null default now(),
  superseded_at timestamptz,
  unique (session_id, version)
);
create unique index uq_active_preference_per_session on preferences (session_id) where status = 'active';

create table tasks (
  id uuid primary key,
  session_id uuid not null references sessions(id),
  turn_id uuid references turns(id),
  context_version integer not null,
  target_preference_version integer not null,
  target_itinerary_version integer,
  type varchar(48) not null,
  status task_status not null default 'queued',
  idempotency_key varchar(128) not null,
  priority smallint not null default 5,
  attempt integer not null default 0,
  max_attempts integer not null default 3,
  deadline_at timestamptz not null,
  started_at timestamptz,
  finished_at timestamptz,
  cancel_requested_at timestamptz,
  payload jsonb not null,
  result_summary jsonb,
  error_code varchar(64),
  error_message text,
  trace_id varchar(32) not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (session_id, idempotency_key)
);
create index idx_tasks_runnable on tasks (status, priority, created_at) where status in ('queued', 'retry_wait');
create index idx_tasks_session_context on tasks (session_id, context_version, status);

create table tool_calls (
  id uuid primary key,
  task_id uuid not null references tasks(id),
  tool_name varchar(64) not null,
  status varchar(16) not null check (status in ('pending', 'running', 'succeeded', 'failed', 'timed_out', 'cancelled', 'discarded')),
  request_hash varchar(64) not null,
  input jsonb not null,
  output jsonb,
  provider_request_id varchar(128),
  error_code varchar(64),
  error_message text,
  latency_ms integer,
  started_at timestamptz,
  finished_at timestamptz,
  created_at timestamptz not null default now(),
  unique (task_id, tool_name, request_hash)
);
create index idx_tool_calls_task on tool_calls (task_id, created_at);

create table itineraries (
  id uuid primary key,
  session_id uuid not null references sessions(id),
  version integer not null,
  context_version integer not null,
  status itinerary_status not null default 'draft',
  content jsonb not null,
  budget_summary jsonb not null,
  source_task_ids uuid[] not null default '{}',
  created_at timestamptz not null default now(),
  confirmed_at timestamptz,
  unique (session_id, version)
);
create unique index uq_confirmed_itinerary_per_session on itineraries (session_id) where status = 'confirmed';

create table traces (
  id bigserial primary key,
  trace_id varchar(32) not null,
  span_id varchar(16),
  session_id uuid references sessions(id),
  turn_id uuid references turns(id),
  task_id uuid references tasks(id),
  span_name varchar(128) not null,
  status varchar(16) not null,
  started_at timestamptz not null,
  ended_at timestamptz,
  attributes jsonb not null default '{}'::jsonb
);
create index idx_traces_trace_id on traces (trace_id, started_at);

create table event_outbox (
  id bigserial primary key,
  session_id uuid not null references sessions(id),
  event_seq bigint not null,
  event_type varchar(64) not null,
  payload jsonb not null,
  dedupe_key varchar(160) not null unique,
  published_at timestamptz,
  created_at timestamptz not null default now(),
  unique (session_id, event_seq)
);
```

### 9.3 行程 JSONB 结构

`itineraries.content` 的最小结构如下，避免把行程展示逻辑拆散到多个难以原子更新的表中。需要复杂筛选或共享时，再从该结构投影到明细表。

```json
{
  "schema_version": 1,
  "destination": "上海",
  "date_range": { "start": "2026-08-10", "end": "2026-08-12" },
  "days": [
    {
      "date": "2026-08-10",
      "items": [
        {
          "slot": "afternoon",
          "type": "museum",
          "name": "上海博物馆东馆",
          "location": { "lat": 31.23, "lng": 121.55 },
          "estimated_cost_cny": 0,
          "source_tool_call_ids": ["uuid"]
        }
      ]
    }
  ],
  "constraints_applied": ["avoid_crowds", "more_museums"],
  "generated_from_context_version": 13
}
```

## 10. 推测态与权威态一致性

### 10.1 数据分类

| 数据 | 类型 | 保存位置 | 更新规则 | UI 展示规则 |
| --- | --- | --- | --- | --- |
| 实时部分转写 | 推测态 | 浏览器内存，可选 Redis TTL | 按 provider event revision 覆盖；最终转写到达后替换 | 标记为“正在识别”，不参与行程计算 |
| 临时口头回复 | 推测态 | 浏览器内存与可选短期事件缓存 | 受 `turn_id` 与 `playback_id` 约束；打断即失效 | 可显示字幕，打断后淡出 |
| 进行中的工具任务进度 | 推测态 | Redis / WebSocket 事件 | 不代表业务结果；终态以数据库为准 | 显示“查询中”或失败提示 |
| 最终转写 | 权威态 | PostgreSQL `transcripts` | 追加 revision；确认后触发 `context_version` 更新 | 重连后由快照恢复 |
| 最终偏好 | 权威态 | PostgreSQL `preferences` | 新版本原子创建，旧 active 标为 superseded | 始终显示最新 active 版本 |
| 工具结果 | 权威态 | PostgreSQL `tool_calls` 与 `tasks` | 仅通过版本和取消校验后可应用 | 迟到结果不进入当前推荐 |
| 最终行程 | 权威态 | PostgreSQL `itineraries` | 条件写入并版本化；每会话最多一个 confirmed | 始终以 confirmed 或最新有效 draft 展示 |

### 10.2 写入与恢复规则

1. **先持久化，后广播**：Worker 在同一数据库事务中写入工具结果、任务状态与 outbox 事件；事务成功后才由 outbox 发布 `task.succeeded`。因此前端看到的成功事件必然可通过快照读回。
2. **推测态可丢失，权威态可重建**：前端断线、刷新或模型连接重建时，丢弃本地 partial transcript、临时回复和旧播放状态；以 `GET /snapshot` 返回的权威数据为基线重建。
3. **版本条件提交**：任务的工具输出可以保留审计记录，但只有 `task.context_version == sessions.context_version`、任务未取消且目标偏好仍 active 时，才可生成或确认当前行程。
4. **事件序列去重**：网关为每个会话分配递增 `event_seq`。前端按序消费，重连时带 `after_event_seq`；重复事件依赖 `event_id` 去重。
5. **不以客户端状态为准**：浏览器的“正在播放”“查询完成”只能影响展示和控制，不能直接改变行程权威状态。

## 11. FastAPI 接口与实时事件协议

### 11.1 HTTP API

除 `POST /v1/internal/*` 外，所有接口需要 `Authorization: Bearer <access_token>`，并校验 `session.user_id == auth.user_id`。

| 方法与路径 | 用途 | 请求关键字段 | 响应关键字段 | 实现要点 |
| --- | --- | --- | --- | --- |
| `POST /v1/sessions` | 创建会话 | `locale`、`timezone`、可选初始 `preference` | `session_id`、`context_version`、`event_ws_url` | 事务创建 session 与 preference v1 |
| `POST /v1/sessions/{session_id}/realtime-token` | 获取实时模型短期连接令牌 | `device_id`、`connection_epoch` | `token`、`expires_at`、`provider_config` | 单次使用，最长 60 秒，不返回供应商主密钥 |
| `GET /v1/sessions/{session_id}/snapshot` | 读取权威会话快照 | 可选 `after_event_seq` | session、active preference、turns、tasks、itinerary、missed events | 用于刷新和恢复 |
| `PATCH /v1/sessions/{session_id}/preferences` | 更新旅行偏好 | `base_context_version`、`patch`、`client_event_id` | 新 `context_version`、`preference_version`、`cancelled_task_ids` | 乐观并发失败返回 `409` 与最新快照摘要 |
| `POST /v1/sessions/{session_id}/interrupt` | 登记用户打断 | `turn_id`、`playback_id`、`occurred_at`、`client_event_id` | `accepted`、`turn_status`、`event_seq` | 低延迟；写取消标记，不等待 Worker |
| `POST /v1/sessions/{session_id}/tasks` | 创建异步任务 | `turn_id`、`type`、`payload`、`idempotency_key` | `task_id`、`status`、`context_version` | 仅允许白名单 task 类型 |
| `GET /v1/sessions/{session_id}/tasks/{task_id}` | 查询任务状态 | 无 | status、attempt、result summary、error | 仅返回当前用户有权查看的数据 |
| `POST /v1/internal/tool-results` | Worker 回传工具处理结果 | `task_id`、`attempt`、`tool_calls`、`trace_id` | `applied`、`discarded`、`next_action` | mTLS 或服务 JWT；支持幂等重放 |
| `POST /v1/sessions/{session_id}/resume` | 断线恢复 | `after_event_seq`、`previous_connection_epoch`、`device_id` | snapshot、missed events、新实时令牌 | 原 connection epoch 失效，新 epoch 加一 |

#### 关键请求示例

```http
PATCH /v1/sessions/3e2.../preferences
Idempotency-Key: cli-01J...
Authorization: Bearer <user-token>
Content-Type: application/json

{
  "base_context_version": 12,
  "client_event_id": "01JABC...",
  "patch": {
    "crowd_tolerance": "low",
    "interests_add": ["museum"],
    "interests_remove": ["nightlife_crowded"]
  },
  "source_turn_id": "f6e..."
}
```

```json
{
  "session_id": "3e2...",
  "context_version": 13,
  "preference_version": 4,
  "cancelled_task_ids": ["0b1...", "ab8..."],
  "event_seq": 87
}
```

### 11.2 会话 WebSocket 控制协议

浏览器与网关使用 `wss://.../v1/sessions/{session_id}/events` 交换应用控制事件。此连接不传输音频；音频始终通过浏览器与实时模型的 WebRTC 连接传输。

通用信封：

```json
{
  "event_id": "01J...",
  "event_seq": 87,
  "type": "agent.interrupt",
  "session_id": "3e2...",
  "turn_id": "f6e...",
  "context_version": 13,
  "occurred_at": "2026-08-04T08:30:01.120Z",
  "traceparent": "00-<trace-id>-<span-id>-01",
  "payload": {}
}
```

| 方向 | 事件类型 | `payload` 关键字段 | 处理语义 |
| --- | --- | --- | --- |
| Client -> Gateway | `turn.finalized` | `transcript_ref`、`language`、`client_event_id` | 固化最终用户输入并触发编排 |
| Client -> Gateway | `preference.update` | `base_context_version`、`patch` | 与 HTTP PATCH 同语义，适合在线会话 |
| Client -> Gateway | `agent.interrupt` | `playback_id`、`reason` | 低延迟取消控制；客户端先本地停播 |
| Client -> Gateway | `playback.started` / `playback.stopped` | `playback_id`、`reason`、时间戳 | 只用于状态与指标，不作权威回复依据 |
| Client -> Gateway | `session.resume` | `after_event_seq`、`connection_epoch` | 请求补发事件和快照 |
| Gateway -> Client | `session.snapshot` | 权威 session、偏好、任务、行程 | 重连和首次加载的基线 |
| Gateway -> Client | `task.queued` / `task.running` / `task.succeeded` / `task.failed` | `task_id`、`type`、`summary` | 显示异步任务状态 |
| Gateway -> Client | `task.result.discarded` | `task_id`、`discard_reason` | 告知结果不影响最新规划 |
| Gateway -> Client | `preference.updated` | `context_version`、`preference` | 客户端替换本地偏好 |
| Gateway -> Client | `agent.reply.available` | `turn_id`、`context_packet_id` | 前端向实时模型发送回复上下文或工具结果摘要 |
| Gateway -> Client | `agent.interrupt.accepted` | `turn_id`、`event_seq` | 确认服务端取消登记 |

### 11.3 实时模型事件适配

供应商事件不能直接扩散到全站领域逻辑。前端适配器应转换为以下内部事件：

| 模型事件能力 | 内部事件 | 关键动作 |
| --- | --- | --- |
| 部分转写 | `realtime.transcript.partial` | 仅更新推测态字幕 |
| 最终转写 | `realtime.transcript.final` | 发送 `turn.finalized` 给网关 |
| 回复开始 | `realtime.response.started` | 记录 playback/turn 关联 |
| 下行音频首包 | `realtime.audio.first_packet` | 上报首包延迟 |
| 回复完成 | `realtime.response.completed` | 结束临时回复状态 |
| 取消完成 | `realtime.response.cancelled` | 与本地已停播状态对齐 |
| 连接状态变化 | `realtime.connection.changed` | 触发重连或恢复流程 |

## 12. 工具契约设计

所有工具均实现同一异步接口：`async execute(request: ToolRequest, cancellation: CancellationToken) -> ToolResult`。请求中包含 `task_id`、`session_id`、`context_version`、`deadline_at`、`traceparent`；任何工具不得访问其他用户的 session 数据。

| 工具 | 输入 | 输出 | 错误处理 |
| --- | --- | --- | --- |
| 天气查询 `weather.get` | `city`、`date_range`、`timezone`、可选 `lat/lng` | 每日天气、温度区间、降水概率、预警、数据时间 | `CITY_NOT_FOUND` 不重试；`RATE_LIMITED` 按 `retry_after`；`UPSTREAM_TIMEOUT` 最多重试 2 次 |
| 景点搜索 `attraction.search` | `destination`、`date_range`、`interests`、`crowd_tolerance`、`opening_hours_required`、坐标范围 | 景点列表、类别、开放时间、预计停留、拥挤度、票价、来源时间 | 缺少位置返回 `INVALID_ARGUMENT`；上游空结果为成功空数组；超时降级为提示需确认 |
| 餐厅搜索 `restaurant.search` | `location`、`meal_slot`、`budget_per_person`、`dietary`、`party_size`、`crowd_tolerance` | 候选餐厅、菜系、价格、营业时间、距离、预约信息 | 不可靠的实时座位信息标记 `freshness`；限流重试；不把推荐当作预订成功 |
| 路线规划 `route.plan` | 起点、终点、出发时间、交通偏好、步行上限、人数 | 路线段、时长、距离、换乘、预估费用、交通异常 | `NO_ROUTE` 返回可解释错误；超时可使用直线距离粗估且标记 `estimated=true` |
| 预算计算 `budget.calculate` | 总预算、人数、天数、行程条目、已知价格、预留比例 | 分类预算、总额、区间、超预算原因、可削减项 | 输入不全返回带缺失字段的 `PARTIAL_INPUT`，仍输出已知范围；货币必须显式指定 |
| 行程保存 `itinerary.save` | `session_id`、`expected_context_version`、`expected_itinerary_version`、行程 JSON、来源 task IDs | `itinerary_id`、新 version、状态、确认时间 | 条件写入失败返回 `VERSION_CONFLICT`，调用方重新编排；JSON schema 不合法返回 `VALIDATION_ERROR` |

### 12.1 标准工具请求与响应

```json
{
  "tool_call_id": "e9c...",
  "task_id": "0b1...",
  "session_id": "3e2...",
  "context_version": 13,
  "deadline_at": "2026-08-04T08:31:30Z",
  "traceparent": "00-<trace-id>-<span-id>-01",
  "input": {
    "city": "上海",
    "date_range": { "start": "2026-08-10", "end": "2026-08-12" }
  }
}
```

```json
{
  "ok": true,
  "data": { "days": [] },
  "freshness": { "observed_at": "2026-08-04T08:30:12Z", "ttl_seconds": 1800 },
  "provider_request_id": "vendor-..."
}
```

失败响应统一为：

```json
{
  "ok": false,
  "error": {
    "code": "UPSTREAM_TIMEOUT",
    "message": "天气服务在 5 秒内未响应",
    "retryable": true,
    "retry_after_ms": 1000
  }
}
```

工具适配层必须进行输入 schema 校验、输出 schema 校验、上游超时、敏感字段剥离和错误映射。错误消息给 Agent 的版本应简短可恢复，完整原始错误只进入受控日志。

## 13. 后台任务处理机制

### 13.1 任务创建与排队

1. 网关根据 `AgentDecision` 在 PostgreSQL 创建 `tasks` 和预期 `tool_calls`，计算幂等键：`sha256(session_id + context_version + task_type + canonical_payload)`。
2. 同一事务写入 `event_outbox`。若 `(session_id, idempotency_key)` 已存在，返回已有 task，避免双击、重连重放和模型重复事件造成重复查询。
3. 提交后 outbox 发布器向 `travel.tasks` 执行 `XADD`。消息包含 task ID、attempt、deadline、context version 和 trace context；消息不携带访问令牌或用户敏感原文。
4. Worker 使用 Consumer Group `travel-workers` 的 `XREADGROUP` 拉取任务。消费前用条件更新把任务从 `queued` 置为 `running`，避免重复消费者执行。

消息结构：

```json
{
  "task_id": "0b1...",
  "attempt": 1,
  "session_id": "3e2...",
  "turn_id": "f6e...",
  "context_version": 13,
  "deadline_at": "2026-08-04T08:31:30Z",
  "traceparent": "00-<trace-id>-<span-id>-01"
}
```

### 13.2 执行、超时、取消和重试

| 环节 | 机制 | 具体规则 |
| --- | --- | --- |
| 执行 | asyncio 并发加信号量 | 每类工具独立并发阈值，避免地图或搜索供应商挤占所有 Worker |
| 超时 | `asyncio.timeout` + task deadline | 单工具默认 5 秒，组合任务默认 20 秒；到 deadline 立即结束并标记 `timed_out` |
| 取消 | Redis 取消键 + 数据库状态 | 每次工具调用前后检查 `cancel:task:{id}`；能取消的 HTTP 请求主动取消，不能取消的结果走迟到丢弃 |
| 重试 | 指数退避加抖动 | 仅对网络错误、`429`、`5xx` 重试，最多 3 次；`4xx` 业务错误不重试 |
| Worker 崩溃 | Streams pending claim | 定时回收超时未 ACK 的 PEL 消息；重新执行前读取数据库状态和幂等键 |
| ACK | 提交后确认 | 只有任务状态和 outbox 在 PostgreSQL 成功提交后执行 `XACK` |

### 13.3 迟到结果丢弃与结果回传

Worker 收到工具结果后，按以下顺序处理：

1. 校验 `task.attempt`、任务状态和请求哈希，拒绝旧 attempt 或重复回传。
2. 读取当前 `sessions.context_version` 与 active preference；若与 task 固定版本不一致，记录工具结果审计信息，将 task/tool call 标为 `discarded`，不触发行程生成。
3. 若版本有效，在单个事务内保存结构化结果、更新 task/tool call 状态、创建 outbox 事件。
4. outbox 发布器发送 `task.succeeded` 或 `task.result.discarded`；成功结果附带摘要而不是全部第三方原始响应。
5. 编排器仅订阅有效的 `ToolResultReady`，重新读取权威上下文后决定继续回复或创建下一批任务。

## 14. 上下文管理策略

### 14.1 上下文包组成

实时模型和 Agent 使用受大小约束的 `ContextPacket`，而不是每次拼接完整历史：

| 层次 | 内容 | 来源 | 更新频率 |
| --- | --- | --- | --- |
| 最近对话窗口 | 最近 6 个已完成 turn 的最终转写与 Agent 摘要 | `turns`、`transcripts` | 每个最终 turn |
| 用户偏好 | 目的地、日期、预算、同行人员、兴趣、拥挤度、交通/饮食限制 | active `preference` | 每次偏好更新立即替换 |
| 行程摘要 | 当前 confirmed/draft 行程、未决冲突、预算余量 | `itineraries` | 行程版本变化 |
| 工具结果摘要 | 结果来源、时效、关键候选、失败和不确定性 | `tool_calls` | 有效任务完成后 |
| 系统约束 | 不虚构实时信息、先确认缺失条件、过期结果不可使用 | 代码常量/版本化模板 | 部署或策略变更 |

### 14.2 压缩与重建

1. 当最近对话窗口超过 token 或字符预算时，Worker 异步生成会话摘要，保存为带 `kind=system` 的摘要 turn。摘要必须包含已确认事实、未确认假设、当前偏好版本、行程版本、未完成任务及其 deadline。
2. 压缩任务使用输入快照版本；若完成时 `context_version` 已变化，摘要不覆盖最新摘要，而是作为候选重新计算。
3. 不把部分转写、被打断回复和 `discarded` 工具结果注入后续上下文。
4. 重连后网关从 PostgreSQL 构造 `ContextPacket`：当前 active preference、最新权威行程、最后 N 个最终 turn、有效工具结果摘要和可恢复任务。客户端丢弃所有未确认的本地模型上下文。
5. `ContextPacket` 带 `context_version`、`preference_version`、`itinerary_version` 和 `packet_id`。前端向实时模型发送更新时只接受不小于当前版本的包。

## 15. 异常处理方案

| 异常 | 检测与即时处理 | 恢复与一致性措施 | 用户可见行为 |
| --- | --- | --- | --- |
| 实时模型连接失败 | SDP/ICE 或供应商鉴权失败；前端停止建连计时 | 刷新一次短期令牌并限次重连；令牌错误记录审计 | 显示语音连接失败，可重试，不丢失已保存会话 |
| WebRTC 连接中断 | `connectionState=failed/disconnected` | 关闭旧 PeerConnection，按退避获取新令牌与快照；递增 connection epoch | 停止当前播放，恢复后显示最新行程与任务 |
| 工具超时 | 工具 deadline 或 `asyncio.TimeoutError` | 标记 `timed_out`，按错误策略重试；可返回降级结果 | 明确说明部分信息未能及时获取 |
| Worker 崩溃 | PEL 长时间未 ACK、健康检查失败 | `XAUTOCLAIM` 重新分配；数据库条件状态避免重复提交 | 任务继续处理中或稍后重试 |
| Redis 不可用 | 网关/Worker ping 与操作失败 | 网关将入队意图写 outbox；恢复后补投；取消以 PostgreSQL 状态为最终依据 | 实时对话仍可继续，外部查询显示暂缓 |
| PostgreSQL 写入失败 | 事务异常、连接池耗尽 | 不广播成功；短暂重试；超阈值熔断新任务并报警 | 提示状态暂未保存，不宣称结果已生效 |
| 重复任务 | 相同 `idempotency_key` 或 Streams 重投 | 返回已存在 task；Worker 用条件状态更新与 request hash 去重 | 不重复显示、不重复收费 |
| 旧任务结果覆盖新需求 | context/preference/version 条件检查失败 | 标记 `discarded`，保留审计，不触发行程更新 | 只展示基于新偏好的方案 |

所有异常响应使用稳定错误码、`trace_id` 和面向用户的简短说明。告警消息包含错误码、服务、依赖、会话匿名标识与最近状态，不包含完整语音文本、访问令牌或第三方密钥。

## 16. 安全设计

| 范畴 | 设计 |
| --- | --- |
| 用户鉴权 | 使用 OIDC/OAuth2 登录后签发短期 JWT；网关从 JWT 获取 `user_id`，不信任客户端传入的用户标识 |
| 短期实时连接令牌 | 服务端使用供应商主密钥换取只绑定 `session_id`、`user_id`、`connection_epoch` 的一次性令牌；TTL 不超过 60 秒，绝不下发主密钥 |
| 会话隔离 | 每个 REST、WS、resume 和 task 查询先执行所有权校验；数据库查询必须带 `session_id` 与 `user_id` 约束；内部 Worker 使用 task ID 读取，避免客户端可控跨会话 ID |
| 工具权限控制 | 工具注册表采用白名单，按任务类型允许的工具集合执行；禁止 Agent 任意 URL 请求、任意 SQL、文件系统或 shell 调用 |
| API 密钥保护 | `.env` 仅开发使用；生产密钥放入 Secret Manager/容器 secrets；日志和异常不输出 `Authorization`、令牌、供应商 API key |
| 日志脱敏 | 转写、位置、订单或联系方式默认不写日志正文；使用哈希会话 ID、字段掩码和最小必要采样；调试原文需受权限与保留期控制 |
| 速率限制 | 按用户、IP、session 分别限制会话创建、令牌签发、偏好更新、打断事件和任务创建；对 WebSocket 消息限制大小、速率与 schema |
| 输入验证 | FastAPI Pydantic schema 校验、JSONB schema 校验、日期/预算/坐标范围校验；拒绝超大 payload 和未定义事件 |
| 传输与存储 | HTTPS/WSS、WebRTC SRTP、数据库和 Redis 启用 TLS；敏感配置加密；会话数据按产品策略设置保留期和删除机制 |

## 17. 可观测性设计

### 17.1 Trace ID 传播

1. 浏览器为创建会话、打断、偏好更新、WebRTC 建连和播放事件创建或延续 W3C `traceparent`。
2. FastAPI 从 HTTP/WS 载荷提取上下文，为数据库事务、Redis `XADD` 和实时令牌调用创建子 span。
3. Redis task 消息显式携带 `traceparent`，Worker 消费后继续同一 trace，并向工具请求头注入该上下文。
4. `trace_id` 同时写入 `tasks`、`tool_calls` 和结构化日志；`traces` 表只保留关键索引，完整 span 由 OTel Collector 输出到追踪后端。

### 17.2 结构化日志字段

每条日志至少包含：`timestamp`、`level`、`service`、`environment`、`trace_id`、`span_id`、`session_id_hash`、`turn_id`、`task_id`、`event_type`、`context_version`、`latency_ms`、`error_code`。转写正文、令牌、完整坐标和供应商原始响应禁止默认写入。

### 17.3 指标与计算口径

| 指标 | Prometheus 指标名 | 起止点 | 标签 |
| --- | --- | --- | --- |
| 音频首包延迟 | `livepilot_realtime_audio_first_packet_seconds` | 用户 utterance final 到收到首个远端音频 RTP/track 数据 | provider、network_type、locale |
| 首段语音响应延迟 | `livepilot_speech_first_playout_seconds` | 用户最终语音结束到浏览器实际开始播放 | provider、scenario、interrupted |
| 用户打断生效时间 | `livepilot_interrupt_effective_seconds` | 客户端触发 interrupt 到本地音频停止 | trigger、connection_state |
| 模型取消确认延迟 | `livepilot_model_cancel_ack_seconds` | 发送 `response.cancel` 到模型确认 | provider |
| 工具调用耗时 | `livepilot_tool_call_seconds` | Worker 发起工具到收到响应 | tool_name、result、attempt |
| 异步任务完成时间 | `livepilot_task_completion_seconds` | task 创建到终态 | task_type、status |
| 队列等待时间 | `livepilot_task_queue_wait_seconds` | task 创建到 Worker 开始执行 | task_type、priority |
| 重连成功率 | `livepilot_reconnect_total` | reconnect 终态计数 | result、reason |
| 错误率 | `livepilot_errors_total` | 错误计数 | service、error_code、retryable |
| 会话稳定性 | `livepilot_session_duration_seconds` | session active 到 close | close_reason |

所有耗时指标使用 Histogram，配置适合语音体验的 buckets：`0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 3, 5, 10, 20, 60` 秒。Grafana 面板必须按整体、供应商、网络类型、任务类型和版本丢弃原因展示 p50/p95，并关联 trace 样本。

### 17.4 告警建议

| 告警 | 条件 | 处理方向 |
| --- | --- | --- |
| 首段语音退化 | 15 分钟 p95 超过 2 秒 | 区分实时模型、浏览器网络与控制面延迟 |
| 打断失效 | 15 分钟 p95 超过 500ms 或失败率超过 1% | 检查前端播放器和数据通道取消事件 |
| 工具依赖异常 | 某工具 5 分钟错误率超过 10% | 启用降级或熔断，检查供应商状态 |
| Worker 积压 | Stream pending 或队列等待 p95 超阈值 | 扩容 Worker 或降级低优先级任务 |
| 数据库失败 | 5 分钟写事务错误率超过 1% | 阻止结果广播，检查连接池与数据库健康 |
| 旧结果异常 | `discarded` 比率异常上升 | 排查频繁偏好变更、任务延迟或版本实现错误 |

## 18. 测试方案

| 测试类型 | 覆盖内容 | 方法与关键断言 |
| --- | --- | --- |
| 单元测试 | Pydantic schema、幂等键、版本比较、取消判断、错误映射、上下文裁剪 | pytest；固定时钟和伪 UUID；验证同输入幂等、旧版本不可提交 |
| API 测试 | 鉴权、会话创建、令牌、偏好更新、打断、恢复、任务查询 | FastAPI `TestClient`/`httpx.AsyncClient`；覆盖 200、401、403、409、422、429 |
| Agent 状态机测试 | turn/session/task 转移与非法转移 | 参数化状态迁移；验证 interrupted/superseded 后不能回到 speaking |
| 工具失败测试 | 429、5xx、超时、空结果、无路线、无效输入 | `respx`/mock server；验证重试次数、退避、降级和错误码 |
| 用户打断测试 | 播放停止、模型取消、服务端取消标记和新 turn | Playwright + mocked WebRTC/model adapter；断言本地停止先于网络响应，旧音频不再播放 |
| 断网重连测试 | WebRTC 断开、WS 断开、事件补发、令牌刷新 | Playwright 网络模拟；断言快照覆盖推测态、事件按 seq 去重 |
| 长会话测试 | 50+ turn、上下文压缩、多次偏好修改、多个任务并发 | 集成测试；断言上下文大小受限且最终行程与 active preference 一致 |
| 并发测试 | 多用户、多 session、任务重投、同幂等键并发提交 | k6/Locust；验证无跨会话泄露、无重复 task、队列延迟可控 |
| 端到端语音演示测试 | 真实浏览器麦克风、实时模型、工具 sandbox、行程展示 | 录制合规测试音频或人工脚本；采集 trace 和 p50/p95，验证语音连续性 |

### 18.1 必测场景

1. 用户连续描述“上海三天、预算五千、夜间活动”，在模型首句后插话，验证 250ms 内本地停止旧音频。
2. 天气与景点任务并发运行期间，用户继续提问，验证音频上行与下行未等待 Worker。
3. 用户更新为“避开人多、多安排博物馆”后，让旧任务延迟返回，验证 task 为 `discarded` 且 confirmed itinerary 未变化。
4. 在工具调用中断 Redis 后恢复，验证 outbox 补投且同一幂等键只形成一个权威 task。
5. Worker 在数据库提交前崩溃，验证 pending 消息可重领，最终只有一次工具结果和一次会话事件。
6. 断网恢复后，验证前端不显示旧 partial transcript 或被打断的临时回复，且可重新开始语音。

## 19. P0 与 P1 功能

| 优先级 | 功能 | 用户价值 | 技术实现 | 验收条件 |
| --- | --- | --- | --- | --- |
| P0 | 创建会话与实时 WebRTC 语音 | 可开始自然语音对话 | FastAPI 临时令牌、浏览器 `getUserMedia`、`RTCPeerConnection`、实时模型适配器 | 用户授权后可收发音频，音频不经过 FastAPI |
| P0 | 用户打断与停止播放 | 不必等待 Agent 说完 | 本地播放器同步停止 + 数据通道取消 + 网关 `interrupt` | 95% 打断在 250ms 内停止本地播放 |
| P0 | 偏好版本管理 | 修改需求立即生效 | `preferences` 版本表、`context_version` 原子递增、409 并发处理 | 最新 active preference 唯一且可重连恢复 |
| P0 | 异步天气与景点查询 | 查询不阻塞语音 | PostgreSQL task + Redis Streams + asyncio Worker | 查询中仍可语音交互；结果可回传 |
| P0 | 旧任务结果丢弃 | 避免基于旧要求给建议 | task 固定 context version，条件写入和 `discarded` 状态 | 迟到旧结果不得修改当前行程 |
| P0 | 会话快照与恢复 | 断线后不中断规划上下文 | snapshot API、event sequence、刷新实时令牌 | 刷新页面或 WebRTC 重连后恢复权威状态 |
| P0 | 基础可观测性 | 能诊断延迟与失败 | OTel trace、Prometheus histograms、结构化日志 | 可查询单 session 的 trace 与关键时延 |
| P1 | 餐厅与路线并行规划 | 行程更完整、更可执行 | 工具适配器、组合任务、路线和预算摘要 | 行程条目带路线、价格或不确定性标记 |
| P1 | 预算计算与冲突检测 | 发现超预算与时间冲突 | 预算工具、行程验证 Worker | 明确输出超预算类别和冲突条目 |
| P1 | 行程确认与版本展示 | 用户可比较并确认方案 | `itineraries` 版本、乐观锁、确认事件 | 每 session 只有一个 confirmed itinerary |
| P1 | 上下文压缩 | 长会话仍保持相关性 | 后台摘要任务、ContextPacket 版本化 | 50 turn 后仍能保留关键偏好和行程事实 |
| P1 | 依赖降级与告警 | 外部服务不稳定时体验可控 | 工具熔断、缓存 freshness、Grafana 告警 | 单工具故障不使会话或音频路径不可用 |

## 20. 部署与本地演示配置

Docker Compose 至少编排以下服务：

| 服务 | 容器职责 | 对外端口/依赖 |
| --- | --- | --- |
| `web` | Vite 构建后的 React 静态站点 | `443` 或开发期 `5173`；依赖 `api`、实时模型 API |
| `api` | FastAPI 会话网关与 WebSocket | `8000`；依赖 `postgres`、`redis`、OTel Collector |
| `worker` | Redis Streams 消费与工具执行 | 无需公开端口；依赖 `postgres`、`redis`、工具服务 |
| `postgres` | 权威持久化 | `5432`，开发环境可映射 |
| `redis` | Streams、短期取消、限流和事件缓存 | `6379`，开发环境可映射 |
| `otel-collector` | 接收 OTLP 并导出 traces/metrics | `4317/4318` |
| `prometheus` | 抓取服务 metrics | `9090` |
| `grafana` | 延迟、错误、任务与重连仪表盘 | `3000` |

环境变量按服务分组：

```dotenv
DATABASE_URL=postgresql+asyncpg://livepilot:***@postgres:5432/livepilot
REDIS_URL=redis://redis:6379/0
REALTIME_PROVIDER_BASE_URL=https://provider.example
REALTIME_PROVIDER_API_KEY=***
WEATHER_API_KEY=***
MAP_API_KEY=***
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
JWT_ISSUER=https://issuer.example
JWT_AUDIENCE=livepilot-web
```

生产部署不得在镜像、前端 bundle、日志或接口响应中包含 `REALTIME_PROVIDER_API_KEY`、工具密钥或数据库密码。前端仅接收网关签发的短期实时令牌。

## 21. 完整验收标准

以下标准在标准演示环境中验收：Chrome 最新稳定版、稳定网络、同一实时模型区域、工具服务 mock 或已知稳定 sandbox，连续执行不少于 30 次。所有延迟从前端单调时钟与 OpenTelemetry trace 交叉校验；同时给出 p50 与 p95。

| 类别 | 验收标准 |
| --- | --- |
| 普通问题首段语音响应延迟 | 从用户最终语音结束到浏览器开始播放模型首段语音，p50 不高于 800ms，p95 不高于 1.5s；每条样本具备 trace 关联。 |
| 音频首包延迟 | 从最终语音结束到接收首个有效远端音频数据，p95 不高于 1.2s。 |
| 用户打断后的停止播放时间 | 从用户插话/VAD/点击停止到本地播放器停止，p50 不高于 100ms，p95 不高于 250ms；不依赖网关确认。 |
| 模型取消 | 打断后模型取消确认 p95 不高于 1s；即使确认迟到，旧远端音频也不得重新播放。 |
| 异步任务不阻塞音频流 | 天气、景点、路线任务运行期间，可连续完成至少 10 轮语音交互；无因等待任务造成的音频播放暂停或上行采集停止。 |
| 任务失败重试 | 对可重试的网络超时、429、5xx，执行至多 3 次指数退避；不可重试 4xx 不重试；最终状态与每次 attempt 均可查询。 |
| 取消与迟到结果 | 偏好更新后旧版本任务被取消或标记过期；即使外部调用成功返回，task 终态为 `discarded`，不会创建或覆盖当前 confirmed itinerary。 |
| 断线恢复 | 在 WebRTC/网页连接中断后，用户可在 30 分钟内使用新令牌恢复；快照包含 active preference、最终转写、未完成任务和最新行程；重复事件不导致重复展示。 |
| 最终行程数据一致性 | 每会话最多一个 `confirmed` itinerary；其 `context_version` 与确认时 active preference 版本一致，来源 task IDs 可追溯。 |
| 连续会话稳定性 | 单会话连续 50 个 turn、至少 5 次偏好修改、至少 20 个异步 task 后，状态机无非法迁移，内存与上下文包不随完整历史线性失控增长。 |
| 访问控制 | 未登录返回 401；非所属用户访问任意 session、task、resume 或 WebSocket 返回 403；实时供应商主密钥不出现在浏览器、日志和响应中。 |
| 可观测性 | 任一演示会话均可通过 `trace_id` 关联浏览器事件、网关请求、Redis 任务、Worker、工具调用和数据库写入；Grafana 展示首段响应、打断、工具、任务、重连和错误的 p50/p95。 |
| 幂等性 | 相同 `client_event_id` 或 `idempotency_key` 重放不会创建第二个偏好版本、打断事件或权威 task。 |
| 异常降级 | 任一工具超时或失败不会断开 WebRTC 音频会话；系统以明确状态提示部分信息不可用，并保留可恢复任务记录。 |

## 22. 演示脚本

1. 创建会话并说：“我准备下周去上海玩三天，预算五千，想安排一些适合晚上的活动。”
2. 验证 Agent 先通过语音确认，并在任务面板显示天气、景点、餐厅、路线、预算和冲突检测任务进入 `queued/running`。
3. 在 Agent 仍输出语音时说：“等等，我不想去人太多的地方，最好多安排一些博物馆。”
4. 验证旧音频在 250ms 内停止，偏好版本增加，旧夜游/热门景点任务被取消或过期，新一批博物馆导向任务入队。
5. 人为让旧任务晚于新任务返回，验证其状态为 `discarded`，当前行程不出现旧偏好推荐。
6. 模拟网络断开，重新建立 WebRTC，验证行程、偏好、任务和最终转写从快照恢复。
7. 在 Grafana 打开该会话的 trace，展示音频首包、首段语音、打断、工具耗时、任务完成和重连指标的 p50/p95。
