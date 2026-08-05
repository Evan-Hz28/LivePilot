建议将项目分为 10 个开发阶段。下面的阶段是“功能开发路线”，不要和先前环境文档的阶段编号混淆。

当前状态：阶段 0 已完成；阶段 1、2 已完成最小验证。下一步应进入阶段 3。

| 阶段 | 完成内容 | 验证方式 | 当前状态 |
| --- | --- | --- | --- |
| 0. 开发环境 | WSL、Docker、Node、uv、Python 3.12、PostgreSQL、Redis | `docker compose ps` 健康；前端启动；`uv run python --version` | 已完成 |
| 1. 后端基础 | FastAPI、SQLAlchemy、Alembic、`sessions`/`tasks` 表 | `/health` 返回 200；`alembic current` 为 head；`\dt` 有表 | 已完成最小版本 |
| 2. 异步任务链路 | API 创建任务、Redis Streams、Worker、结果写回 | `smoke_test` 从 `queued` 变为 `succeeded` | 已完成 |
| 3. 会话与状态模型 | `turns`、`preferences`、`context_version`、任务查询、WebSocket 事件 | 创建会话；更新偏好后版本递增；错误版本返回 409 | 下一步 |
| 4. Agent 编排 | `agent.plan`、`agent.compose`、上下文包、固定 Mock 工具决策 | 一条文本需求能产生工具任务，并生成结构化回复上下文 | 待开发 |
| 5. 前端会话界面 | 会话页、任务面板、行程展示、REST/WebSocket、CORS | 浏览器创建会话；任务状态实时更新；前端无长期密钥 | 待开发 |
| 6. 实时语音 | 后端短期令牌、前端 `getUserMedia`、WebRTC、音频播放 | 浏览器获得麦克风权限；能播放远端音频；长期 Key 不在前端 | 待开发 |
| 7. 打断与恢复 | 本地停播、模型取消、旧任务 `discarded`、断线快照恢复 | 插话后旧音频立即停止；旧任务不覆盖新偏好；刷新后恢复会话 | 待开发 |
| 8. 真实旅行工具与行程 | 天气、POI、餐厅、路线、预算、行程版本 | 真实或 Mock 工具结果写入；行程满足预算和偏好 | 待开发 |
| 9. 可观测性与安全 | OTel、Prometheus、Grafana、认证、密钥脱敏、限流 | 单个 `trace_id` 串联 API/Worker/工具；Grafana 有延迟与错误指标 | 待开发 |
| 10. 端到端验收与部署 | 完整 Compose、自动化测试、演示脚本、失败降级 | 语音旅行规划、打断、旧结果丢弃、断线恢复均通过 | 待开发 |

阶段 3 是当前最合理的下一步：先让后端具备真正的会话、偏好和版本管理；Agent、WebRTC 和真实工具都依赖这层权威状态。

完整目标架构见 [LivePilotInstruction.md:1](/home/zonghong/project/LivePilot/docs/LivePilotInstruction.md#L1)，各阶段流程图见 [system-flows.md:1](/home/zonghong/project/LivePilot/docs/system-flows.md#L1)。