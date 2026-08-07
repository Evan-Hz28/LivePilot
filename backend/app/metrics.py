from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Histogram

registry = CollectorRegistry()

task_completion_seconds = Histogram(
    "livepilot_task_completion_seconds",
    "Task completion time",
    labelnames=("task_type", "status"),
    registry=registry,
)
task_queue_wait_seconds = Histogram(
    "livepilot_task_queue_wait_seconds",
    "Task queue wait time",
    labelnames=("task_type", "status"),
    registry=registry,
)
tool_call_seconds = Histogram(
    "livepilot_tool_call_seconds",
    "Tool call time",
    labelnames=("tool_name", "result", "attempt"),
    registry=registry,
)
interrupt_effective_seconds = Histogram(
    "livepilot_interrupt_effective_seconds",
    "Interrupt effectiveness time",
    registry=registry,
)
model_cancel_ack_seconds = Histogram(
    "livepilot_model_cancel_ack_seconds",
    "Model cancel acknowledgement time",
    registry=registry,
)
tasks_discarded_total = Counter(
    "livepilot_tasks_discarded_total", "Discarded tasks", registry=registry
)
resume_conflicts_total = Counter(
    "livepilot_resume_conflicts_total", "Resume conflicts", registry=registry
)
outbox_publish_failures_total = Counter(
    "livepilot_outbox_publish_failures_total", "Outbox publish failures", registry=registry
)
errors_total = Counter(
    "livepilot_errors_total", "Controlled errors", labelnames=("code",), registry=registry
)
