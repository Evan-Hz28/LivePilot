import asyncio
import hashlib
import json
import logging
import socket
import time
from datetime import datetime, timezone
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import select

from app.cancellation import has_task_cancel_key
from app.config import settings
from app.db import async_session_factory
from app.models import EventOutbox, Preference, Task, ToolCall, TravelSession
from app.tools import mock_travel_adapter
from app.metrics import errors_total, task_completion_seconds, task_queue_wait_seconds, tasks_discarded_total, tool_call_seconds
from app.observability import extract_trace_context, trace_scope
TASK_STREAM = "travel.tasks"
GROUP_NAME = "livepilot-workers"
CONSUMER_NAME = f"{socket.gethostname()}-worker"

logger = logging.getLogger(__name__)


async def ensure_consumer_group(redis: Redis) -> None:
    try:
        await redis.xgroup_create(
            TASK_STREAM,
            GROUP_NAME,
            id="0-0",
            mkstream=True,
        )
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):
            raise


async def _append_task_event(
    database_session,
    *,
    session_id: UUID,
    event_type: str,
    payload: dict,
    dedupe_key: str,
) -> None:
    session = await database_session.scalar(
        select(TravelSession)
        .where(TravelSession.id == session_id)
        .with_for_update()
    )
    if session is None:
        return
    existing = await database_session.scalar(
        select(EventOutbox).where(EventOutbox.dedupe_key == dedupe_key)
    )
    if existing is not None:
        return
    session.last_event_seq += 1
    database_session.add(
        EventOutbox(
            session_id=session_id,
            event_seq=session.last_event_seq,
            event_type=event_type,
            payload=payload,
            dedupe_key=dedupe_key,
        )
    )


async def _lock_task_and_session(database_session, task_id: UUID):
    task_reference = await database_session.get(Task, task_id)
    if task_reference is None:
        return None, None
    session = await database_session.scalar(
        select(TravelSession)
        .where(TravelSession.id == task_reference.session_id)
        .with_for_update()
    )
    if session is None:
        return None, None
    task = await database_session.scalar(
        select(Task).where(Task.id == task_id).with_for_update()
    )
    return session, task


async def _task_context_is_current(database_session, task: Task) -> bool:
    if task.context_version is None:
        return True
    session = await database_session.get(TravelSession, task.session_id)
    if session is None or session.context_version != task.context_version:
        return False
    if task.target_preference_version is None:
        return True
    preference = await database_session.scalar(
        select(Preference).where(
            Preference.session_id == task.session_id,
            Preference.status == "active",
            Preference.version == task.target_preference_version,
        )
    )
    return preference is not None


async def _task_cancellation_requested(task_id: UUID, redis: Redis | None) -> bool:
    if await has_task_cancel_key(redis, task_id):
        return True
    async with async_session_factory() as database_session:
        task = await database_session.get(Task, task_id)
        return task is not None and task.status == "cancel_requested"


async def _mark_task_cancelled(task_id: UUID) -> bool:
    async with async_session_factory() as database_session:
        async with database_session.begin():
            _, task = await _lock_task_and_session(database_session, task_id)
            if task is None or task.status != "cancel_requested":
                return False
            now = datetime.now(timezone.utc)
            task.status = "cancelled"
            task.finished_at = now
            tool_calls = list(
                (
                    await database_session.scalars(
                        select(ToolCall)
                        .where(
                            ToolCall.task_id == task.id,
                            ToolCall.status.in_(("pending", "running")),
                        )
                        .with_for_update()
                    )
                ).all()
            )
            for tool_call in tool_calls:
                tool_call.status = "cancelled"
                tool_call.finished_at = now
            await _append_task_event(
                database_session,
                session_id=task.session_id,
                event_type="task.cancelled",
                payload={
                    "task_id": str(task.id),
                    "turn_id": str(task.turn_id) if task.turn_id else None,
                    "context_version": task.context_version,
                },
                dedupe_key=f"task.cancelled:{task.id}",
            )
            return True


def _tool_request_hash(tool_input: dict) -> str:
    canonical_input = json.dumps(tool_input, ensure_ascii=True, sort_keys=True)
    return hashlib.sha256(canonical_input.encode("utf-8")).hexdigest()


async def _finish_tool_call(
    database_session,
    *,
    tool_call_id: UUID | None,
    status: str,
    finished_at: datetime,
    output: dict | None = None,
    error_message: str | None = None,
) -> None:
    if tool_call_id is None:
        return
    tool_call = await database_session.scalar(
        select(ToolCall).where(ToolCall.id == tool_call_id).with_for_update()
    )
    if tool_call is None:
        return
    tool_call.status = status
    tool_call.output = output
    tool_call.error_message = error_message
    tool_call.finished_at = finished_at
    if tool_call.started_at is not None:
        tool_call.latency_ms = int(
            (finished_at - tool_call.started_at).total_seconds() * 1000
        )


async def process_task(
    task_id: UUID,
    redis: Redis | None = None,
    trace_fields: dict[str, str] | None = None,
) -> None:
    with trace_scope(
        "task.consume",
        {
            "event_type": "task.queued",
            "trace_context_missing": not trace_fields or "traceparent" not in trace_fields,
        },
        parent=extract_trace_context(trace_fields or {}),
    ):
        await _process_task(task_id, redis)


async def _process_task(task_id: UUID, redis: Redis | None = None) -> None:
    task_type: str | None = None
    tool_call_id: UUID | None = None
    tool_input: dict | None = None
    task_created_at: datetime | None = None
    attempt = 0
    async with async_session_factory() as database_session:
        async with database_session.begin():
            _, task = await _lock_task_and_session(database_session, task_id)
            if task is None:
                return
            if task.status == "cancel_requested":
                task.status = "cancelled"
                task.finished_at = datetime.now(timezone.utc)
                await _append_task_event(
                    database_session,
                    session_id=task.session_id,
                    event_type="task.cancelled",
                    payload={
                        "task_id": str(task.id),
                        "turn_id": str(task.turn_id) if task.turn_id else None,
                        "context_version": task.context_version,
                    },
                    dedupe_key=f"task.cancelled:{task.id}",
                )
                return
            if task.status != "queued":
                return
            now = datetime.now(timezone.utc)
            task.status = "running"
            task.attempt += 1
            task.started_at = now
            await _append_task_event(
                database_session,
                session_id=task.session_id,
                event_type="task.running",
                payload={
                    "task_id": str(task.id),
                    "turn_id": str(task.turn_id) if task.turn_id else None,
                    "context_version": task.context_version,
                },
                dedupe_key=f"task.running:{task.id}",
            )
            task_type = task.task_type
            task_created_at = task.created_at
            attempt = task.attempt
            if task_created_at is not None:
                task_queue_wait_seconds.labels(task_type, "started").observe(
                    max(0, (now - task_created_at).total_seconds())
                )
            if task_type == mock_travel_adapter.tool_name:
                if (
                    task.context_version is None
                    or task.target_preference_version is None
                ):
                    raise ValueError("mock_travel task is missing fixed versions")
                tool_input = dict(task.payload)
                tool_call = ToolCall(
                    task_id=task.id,
                    session_id=task.session_id,
                    context_version=task.context_version,
                    target_preference_version=task.target_preference_version,
                    tool_name=mock_travel_adapter.tool_name,
                    status="running",
                    request_hash=_tool_request_hash(tool_input),
                    input=tool_input,
                    started_at=now,
                )
                database_session.add(tool_call)
                await database_session.flush()
                tool_call_id = tool_call.id

    try:
        if await _task_cancellation_requested(task_id, redis):
            await _mark_task_cancelled(task_id)
            return

        if task_type == "smoke_test":
            result = {"message": "worker ok"}
        elif task_type == mock_travel_adapter.tool_name and tool_input is not None:
            tool_started = time.monotonic()
            with trace_scope(
                "tool.call",
                {"tool_name": mock_travel_adapter.tool_name, "attempt": attempt},
            ):
                result = await mock_travel_adapter.execute(tool_input)
            tool_call_seconds.labels(
                mock_travel_adapter.tool_name, "succeeded", str(attempt)
            ).observe(time.monotonic() - tool_started)
        else:
            raise ValueError(f"Unsupported task type: {task_type}")

        cancelled_after_execution = await _task_cancellation_requested(task_id, redis)
        async with async_session_factory() as database_session:
            async with database_session.begin():
                _, task = await _lock_task_and_session(database_session, task_id)
                if task is None:
                    return

                cancelled_before_commit = await has_task_cancel_key(redis, task_id)
                current = await _task_context_is_current(database_session, task)
                finished_at = datetime.now(timezone.utc)
                task.result = result
                task.finished_at = finished_at
                if (
                    cancelled_after_execution
                    or cancelled_before_commit
                    or not current
                    or task.status in {"cancel_requested", "cancelled"}
                ):
                    task.status = "discarded"
                    tasks_discarded_total.inc()
                    await _finish_tool_call(
                        database_session,
                        tool_call_id=tool_call_id,
                        status="discarded",
                        finished_at=finished_at,
                        output=result,
                    )
                    await _append_task_event(
                        database_session,
                        session_id=task.session_id,
                        event_type="task.result.discarded",
                        payload={
                            "task_id": str(task.id),
                            "turn_id": str(task.turn_id) if task.turn_id else None,
                            "context_version": task.context_version,
                        },
                        dedupe_key=f"task.discarded:{task.id}",
                    )
                    if task_created_at is not None and task_type is not None:
                        task_completion_seconds.labels(task_type, "discarded").observe(
                            max(0, (finished_at - task_created_at).total_seconds())
                        )
                    return

                task.status = "succeeded"
                await _finish_tool_call(
                    database_session,
                    tool_call_id=tool_call_id,
                    status="succeeded",
                    finished_at=finished_at,
                    output=result,
                )
                await _append_task_event(
                    database_session,
                    session_id=task.session_id,
                    event_type="task.succeeded",
                    payload={
                        "task_id": str(task.id),
                        "turn_id": str(task.turn_id) if task.turn_id else None,
                        "context_version": task.context_version,
                    },
                    dedupe_key=f"task.succeeded:{task.id}",
                )
                if task_created_at is not None and task_type is not None:
                    task_completion_seconds.labels(task_type, "succeeded").observe(
                        max(0, (finished_at - task_created_at).total_seconds())
                    )

        # task.succeeded outbox events are delivered to agent.compose by app.outbox.
        logger.info("task succeeded", extra={"task_id": task_id})
    except Exception:
        errors_total.labels("TASK_EXECUTION_FAILED").inc()
        async with async_session_factory() as database_session:
            async with database_session.begin():
                _, task = await _lock_task_and_session(database_session, task_id)
                if task is not None and task.status == "cancel_requested":
                    finished_at = datetime.now(timezone.utc)
                    task.status = "cancelled"
                    task.finished_at = finished_at
                    await _finish_tool_call(
                        database_session,
                        tool_call_id=tool_call_id,
                        status="cancelled",
                        finished_at=finished_at,
                    )
                    await _append_task_event(
                        database_session,
                        session_id=task.session_id,
                        event_type="task.cancelled",
                        payload={
                            "task_id": str(task.id),
                            "turn_id": str(task.turn_id) if task.turn_id else None,
                            "context_version": task.context_version,
                        },
                        dedupe_key=f"task.cancelled:{task.id}",
                    )
                elif task is not None and task.status not in {
                    "cancelled",
                    "discarded",
                    "succeeded",
                }:
                    finished_at = datetime.now(timezone.utc)
                    task.status = "failed"
                    task.error_message = "Task execution failed"
                    task.finished_at = finished_at
                    await _finish_tool_call(
                        database_session,
                        tool_call_id=tool_call_id,
                        status="failed",
                        finished_at=finished_at,
                        error_message="Task execution failed",
                    )
                    await _append_task_event(
                        database_session,
                        session_id=task.session_id,
                        event_type="task.failed",
                        payload={
                            "task_id": str(task.id),
                            "error_code": "TASK_EXECUTION_FAILED",
                        },
                        dedupe_key=f"task.failed:{task.id}",
                    )

        logger.exception(
            "task failed",
            extra={"task_id": task_id, "error_code": "TASK_EXECUTION_FAILED"},
        )


async def main() -> None:
    settings.validate_runtime_config(expected_service_role="task-worker")
    from app.observability import bootstrap_observability

    bootstrap_observability()
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=None,
    )

    try:
        await ensure_consumer_group(redis)
        logger.info("worker started: %s", CONSUMER_NAME)

        while True:
            messages = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {TASK_STREAM: ">"},
                count=1,
                block=5000,
            )

            for _, entries in messages:
                for message_id, fields in entries:
                    await process_task(UUID(fields["task_id"]), redis, fields)
                    await redis.xack(TASK_STREAM, GROUP_NAME, message_id)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("worker stopped")
