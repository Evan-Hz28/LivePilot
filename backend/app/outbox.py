from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import select

from app.agent.service import (
    COMPOSE_STREAM,
    PLAN_STREAM,
    TRAVEL_TASK_STREAM,
    build_context_packet,
)
from app.config import settings
from app.db import async_session_factory
from app.models import EventOutbox, Task
from app.metrics import outbox_publish_failures_total
from app.observability import extract_trace_context, inject_trace_context, trace_scope

EVENT_STREAM = "session.events"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OutboxPublishResult:
    attempted: int
    published: int
    failed: int


def event_stream_fields(event: EventOutbox) -> dict[str, str]:
    fields = {
        "event_id": str(event.id),
        "session_id": str(event.session_id),
        "event_seq": str(event.event_seq),
        "event_type": event.event_type,
        "payload": json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
    }
    return inject_trace_context(fields)


async def _publish_turn_finalized(redis: Redis, event: EventOutbox) -> None:
    async with async_session_factory() as database_session:
        packet = await build_context_packet(
            database_session,
            session_id=event.session_id,
            turn_id=UUID(str(event.payload["turn_id"])),
            context_version=int(event.payload["context_version"]),
            preference_version=int(event.payload["preference_version"]),
        )
    await redis.xadd(
        PLAN_STREAM,
        inject_trace_context(
            {
            "session_id": str(event.session_id),
            "turn_id": event.payload["turn_id"],
            "context_version": str(event.payload["context_version"]),
            "packet": packet.model_dump_json(),
            }
        ),
    )


async def _publish_task_queued(redis: Redis, event: EventOutbox) -> None:
    async with async_session_factory() as database_session:
        task = await database_session.get(Task, UUID(str(event.payload["task_id"])))
    if task is None:
        raise LookupError(f"Task not found: {event.payload['task_id']}")

    await redis.xadd(
        TRAVEL_TASK_STREAM,
        inject_trace_context(
            {
            "task_id": str(task.id),
            "task_type": task.task_type,
            "session_id": str(task.session_id),
            "turn_id": str(task.turn_id or ""),
            "context_version": str(task.context_version or ""),
            "deadline_at": task.deadline_at.isoformat() if task.deadline_at else "",
            }
        ),
    )


async def _publish_task_succeeded(redis: Redis, event: EventOutbox) -> None:
    turn_id = event.payload.get("turn_id")
    context_version = event.payload.get("context_version")
    if turn_id is None or context_version is None:
        return
    await redis.xadd(
        COMPOSE_STREAM,
        inject_trace_context(
            {
            "task_id": event.payload["task_id"],
            "session_id": str(event.session_id),
            "turn_id": str(turn_id),
            "context_version": str(context_version),
            }
        ),
    )


async def publish_event(redis: Redis, event: EventOutbox) -> None:
    """Publish one durable event; callers mark it complete only after this returns."""
    with trace_scope(
        "outbox.publish",
        {"event_type": event.event_type},
        parent=extract_trace_context({"traceparent": event.traceparent})
        if event.traceparent
        else None,
    ):
        if event.event_type == "turn.finalized":
            await _publish_turn_finalized(redis, event)
        elif event.event_type == "task.queued":
            await _publish_task_queued(redis, event)
        elif event.event_type == "task.succeeded":
            await _publish_task_succeeded(redis, event)

        await redis.xadd(EVENT_STREAM, event_stream_fields(event))


async def publish_pending_events(
    redis: Redis | None = None,
    *,
    limit: int = 100,
) -> OutboxPublishResult:
    """Publish unpublished outbox rows, leaving failures available for retry."""
    if limit < 1:
        return OutboxPublishResult(attempted=0, published=0, failed=0)

    owns_redis = redis is None
    if redis is None:
        redis = Redis.from_url(settings.redis_url, decode_responses=True)

    try:
        async with async_session_factory() as database_session:
            events = list(
                (
                    await database_session.scalars(
                        select(EventOutbox)
                        .where(EventOutbox.published_at.is_(None))
                        .order_by(EventOutbox.id)
                        .limit(limit)
                    )
                ).all()
            )

        published = 0
        failed = 0
        for event in events:
            try:
                await publish_event(redis, event)
                async with async_session_factory() as database_session:
                    async with database_session.begin():
                        current = await database_session.scalar(
                            select(EventOutbox)
                            .where(EventOutbox.id == event.id)
                            .with_for_update()
                        )
                        if current is not None and current.published_at is None:
                            current.published_at = datetime.now(timezone.utc)
                            published += 1
            except Exception:
                failed += 1
                outbox_publish_failures_total.inc()
                logger.exception("outbox publish failed", extra={"error_code": "OUTBOX_PUBLISH"})

        return OutboxPublishResult(
            attempted=len(events),
            published=published,
            failed=failed,
        )
    finally:
        if owns_redis:
            await redis.aclose()


async def run_outbox_publisher(poll_interval: float = 0.5) -> None:
    """Run the retry loop used by the API process in local development."""
    settings.validate_runtime_config(expected_service_role="api")
    from app.observability import bootstrap_observability

    bootstrap_observability()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        while True:
            try:
                result = await publish_pending_events(redis)
            except Exception:
                logger.exception("outbox publisher loop failed")
                result = None
            if result is not None and result.published:
                logger.debug("published %s outbox events", result.published)
            await asyncio.sleep(poll_interval)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(run_outbox_publisher())
    except KeyboardInterrupt:
        logger.info("outbox publisher stopped")
