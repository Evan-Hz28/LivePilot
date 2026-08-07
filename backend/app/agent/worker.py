from __future__ import annotations

import asyncio
import logging
import socket
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings
from app.db import async_session_factory
from app.observability import extract_trace_context, trace_scope

from .schemas import ContextPacket
from .service import (
    AGENT_COMPOSE_STREAM,
    PLAN_STREAM,
    build_context_packet,
    compose_reply,
    create_tasks_for_decision,
    decide,
)

GROUP_NAME = "livepilot-agent-workers"
CONSUMER_NAME = f"{socket.gethostname()}-agent-worker"
logger = logging.getLogger(__name__)


async def ensure_consumer_groups(redis: Redis) -> None:
    for stream in (PLAN_STREAM, AGENT_COMPOSE_STREAM):
        try:
            await redis.xgroup_create(stream, GROUP_NAME, id="0-0", mkstream=True)
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise


async def process_plan_message(fields: dict[str, str], redis: Redis | None = None) -> list[UUID]:
    with trace_scope(
        "agent.plan.consume",
        {
            "event_type": "turn.finalized",
            "trace_context_missing": "traceparent" not in fields,
        },
        parent=extract_trace_context(fields),
    ):
        packet = ContextPacket.model_validate_json(fields["packet"])
        decision = decide(packet)
        async with async_session_factory() as database_session:
            tasks = await create_tasks_for_decision(
                database_session,
                packet=packet,
                decision=decision,
            )

    # task.queued outbox events are delivered to travel.tasks by app.outbox.
    return [task.id for task in tasks]


async def process_compose_message(fields: dict[str, str]) -> bool:
    with trace_scope(
        "agent.compose.consume",
        {
            "event_type": "task.succeeded",
            "trace_context_missing": "traceparent" not in fields,
        },
        parent=extract_trace_context(fields),
    ):
        async with async_session_factory() as database_session:
            result = await compose_reply(
                database_session,
                session_id=UUID(fields["session_id"]),
                turn_id=UUID(fields["turn_id"]),
                context_version=int(fields["context_version"]),
            )
        if result.reply is not None:
            return True
        if not result.itinerary_conflict:
            return False

        async with async_session_factory() as database_session:
            packet = await build_context_packet(
                database_session,
                session_id=UUID(fields["session_id"]),
                turn_id=UUID(fields["turn_id"]),
                context_version=int(fields["context_version"]),
            )
        async with async_session_factory() as database_session:
            await create_tasks_for_decision(
                database_session,
                packet=packet,
                decision=decide(packet),
            )
        return False


async def main() -> None:
    settings.validate_runtime_config(expected_service_role="agent-worker")
    from app.observability import bootstrap_observability

    bootstrap_observability()
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=None,
    )
    try:
        await ensure_consumer_groups(redis)
        while True:
            messages = await redis.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {PLAN_STREAM: ">", AGENT_COMPOSE_STREAM: ">"},
                count=1,
                block=5000,
            )
            for stream, entries in messages:
                for message_id, fields in entries:
                    if stream == PLAN_STREAM:
                        await process_plan_message(fields, redis)
                    else:
                        await process_compose_message(fields)
                    await redis.xack(stream, GROUP_NAME, message_id)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("agent worker stopped")
