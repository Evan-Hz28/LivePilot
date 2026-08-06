from __future__ import annotations

import asyncio
import logging
import socket
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings
from app.db import async_session_factory

from .schemas import ContextPacket
from .service import (
    AGENT_COMPOSE_STREAM,
    PLAN_STREAM,
    TRAVEL_TASK_STREAM,
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
    packet = ContextPacket.model_validate_json(fields["packet"])
    decision = decide(packet)
    async with async_session_factory() as database_session:
        tasks = await create_tasks_for_decision(
            database_session,
            packet=packet,
            decision=decision,
        )

    if redis is not None:
        for task in tasks:
            await redis.xadd(
                TRAVEL_TASK_STREAM,
                {
                    "task_id": str(task.id),
                    "task_type": task.task_type,
                    "session_id": str(task.session_id),
                    "turn_id": str(task.turn_id or ""),
                    "context_version": str(task.context_version or ""),
                    "deadline_at": task.deadline_at.isoformat()
                    if task.deadline_at
                    else "",
                },
            )
    return [task.id for task in tasks]


async def process_compose_message(fields: dict[str, str]) -> bool:
    async with async_session_factory() as database_session:
        reply = await compose_reply(
            database_session,
            session_id=UUID(fields["session_id"]),
            turn_id=UUID(fields["turn_id"]),
            context_version=int(fields["context_version"]),
        )
    return reply is not None


async def main() -> None:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
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
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("agent worker stopped")
