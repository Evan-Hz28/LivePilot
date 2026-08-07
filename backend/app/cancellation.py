import logging
from collections.abc import Iterable
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

CANCEL_KEY_TTL_SECONDS = 300
_CANCEL_KEY_PREFIX = "cancel:task:"
logger = logging.getLogger(__name__)


def task_cancel_key(task_id: UUID) -> str:
    return f"{_CANCEL_KEY_PREFIX}{task_id}"


async def write_task_cancel_keys(redis: Redis, task_ids: Iterable[UUID]) -> None:
    for task_id in task_ids:
        try:
            await redis.set(
                task_cancel_key(task_id),
                "1",
                ex=CANCEL_KEY_TTL_SECONDS,
            )
        except RedisError:
            # PostgreSQL task state is authoritative; this key only shortens cancellation latency.
            logger.warning(
                "could not write cancellation key",
                extra={"task_id": task_id, "error_code": "CANCEL_KEY_WRITE"},
            )


async def has_task_cancel_key(redis: Redis | None, task_id: UUID) -> bool:
    if redis is None:
        return False
    try:
        return bool(await redis.exists(task_cancel_key(task_id)))
    except RedisError:
        logger.warning(
            "could not read cancellation key",
            extra={"task_id": task_id, "error_code": "CANCEL_KEY_READ"},
        )
        return False
