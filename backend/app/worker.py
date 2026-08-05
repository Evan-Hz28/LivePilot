import asyncio
import logging
import socket
from datetime import datetime, timezone
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from app.config import settings
from app.db import async_session_factory
from app.models import Task

TASK_STREAM = "travel.tasks"
GROUP_NAME = "livepilot-workers"
CONSUMER_NAME = f"{socket.gethostname()}-worker"

logging.basicConfig(level=logging.INFO)
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


async def process_task(task_id: UUID) -> None:
    async with async_session_factory() as database_session:
        task = await database_session.get(Task, task_id)

        if task is None or task.status != "queued":
            return

        task.status = "running"
        task.started_at = datetime.now(timezone.utc)
        await database_session.commit()

    try:
        result = {"message": "worker ok"}

        async with async_session_factory() as database_session:
            task = await database_session.get(Task, task_id)

            if task is None:
                return

            task.status = "succeeded"
            task.result = result
            task.finished_at = datetime.now(timezone.utc)
            await database_session.commit()

        logger.info("task succeeded: %s", task_id)
    except Exception as error:
        async with async_session_factory() as database_session:
            task = await database_session.get(Task, task_id)

            if task is not None:
                task.status = "failed"
                task.error_message = str(error)
                task.finished_at = datetime.now(timezone.utc)
                await database_session.commit()

        logger.exception("task failed: %s", task_id)


async def main() -> None:
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
                    await process_task(UUID(fields["task_id"]))
                    await redis.xack(TASK_STREAM, GROUP_NAME, message_id)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("worker stopped")