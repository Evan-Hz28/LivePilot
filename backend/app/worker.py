import asyncio
import logging
import socket
from datetime import datetime, timezone
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import select

from app.cancellation import has_task_cancel_key
from app.config import settings
from app.db import async_session_factory
from app.models import EventOutbox, Preference, Task, TravelSession
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
            return True


def _mock_result(task: Task) -> dict:
    destination = str(task.payload.get("destination") or "未指定目的地")
    return {
        "mock": True,
        "tool": "mock_travel",
        "destination": destination,
        "recommendations": [
            {"name": f"{destination}历史街区", "category": "culture"},
            {"name": f"{destination}城市公园", "category": "outdoors"},
        ],
        "query": task.payload.get("query", ""),
    }


async def process_task(task_id: UUID, redis: Redis | None = None) -> None:
    task_type: str | None = None
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
            task.status = "running"
            task.attempt += 1
            task.started_at = datetime.now(timezone.utc)
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

    try:
        if await _task_cancellation_requested(task_id, redis):
            await _mark_task_cancelled(task_id)
            return

        if task_type == "smoke_test":
            result = {"message": "worker ok"}
        elif task_type == "mock_travel":
            async with async_session_factory() as database_session:
                task = await database_session.get(Task, task_id)
                if task is None:
                    return
                result = _mock_result(task)
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
                task.result = result
                task.finished_at = datetime.now(timezone.utc)
                if (
                    cancelled_after_execution
                    or cancelled_before_commit
                    or not current
                    or task.status in {"cancel_requested", "cancelled"}
                ):
                    task.status = "discarded"
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
                    return

                task.status = "succeeded"
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

        # task.succeeded outbox events are delivered to agent.compose by app.outbox.
        logger.info("task succeeded: %s", task_id)
    except Exception as error:
        async with async_session_factory() as database_session:
            async with database_session.begin():
                _, task = await _lock_task_and_session(database_session, task_id)
                if task is not None and task.status == "cancel_requested":
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
                elif task is not None and task.status not in {
                    "cancelled",
                    "discarded",
                    "succeeded",
                }:
                    task.status = "failed"
                    task.error_message = str(error)
                    task.finished_at = datetime.now(timezone.utc)
                    await _append_task_event(
                        database_session,
                        session_id=task.session_id,
                        event_type="task.failed",
                        payload={"task_id": str(task.id), "error": str(error)},
                        dedupe_key=f"task.failed:{task.id}",
                    )

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
                    await process_task(UUID(fields["task_id"]), redis)
                    await redis.xack(TASK_STREAM, GROUP_NAME, message_id)
    finally:
        await redis.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("worker stopped")
