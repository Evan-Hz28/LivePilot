from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, status
from redis.asyncio import Redis

from app.config import settings
from app.db import async_session_factory
from app.models import Task, TravelSession

TASK_STREAM = "travel.tasks"

app = FastAPI(title="LivePilot API")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/demo/tasks/smoke-test",
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_smoke_test_task() -> dict[str, str]:
    session_id = uuid4()
    task_id = uuid4()

    async with async_session_factory() as database_session:
        database_session.add(
            TravelSession(
                id=session_id,
                user_id=uuid4(),
            )
        )
        await database_session.flush()

        database_session.add(
            Task(
                id=task_id,
                session_id=session_id,
                task_type="smoke_test",
                payload={},
            )
        )
        await database_session.commit()

    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await redis.xadd(
            TASK_STREAM,
            {
                "task_id": str(task_id),
                "task_type": "smoke_test",
            },
        )
    finally:
        await redis.aclose()

    return {
        "task_id": str(task_id),
        "status": "queued",
    }


@app.get("/demo/tasks/{task_id}")
async def get_task(task_id: UUID) -> dict[str, object]:
    async with async_session_factory() as database_session:
        task = await database_session.get(Task, task_id)

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": str(task.id),
        "status": task.status,
        "result": task.result,
        "error_message": task.error_message,
    }