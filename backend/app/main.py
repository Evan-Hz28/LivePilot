from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from sqlalchemy import Select, select, update

from app.config import settings
from app.db import async_session_factory
from app.models import EventOutbox, Preference, Task, TravelSession, Turn

TASK_STREAM = "travel.tasks"

app = FastAPI(title="LivePilot API")


class CreateSessionRequest(BaseModel):
    locale: str = Field(default="zh-CN", min_length=1, max_length=16)
    timezone: str = Field(default="Asia/Shanghai", min_length=1, max_length=64)
    preference: dict[str, Any] = Field(default_factory=dict)


class PreferenceUpdateRequest(BaseModel):
    base_context_version: int = Field(ge=0)
    patch: dict[str, Any] = Field(min_length=1)
    client_event_id: str | None = Field(default=None, min_length=1, max_length=100)
    source_turn_id: UUID | None = None


def serialize_session(session: TravelSession) -> dict[str, object]:
    return {
        "session_id": str(session.id),
        "status": session.status,
        "context_version": session.context_version,
        "last_event_seq": session.last_event_seq,
        "locale": session.locale,
        "timezone": session.timezone,
        "created_at": session.created_at,
    }


def serialize_preference(preference: Preference) -> dict[str, object]:
    return {
        "preference_id": str(preference.id),
        "version": preference.version,
        "status": preference.status,
        "payload": preference.payload,
        "source_turn_id": (
            str(preference.source_turn_id) if preference.source_turn_id else None
        ),
        "created_at": preference.created_at,
    }


def serialize_turn(turn: Turn) -> dict[str, object]:
    return {
        "turn_id": str(turn.id),
        "sequence_no": turn.sequence_no,
        "kind": turn.kind,
        "status": turn.status,
        "context_version": turn.context_version,
        "parent_turn_id": str(turn.parent_turn_id) if turn.parent_turn_id else None,
        "interrupt_reason": turn.interrupt_reason,
        "started_at": turn.started_at,
        "finalized_at": turn.finalized_at,
        "completed_at": turn.completed_at,
    }


def serialize_task(task: Task) -> dict[str, object]:
    return {
        "task_id": str(task.id),
        "task_type": task.task_type,
        "status": task.status,
        "payload": task.payload,
        "result": task.result,
        "error_message": task.error_message,
        "attempt": task.attempt,
        "created_at": task.created_at,
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    }


def serialize_event(event: EventOutbox) -> dict[str, object]:
    return {
        "event_seq": event.event_seq,
        "event_type": event.event_type,
        "payload": event.payload,
        "created_at": event.created_at,
    }


def active_preference_query(session_id: UUID) -> Select[tuple[Preference]]:
    return select(Preference).where(
        Preference.session_id == session_id,
        Preference.status == "active",
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/sessions", status_code=status.HTTP_201_CREATED)
async def create_session(request: CreateSessionRequest) -> dict[str, object]:
    session_id = uuid4()

    async with async_session_factory() as database_session:
        async with database_session.begin():
            travel_session = TravelSession(
                id=session_id,
                user_id=uuid4(),
                last_event_seq=1,
                locale=request.locale,
                timezone=request.timezone,
            )
            database_session.add(travel_session)
            await database_session.flush()

            preference = Preference(
                session_id=session_id,
                version=1,
                payload=request.preference,
            )
            database_session.add(preference)
            database_session.add(
                EventOutbox(
                    session_id=session_id,
                    event_seq=1,
                    event_type="session.created",
                    payload={
                        "context_version": 0,
                        "preference_version": 1,
                    },
                    dedupe_key=f"session.created:{session_id}",
                )
            )

    return {
        "session_id": str(session_id),
        "context_version": 0,
        "preference_version": 1,
        "event_seq": 1,
    }


@app.get("/v1/sessions/{session_id}/snapshot")
async def get_session_snapshot(
    session_id: UUID,
    after_event_seq: int = Query(default=0, ge=0),
) -> dict[str, object]:
    async with async_session_factory() as database_session:
        travel_session = await database_session.get(TravelSession, session_id)
        if travel_session is None:
            raise HTTPException(status_code=404, detail="Session not found")

        preference = await database_session.scalar(active_preference_query(session_id))
        turns = list(
            (
                await database_session.scalars(
                    select(Turn)
                    .where(Turn.session_id == session_id)
                    .order_by(Turn.sequence_no)
                )
            ).all()
        )
        tasks = list(
            (
                await database_session.scalars(
                    select(Task)
                    .where(Task.session_id == session_id)
                    .order_by(Task.created_at)
                )
            ).all()
        )
        events = list(
            (
                await database_session.scalars(
                    select(EventOutbox)
                    .where(
                        EventOutbox.session_id == session_id,
                        EventOutbox.event_seq > after_event_seq,
                    )
                    .order_by(EventOutbox.event_seq)
                )
            ).all()
        )

    return {
        "session": serialize_session(travel_session),
        "active_preference": serialize_preference(preference)
        if preference is not None
        else None,
        "turns": [serialize_turn(turn) for turn in turns],
        "tasks": [serialize_task(task) for task in tasks],
        "itinerary": {
            "status": "not_created",
            "context_version": travel_session.context_version,
        },
        "missed_events": [serialize_event(event) for event in events],
    }


@app.patch("/v1/sessions/{session_id}/preferences")
async def update_preferences(
    session_id: UUID,
    request: PreferenceUpdateRequest,
) -> dict[str, object]:
    client_event_id = request.client_event_id or str(uuid4())
    dedupe_key = f"preference.update:{session_id}:{client_event_id}"

    async with async_session_factory() as database_session:
        conflict: dict[str, int] | None = None
        idempotent_event: EventOutbox | None = None
        updated_state: tuple[int, int] | None = None
        preference: Preference | None = None

        async with database_session.begin():
            idempotent_event = await database_session.scalar(
                select(EventOutbox).where(EventOutbox.dedupe_key == dedupe_key)
            )
            if idempotent_event is None:
                updated = await database_session.execute(
                    update(TravelSession)
                    .where(
                        TravelSession.id == session_id,
                        TravelSession.context_version == request.base_context_version,
                    )
                    .values(
                        context_version=TravelSession.context_version + 1,
                        last_event_seq=TravelSession.last_event_seq + 1,
                    )
                    .returning(
                        TravelSession.context_version,
                        TravelSession.last_event_seq,
                    )
                )
                state = updated.one_or_none()

                if state is None:
                    travel_session = await database_session.get(
                        TravelSession,
                        session_id,
                    )
                    if travel_session is None:
                        raise HTTPException(
                            status_code=404,
                            detail="Session not found",
                        )

                    idempotent_event = await database_session.scalar(
                        select(EventOutbox).where(
                            EventOutbox.dedupe_key == dedupe_key
                        )
                    )
                    if idempotent_event is None:
                        active = await database_session.scalar(
                            active_preference_query(session_id)
                        )
                        conflict = {
                            "context_version": travel_session.context_version,
                            "preference_version": active.version if active else 0,
                        }
                else:
                    active = await database_session.scalar(
                        active_preference_query(session_id)
                    )
                    if active is None:
                        raise RuntimeError("Session has no active preference")

                    if request.source_turn_id is not None:
                        source_turn = await database_session.scalar(
                            select(Turn.id).where(
                                Turn.id == request.source_turn_id,
                                Turn.session_id == session_id,
                            )
                        )
                        if source_turn is None:
                            raise HTTPException(
                                status_code=422,
                                detail="source_turn_id does not belong to the session",
                            )

                    active.status = "superseded"
                    active.superseded_at = datetime.now(timezone.utc)
                    await database_session.flush()
                    preference = Preference(
                        session_id=session_id,
                        version=active.version + 1,
                        payload={**active.payload, **request.patch},
                        source_turn_id=request.source_turn_id,
                    )
                    updated_state = (state.context_version, state.last_event_seq)
                    database_session.add(preference)
                    database_session.add(
                        EventOutbox(
                            session_id=session_id,
                            event_seq=state.last_event_seq,
                            event_type="preference.updated",
                            payload={
                                "context_version": state.context_version,
                                "preference_version": preference.version,
                            },
                            dedupe_key=dedupe_key,
                        )
                    )

        if idempotent_event is not None:
            return {
                "session_id": str(session_id),
                "context_version": idempotent_event.payload["context_version"],
                "preference_version": idempotent_event.payload[
                    "preference_version"
                ],
                "cancelled_task_ids": [],
                "event_seq": idempotent_event.event_seq,
            }

        if conflict is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "Context version conflict",
                    **conflict,
                },
            )

    assert updated_state is not None
    assert preference is not None
    return {
        "session_id": str(session_id),
        "context_version": updated_state[0],
        "preference_version": preference.version,
        "cancelled_task_ids": [],
        "event_seq": updated_state[1],
    }


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
