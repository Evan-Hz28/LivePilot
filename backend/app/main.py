import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from collections.abc import AsyncIterator
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import Select, select, update

from app.config import settings
from app.db import async_session_factory
from app.models import EventOutbox, Preference, Task, TravelSession, Turn
from app.agent.service import (
    finalize_text_turn,
)
from app.outbox import run_outbox_publisher
from app.realtime import (
    RealtimeTokenError,
    issue_realtime_token,
    redeem_realtime_token,
)

TASK_STREAM = "travel.tasks"


@asynccontextmanager
async def lifespan(_: FastAPI):
    publisher_task = asyncio.create_task(run_outbox_publisher())
    try:
        yield
    finally:
        publisher_task.cancel()
        await asyncio.gather(publisher_task, return_exceptions=True)


app = FastAPI(title="LivePilot API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


class CreateSessionRequest(BaseModel):
    locale: str = Field(default="zh-CN", min_length=1, max_length=16)
    timezone: str = Field(default="Asia/Shanghai", min_length=1, max_length=64)
    preference: dict[str, Any] = Field(default_factory=dict)


class PreferenceUpdateRequest(BaseModel):
    base_context_version: int = Field(ge=0)
    patch: dict[str, Any] = Field(min_length=1)
    client_event_id: str | None = Field(default=None, min_length=1, max_length=100)
    source_turn_id: UUID | None = None


class FinalizeTurnRequest(BaseModel):
    text: str = Field(min_length=1, max_length=20_000)
    client_event_id: str | None = Field(default=None, min_length=1, max_length=100)


class RealtimeTokenRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)


class RealtimeTokenRedeemRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    token: str = Field(min_length=1, max_length=512)


async def get_realtime_redis() -> AsyncIterator[Redis]:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        yield redis
    finally:
        await redis.aclose()


RealtimeRedis = Annotated[Redis, Depends(get_realtime_redis)]


def serialize_session(session: TravelSession) -> dict[str, object]:
    return {
        "session_id": str(session.id),
        "status": session.status,
        "context_version": session.context_version,
        "realtime_connection_epoch": session.realtime_connection_epoch,
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
    content = turn.content or {}
    return {
        "turn_id": str(turn.id),
        "sequence_no": turn.sequence_no,
        "kind": turn.kind,
        "status": turn.status,
        "context_version": turn.context_version,
        "parent_turn_id": str(turn.parent_turn_id) if turn.parent_turn_id else None,
        "interrupt_reason": turn.interrupt_reason,
        "content": turn.content,
        "text": content.get("text"),
        "reply_context": content.get("reply_context"),
        "started_at": turn.started_at,
        "finalized_at": turn.finalized_at,
        "completed_at": turn.completed_at,
    }


def serialize_task(task: Task) -> dict[str, object]:
    return {
        "task_id": str(task.id),
        "session_id": str(task.session_id),
        "turn_id": str(task.turn_id) if task.turn_id else None,
        "task_type": task.task_type,
        "status": task.status,
        "context_version": task.context_version,
        "target_preference_version": task.target_preference_version,
        "idempotency_key": task.idempotency_key,
        "payload": task.payload,
        "result": task.result,
        "error_message": task.error_message,
        "attempt": task.attempt,
        "deadline_at": task.deadline_at,
        "created_at": task.created_at,
        "started_at": task.started_at,
        "finished_at": task.finished_at,
    }


def serialize_event(event: EventOutbox) -> dict[str, object]:
    return {
        "event_id": str(event.id),
        "session_id": str(event.session_id),
        "event_seq": event.event_seq,
        "event_type": event.event_type,
        "payload": event.payload,
        "dedupe_key": event.dedupe_key,
        "published_at": event.published_at,
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
        "event_ws_url": f"/v1/sessions/{session_id}/events",
    }


async def _load_session_snapshot(
    session_id: UUID,
    after_event_seq: int = 0,
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
        "after_event_seq": after_event_seq,
    }


@app.get("/v1/sessions/{session_id}/snapshot")
async def get_session_snapshot(
    session_id: UUID,
    after_event_seq: int = Query(default=0, ge=0),
) -> dict[str, object]:
    return await _load_session_snapshot(session_id, after_event_seq)


@app.post("/v1/sessions/{session_id}/realtime-token")
async def create_realtime_token(
    session_id: UUID,
    request: RealtimeTokenRequest,
    redis: RealtimeRedis,
) -> dict[str, object]:
    async with async_session_factory() as database_session:
        try:
            grant = await issue_realtime_token(
                database_session,
                redis,
                session_id=session_id,
                device_id=request.device_id,
            )
        except LookupError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except RedisError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Realtime token service unavailable",
            ) from error

    return {
        "token": grant.token,
        "device_id": grant.device_id,
        "connection_epoch": grant.connection_epoch,
        "expires_at": grant.expires_at,
        "provider_config": grant.provider_config,
    }


@app.post("/v1/sessions/{session_id}/realtime-token/redeem")
async def redeem_session_realtime_token(
    session_id: UUID,
    request: RealtimeTokenRedeemRequest,
    redis: RealtimeRedis,
) -> dict[str, object]:
    async with async_session_factory() as database_session:
        try:
            grant = await redeem_realtime_token(
                database_session,
                redis,
                session_id=session_id,
                device_id=request.device_id,
                token=request.token,
            )
        except RealtimeTokenError as error:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Realtime token is invalid or expired",
            ) from error
        except RedisError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Realtime token service unavailable",
            ) from error

    return {
        "device_id": grant.device_id,
        "connection_epoch": grant.connection_epoch,
        "expires_at": grant.expires_at,
        "provider_config": grant.provider_config,
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
    "/v1/sessions/{session_id}/turns",
    status_code=status.HTTP_202_ACCEPTED,
)
async def finalize_turn(
    session_id: UUID,
    request: FinalizeTurnRequest,
) -> dict[str, object]:
    client_event_id = request.client_event_id or str(uuid4())
    async with async_session_factory() as database_session:
        try:
            finalized = await finalize_text_turn(
                database_session,
                session_id=session_id,
                text=request.text,
                client_event_id=client_event_id,
            )
        except LookupError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    return {
        "session_id": str(session_id),
        "turn_id": str(finalized.turn_id),
        "context_version": finalized.context_version,
        "preference_version": finalized.preference_version,
        "event_seq": finalized.event_seq,
        "status": "pending",
        "client_event_id": client_event_id,
    }


def _snapshot_message(snapshot: dict[str, object]) -> dict[str, object]:
    session = snapshot["session"]
    assert isinstance(session, dict)
    event_seq = int(session["last_event_seq"])
    session_id = str(session["session_id"])
    return {
        "type": "session.snapshot",
        "event_type": "session.snapshot",
        "event_id": f"snapshot:{session_id}:{event_seq}",
        "event_seq": event_seq,
        "session_id": session_id,
        "payload": jsonable_encoder(snapshot),
        "snapshot": jsonable_encoder(snapshot),
    }


def _event_message(event: dict[str, object]) -> dict[str, object]:
    return jsonable_encoder({
        "type": event["event_type"],
        "event_type": event["event_type"],
        "event_id": event["event_id"],
        "event_seq": event["event_seq"],
        "session_id": event["session_id"],
        "payload": event["payload"],
        "created_at": event["created_at"],
    })


async def _send_snapshot_and_missed_events(
    websocket: WebSocket,
    snapshot: dict[str, object],
) -> int:
    await websocket.send_json(_snapshot_message(snapshot))
    missed_events = snapshot["missed_events"]
    assert isinstance(missed_events, list)
    for event in missed_events:
        await websocket.send_json(_event_message(event))
    session = snapshot["session"]
    assert isinstance(session, dict)
    return int(session["last_event_seq"])


@app.websocket("/v1/sessions/{session_id}/events")
@app.websocket("/v1/sessions/{session_id}/ws")
async def session_events(websocket: WebSocket, session_id: UUID) -> None:
    try:
        after_event_seq = int(websocket.query_params.get("after_event_seq", "0"))
    except ValueError:
        await websocket.close(code=1008, reason="Invalid event cursor")
        return
    if after_event_seq < 0:
        await websocket.close(code=1008, reason="after_event_seq must be non-negative")
        return

    try:
        snapshot = await _load_session_snapshot(session_id, after_event_seq)
    except HTTPException:
        await websocket.close(code=1008, reason="Session not found")
        return

    await websocket.accept()
    after_event_seq = await _send_snapshot_and_missed_events(websocket, snapshot)

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_json(), timeout=0.5)
            except asyncio.TimeoutError:
                message = None
            except WebSocketDisconnect:
                return

            if message is not None:
                message_type = message.get("type")
                if message_type == "session.resume":
                    requested_seq = message.get("after_event_seq", after_event_seq)
                    try:
                        requested_seq = max(0, int(requested_seq))
                        snapshot = await _load_session_snapshot(session_id, requested_seq)
                    except (TypeError, ValueError):
                        await websocket.send_json(
                            {"type": "error", "detail": "Invalid event cursor"}
                        )
                        continue
                    after_event_seq = await _send_snapshot_and_missed_events(
                        websocket, snapshot
                    )
                elif message_type == "turn.finalized":
                    payload = message.get("payload") or message
                    text = payload.get("text")
                    if not isinstance(text, str) or not text.strip():
                        await websocket.send_json(
                            {"type": "error", "detail": "text is required"}
                        )
                        continue
                    client_event_id = payload.get("client_event_id") or str(uuid4())
                    async with async_session_factory() as database_session:
                        try:
                            finalized = await finalize_text_turn(
                                database_session,
                                session_id=session_id,
                                text=text,
                                client_event_id=client_event_id,
                            )
                        except LookupError:
                            await websocket.send_json(
                                {"type": "error", "detail": "Session not found"}
                            )
                            continue
                    await websocket.send_json(
                        {
                            "type": "turn.accepted",
                            "event_seq": finalized.event_seq,
                            "session_id": str(session_id),
                            "payload": {
                                "turn_id": str(finalized.turn_id),
                                "context_version": finalized.context_version,
                                "preference_version": finalized.preference_version,
                                "client_event_id": client_event_id,
                            },
                        }
                    )
                continue

            snapshot = await _load_session_snapshot(session_id, after_event_seq)
            missed_events = snapshot["missed_events"]
            assert isinstance(missed_events, list)
            for event in missed_events:
                await websocket.send_json(_event_message(event))
                after_event_seq = int(event["event_seq"])
    except WebSocketDisconnect:
        return


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
