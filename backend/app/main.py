import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel, ConfigDict, Field
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import Select, select, update

from app.auth import (
    AuthenticationError,
    CurrentPrincipal,
    SessionOwner,
    authenticate_websocket,
    get_or_create_principal,
    websocket_session_is_owned,
)
from app.input_validation import validate_json_value
from app.config import settings
from app.cancellation import write_task_cancel_keys
from app.db import async_session_factory
from app.main_dependencies import RealtimeRedis, get_realtime_redis
from app.rate_limit import (
    CreateSessionRateLimit,
    ReadSessionRateLimit,
    WriteSessionRateLimit,
    enforce_rate_limit,
)
from app.metrics import (
    errors_total,
    interrupt_effective_seconds,
    registry,
    resume_conflicts_total,
)
from app.observability import (
    bootstrap_observability,
    current_traceparent,
    extract_trace_context,
    trace_scope,
)
from app.interrupts import InterruptResult, register_interrupt
from app.models import (
    EventOutbox,
    Itinerary,
    Principal,
    Preference,
    Task,
    ToolCall,
    TravelSession,
    Turn,
)
from app.agent.service import (
    finalize_text_turn,
)
from app.outbox import run_outbox_publisher
from app.realtime import (
    RealtimeTokenEpochError,
    RealtimeTokenError,
    issue_realtime_token,
    redeem_realtime_token,
)

TASK_STREAM = "travel.tasks"
IDENTIFIER_PATTERN = r"^[A-Za-z0-9._:-]+$"
ALLOWED_INTERRUPT_REASONS = {"user_interrupt", "voice_stopped", "user_cancelled"}


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings.validate_runtime_config(expected_service_role="api")
    bootstrap_observability()
    publisher_task = asyncio.create_task(run_outbox_publisher())
    try:
        yield
    finally:
        publisher_task.cancel()
        await asyncio.gather(publisher_task, return_exceptions=True)


app = FastAPI(title="LivePilot API", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_cors_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "traceparent", "tracestate"],
)


@app.exception_handler(RequestValidationError)
async def invalid_request(_: Request, __: RequestValidationError) -> JSONResponse:
    errors_total.labels("REQUEST_VALIDATION").inc()
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={"detail": "Invalid request"},
    )


@app.middleware("http")
async def enforce_request_body_limit(request, call_next):
    if request.url.path == "/metrics" and request.headers.get("origin"):
        return JSONResponse(status_code=status.HTTP_404_NOT_FOUND, content={"detail": "Not found"})
    content_type = request.headers.get("content-type", "").split(";", 1)[0].lower()
    is_json_control_request = content_type == "application/json" and request.url.path != "/health"
    content_length = request.headers.get("content-length")
    if is_json_control_request and content_length is not None:
        try:
            body_size = int(content_length)
        except ValueError:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Invalid Content-Length"},
            )
        if body_size > settings.max_api_body_bytes:
            return JSONResponse(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                content={"detail": "Request body exceeds the allowed limit"},
            )
    if is_json_control_request:
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > settings.max_api_body_bytes:
                return JSONResponse(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    content={"detail": "Request body exceeds the allowed limit"},
                )
        request._body = bytes(body)
    return await call_next(request)


class CreateSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    locale: str = Field(default="zh-CN", min_length=1, max_length=16)
    timezone: str = Field(default="Asia/Shanghai", min_length=1, max_length=64)
    preference: dict[str, Any] = Field(default_factory=dict)


class PreferenceUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    base_context_version: int = Field(ge=0)
    patch: dict[str, Any] = Field(min_length=1)
    client_event_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        pattern=IDENTIFIER_PATTERN,
    )
    source_turn_id: UUID | None = None


class FinalizeTurnRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str = Field(min_length=1, max_length=20_000)
    client_event_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        pattern=IDENTIFIER_PATTERN,
    )


class RealtimeTokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    device_id: str = Field(min_length=1, max_length=128, pattern=IDENTIFIER_PATTERN)


class RealtimeTokenRedeemRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    device_id: str = Field(min_length=1, max_length=128, pattern=IDENTIFIER_PATTERN)
    token: str = Field(min_length=1, max_length=512)


class InterruptRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    turn_id: UUID
    playback_id: str | None = Field(default=None, max_length=128)
    reason: str = Field(default="user_interrupt", min_length=1, max_length=64)
    client_event_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
        pattern=IDENTIFIER_PATTERN,
    )
    occurred_at: datetime | None = None


class ResumeSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    after_event_seq: int = Field(ge=0)
    previous_connection_epoch: int = Field(ge=0)
    device_id: str = Field(min_length=1, max_length=128, pattern=IDENTIFIER_PATTERN)


def serialize_interrupt(result: InterruptResult) -> dict[str, object]:
    return {
        "accepted": True,
        "turn_id": str(result.turn_id),
        "turn_status": result.turn_status,
        "context_version": result.context_version,
        "cancelled_task_ids": [str(task_id) for task_id in result.cancelled_task_ids],
        "event_seq": result.event_seq,
    }


async def handle_interrupt(
    *,
    session_id: UUID,
    request: InterruptRequest,
    redis: Redis,
) -> dict[str, object]:
    client_event_id = request.client_event_id or str(uuid4())
    occurred_at = request.occurred_at or datetime.now(timezone.utc)
    async with async_session_factory() as database_session:
        result = await register_interrupt(
            database_session,
            session_id=session_id,
            turn_id=request.turn_id,
            playback_id=request.playback_id,
            reason=request.reason,
            occurred_at=occurred_at,
            client_event_id=client_event_id,
        )
    await write_task_cancel_keys(redis, result.cancelled_task_ids)
    interrupt_effective_seconds.observe(
        max(0, (datetime.now(timezone.utc) - occurred_at).total_seconds())
    )
    return serialize_interrupt(result)


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
        "target_itinerary_version": task.target_itinerary_version,
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


def serialize_tool_call(tool_call: ToolCall) -> dict[str, object]:
    return {
        "tool_call_id": str(tool_call.id),
        "task_id": str(tool_call.task_id),
        "session_id": str(tool_call.session_id),
        "context_version": tool_call.context_version,
        "target_preference_version": tool_call.target_preference_version,
        "tool_name": tool_call.tool_name,
        "status": tool_call.status,
        "request_hash": tool_call.request_hash,
        "input": tool_call.input,
        "output": tool_call.output,
        "provider_request_id": tool_call.provider_request_id,
        "error_code": tool_call.error_code,
        "error_message": tool_call.error_message,
        "latency_ms": tool_call.latency_ms,
        "started_at": tool_call.started_at,
        "finished_at": tool_call.finished_at,
        "created_at": tool_call.created_at,
    }


def serialize_itinerary(itinerary: Itinerary) -> dict[str, object]:
    return {
        "itinerary_id": str(itinerary.id),
        "session_id": str(itinerary.session_id),
        "version": itinerary.version,
        "context_version": itinerary.context_version,
        "preference_version": itinerary.preference_version,
        "status": itinerary.status,
        "content": itinerary.content,
        "budget_summary": itinerary.budget_summary,
        "source_task_ids": [str(task_id) for task_id in itinerary.source_task_ids],
        "created_at": itinerary.created_at,
        "confirmed_at": itinerary.confirmed_at,
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


@app.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/sessions", status_code=status.HTTP_201_CREATED)
async def create_session(
    request: CreateSessionRequest,
    identity: CurrentPrincipal,
    _: CreateSessionRateLimit,
) -> dict[str, object]:
    session_id = uuid4()
    validate_json_value(request.preference)

    async with async_session_factory() as database_session:
        async with database_session.begin():
            principal = await get_or_create_principal(database_session, identity)
            travel_session = TravelSession(
                id=session_id,
                user_id=principal.id,
                owner_principal_id=principal.id,
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
        tool_calls = list(
            (
                await database_session.scalars(
                    select(ToolCall)
                    .where(ToolCall.session_id == session_id)
                    .order_by(ToolCall.created_at)
                )
            ).all()
        )
        itinerary = await database_session.scalar(
            select(Itinerary).where(
                Itinerary.session_id == session_id,
                Itinerary.status == "confirmed",
            )
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
        "tool_calls": [serialize_tool_call(tool_call) for tool_call in tool_calls],
        "itinerary": (
            serialize_itinerary(itinerary)
            if itinerary is not None
            else {
                "status": "not_created",
                "version": 0,
                "context_version": travel_session.context_version,
            }
        ),
        "missed_events": [serialize_event(event) for event in events],
        "after_event_seq": after_event_seq,
    }


@app.get("/v1/sessions/{session_id}/snapshot")
async def get_session_snapshot(
    session_id: UUID,
    _: SessionOwner,
    __: ReadSessionRateLimit,
    after_event_seq: int = Query(default=0, ge=0),
) -> dict[str, object]:
    return await _load_session_snapshot(session_id, after_event_seq)


@app.post("/v1/sessions/{session_id}/realtime-token")
async def create_realtime_token(
    session_id: UUID,
    request: RealtimeTokenRequest,
    redis: RealtimeRedis,
    _: SessionOwner,
    __: WriteSessionRateLimit,
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
    _: SessionOwner,
    __: WriteSessionRateLimit,
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


@app.post("/v1/sessions/{session_id}/resume")
async def resume_session(
    session_id: UUID,
    request: ResumeSessionRequest,
    redis: RealtimeRedis,
    _: SessionOwner,
    __: WriteSessionRateLimit,
) -> dict[str, object]:
    async with async_session_factory() as database_session:
        try:
            grant = await issue_realtime_token(
                database_session,
                redis,
                session_id=session_id,
                device_id=request.device_id,
                expected_connection_epoch=request.previous_connection_epoch,
            )
        except LookupError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except RealtimeTokenEpochError as error:
            resume_conflicts_total.inc()
            raise HTTPException(status_code=409, detail=str(error)) from error
        except RedisError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Realtime token service unavailable",
            ) from error

    snapshot = await _load_session_snapshot(session_id, request.after_event_seq)
    return {
        "snapshot": snapshot,
        "token": grant.token,
        "device_id": grant.device_id,
        "connection_epoch": grant.connection_epoch,
        "expires_at": grant.expires_at,
        "provider_config": grant.provider_config,
    }


@app.patch("/v1/sessions/{session_id}/preferences")
async def update_preferences(
    session_id: UUID,
    request: PreferenceUpdateRequest,
    _: SessionOwner,
    __: WriteSessionRateLimit,
) -> dict[str, object]:
    client_event_id = request.client_event_id or str(uuid4())
    validate_json_value(request.patch)
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


@app.post("/v1/sessions/{session_id}/interrupt")
async def interrupt_session(
    session_id: UUID,
    request: InterruptRequest,
    redis: RealtimeRedis,
    _: SessionOwner,
    __: WriteSessionRateLimit,
) -> dict[str, object]:
    if request.reason not in ALLOWED_INTERRUPT_REASONS:
        raise HTTPException(status_code=422, detail="Invalid interrupt reason")
    try:
        return await handle_interrupt(
            session_id=session_id,
            request=request,
            redis=redis,
        )
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post(
    "/v1/sessions/{session_id}/turns",
    status_code=status.HTTP_202_ACCEPTED,
)
async def finalize_turn(
    session_id: UUID,
    request: FinalizeTurnRequest,
    _: SessionOwner,
    __: WriteSessionRateLimit,
) -> dict[str, object]:
    if not request.text.strip():
        raise HTTPException(status_code=422, detail="text must not be blank")
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
        "traceparent": current_traceparent(),
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
        "traceparent": current_traceparent(),
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
        identity = authenticate_websocket(websocket)
    except AuthenticationError:
        await websocket.close(code=4401)
        return
    if not await websocket_session_is_owned(session_id, identity):
        await websocket.close(code=1008, reason="Session not found")
        return
    origin = websocket.headers.get("origin")
    if origin is not None and not settings.is_trusted_origin(origin):
        await websocket.close(code=1008, reason="Origin not allowed")
        return
    redis = getattr(app.state, "websocket_redis", None)
    owns_redis = redis is None
    if redis is None:
        redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await enforce_rate_limit(
            redis,
            bucket="ws-connect",
            limit=settings.rate_limit_ws_connect,
            identity=identity,
        )
        await enforce_rate_limit(
            redis,
            bucket="ws-connect",
            limit=settings.rate_limit_ws_connect,
            identity=identity,
            session_id=session_id,
        )
    except HTTPException as error:
        await websocket.close(
            code=1013,
            reason=(
                "Try again later"
                if error.status_code == status.HTTP_429_TOO_MANY_REQUESTS
                else "Service unavailable"
            ),
        )
        return
    finally:
        if owns_redis:
            await redis.aclose()

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

    requested_protocols = {
        value.strip()
        for value in websocket.headers.get("sec-websocket-protocol", "").split(",")
    }
    await websocket.accept(
        subprotocol="livepilot" if "livepilot" in requested_protocols else None
    )
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
                try:
                    validate_json_value(message)
                except HTTPException as error:
                    await websocket.send_json(
                        {"type": "error", "detail": error.detail}
                    )
                    continue
                if not isinstance(message, dict):
                    await websocket.send_json(
                        {"type": "error", "detail": "Invalid event message"}
                    )
                    continue
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
                    if not isinstance(payload, dict):
                        await websocket.send_json(
                            {"type": "error", "detail": "Invalid turn event"}
                        )
                        continue
                    try:
                        request = FinalizeTurnRequest.model_validate(payload)
                    except ValueError:
                        await websocket.send_json(
                            {"type": "error", "detail": "Invalid turn event"}
                        )
                        continue
                    if not request.text.strip():
                        await websocket.send_json(
                            {"type": "error", "detail": "text is required"}
                        )
                        continue
                    client_event_id = request.client_event_id or str(uuid4())
                    with trace_scope(
                        "websocket.turn.finalized",
                        {"event_type": "turn.finalized"},
                        parent=extract_trace_context(message),
                    ):
                        async with async_session_factory() as database_session:
                            try:
                                finalized = await finalize_text_turn(
                                    database_session,
                                    session_id=session_id,
                                    text=request.text,
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
                elif message_type == "agent.interrupt":
                    nested_payload = message.get("payload")
                    payload = {
                        **message,
                        **(nested_payload if isinstance(nested_payload, dict) else {}),
                    }
                    try:
                        request = InterruptRequest.model_validate(
                            {
                                name: payload[name]
                                for name in (
                                    "turn_id",
                                    "playback_id",
                                    "reason",
                                    "client_event_id",
                                    "occurred_at",
                                )
                                if name in payload
                            }
                        )
                    except ValueError:
                        await websocket.send_json(
                            {"type": "error", "detail": "Invalid interrupt event"}
                        )
                        continue
                    if request.reason not in ALLOWED_INTERRUPT_REASONS:
                        await websocket.send_json(
                            {"type": "error", "detail": "Invalid interrupt reason"}
                        )
                        continue

                    redis = getattr(app.state, "websocket_redis", None)
                    owns_redis = redis is None
                    if redis is None:
                        redis = Redis.from_url(settings.redis_url, decode_responses=True)
                    try:
                        with trace_scope(
                            "websocket.agent.interrupt",
                            {"event_type": "agent.interrupt"},
                            parent=extract_trace_context(message),
                        ):
                            accepted = await handle_interrupt(
                                session_id=session_id,
                                request=request,
                                redis=redis,
                            )
                    except LookupError:
                        await websocket.send_json(
                            {"type": "error", "detail": "Session not found"}
                        )
                    except ValueError as error:
                        await websocket.send_json(
                            {"type": "error", "detail": "Invalid interrupt event"}
                        )
                    except RuntimeError as error:
                        await websocket.send_json(
                            {"type": "error", "detail": str(error)}
                        )
                    else:
                        await websocket.send_json(
                            {
                                "type": "agent.interrupt.accepted",
                                "session_id": str(session_id),
                                "event_seq": accepted["event_seq"],
                                "payload": accepted,
                            }
                        )
                    finally:
                        if owns_redis:
                            await redis.aclose()
                else:
                    await websocket.send_json(
                        {"type": "error", "detail": "Unsupported event type"}
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
async def create_smoke_test_task(
    identity: CurrentPrincipal,
    _: CreateSessionRateLimit,
) -> dict[str, str]:
    session_id = uuid4()
    task_id = uuid4()

    async with async_session_factory() as database_session:
        async with database_session.begin():
            principal = await get_or_create_principal(database_session, identity)
            database_session.add(
                TravelSession(
                    id=session_id,
                    user_id=principal.id,
                    owner_principal_id=principal.id,
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
async def get_task(
    task_id: UUID,
    identity: CurrentPrincipal,
) -> dict[str, object]:
    async with async_session_factory() as database_session:
        task = await database_session.scalar(
            select(Task)
            .join(TravelSession, Task.session_id == TravelSession.id)
            .join(Principal, TravelSession.owner_principal_id == Principal.id)
            .where(
                Task.id == task_id,
                Principal.issuer == identity.issuer,
                Principal.subject == identity.subject,
            )
        )

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": str(task.id),
        "status": task.status,
        "result": task.result,
        "error_message": task.error_message,
    }
