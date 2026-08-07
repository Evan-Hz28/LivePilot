from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.input_validation import validate_json_value
from app.metrics import tasks_discarded_total
from app.models import (
    EventOutbox,
    Itinerary,
    Preference,
    Task,
    ToolCall,
    TravelSession,
    Turn,
)

from .schemas import (
    AgentDecision,
    ContextPacket,
    ContextTurn,
    ReplyContext,
    TaskPlan,
)

PLAN_STREAM = "agent.plan"
COMPOSE_STREAM = "agent.compose"
TRAVEL_TASK_STREAM = "travel.tasks"
# Explicit aliases keep stream names discoverable to future WebSocket/publisher code.
AGENT_PLAN_STREAM = PLAN_STREAM
AGENT_COMPOSE_STREAM = COMPOSE_STREAM


@dataclass(frozen=True)
class FinalizedTurn:
    turn_id: UUID
    context_version: int
    preference_version: int
    event_seq: int
    client_event_id: str


@dataclass(frozen=True)
class ComposeResult:
    reply: ReplyContext | None
    itinerary_conflict: bool = False


def _event_payload(event: EventOutbox) -> dict[str, Any]:
    return {
        "session_id": str(event.session_id),
        "event_seq": event.event_seq,
        **event.payload,
    }


async def finalize_text_turn(
    database_session: AsyncSession,
    *,
    session_id: UUID,
    text: str,
    client_event_id: str,
) -> FinalizedTurn:
    """Persist a user text turn and advance the context in one transaction."""
    dedupe_key = f"turn.finalized:{session_id}:{client_event_id}"

    async with database_session.begin():
        existing = await database_session.scalar(
            select(EventOutbox).where(EventOutbox.dedupe_key == dedupe_key)
        )
        if existing is not None:
            return FinalizedTurn(
                turn_id=UUID(existing.payload["turn_id"]),
                context_version=int(existing.payload["context_version"]),
                preference_version=int(existing.payload["preference_version"]),
                event_seq=existing.event_seq,
                client_event_id=client_event_id,
            )

        session = await database_session.scalar(
            select(TravelSession)
            .where(TravelSession.id == session_id)
            .with_for_update()
        )
        if session is None:
            raise LookupError("Session not found")

        preference = await database_session.scalar(
            select(Preference).where(
                Preference.session_id == session_id,
                Preference.status == "active",
            )
        )
        if preference is None:
            raise RuntimeError("Session has no active preference")

        next_sequence = await database_session.scalar(
            select(func.coalesce(func.max(Turn.sequence_no), 0) + 1).where(
                Turn.session_id == session_id
            )
        )
        session.context_version += 1
        session.last_event_seq += 1
        turn = Turn(
            session_id=session_id,
            sequence_no=int(next_sequence),
            kind="user_text",
            status="user_final",
            context_version=session.context_version,
            content={"text": text},
            finalized_at=datetime.now(timezone.utc),
        )
        database_session.add(turn)
        await database_session.flush()
        database_session.add(
            EventOutbox(
                session_id=session_id,
                event_seq=session.last_event_seq,
                event_type="turn.finalized",
                payload={
                    "turn_id": str(turn.id),
                    "context_version": session.context_version,
                    "preference_version": preference.version,
                    "kind": turn.kind,
                },
                dedupe_key=dedupe_key,
            )
        )
        return FinalizedTurn(
            turn_id=turn.id,
            context_version=session.context_version,
            preference_version=preference.version,
            event_seq=session.last_event_seq,
            client_event_id=client_event_id,
        )


async def build_context_packet(
    database_session: AsyncSession,
    *,
    session_id: UUID,
    turn_id: UUID,
    context_version: int | None = None,
    preference_version: int | None = None,
) -> ContextPacket:
    session = await database_session.get(TravelSession, session_id)
    turn = await database_session.scalar(
        select(Turn).where(Turn.id == turn_id, Turn.session_id == session_id)
    )
    preference_query = select(Preference).where(Preference.session_id == session_id)
    if preference_version is None:
        preference_query = preference_query.where(Preference.status == "active")
    else:
        preference_query = preference_query.where(Preference.version == preference_version)
    preference = await database_session.scalar(preference_query)
    itinerary = await database_session.scalar(
        select(Itinerary).where(
            Itinerary.session_id == session_id,
            Itinerary.status == "confirmed",
        )
    )
    if session is None or turn is None or preference is None:
        raise LookupError("Session context not found")

    recent = list(
        (
            await database_session.scalars(
                select(Turn)
                .where(
                    Turn.session_id == session_id,
                    Turn.status.in_(["user_final", "completed"]),
                    Turn.sequence_no <= turn.sequence_no,
                )
                .order_by(Turn.sequence_no.desc())
                .limit(6)
            )
        ).all()
    )
    recent.reverse()
    user_text = (turn.content or {}).get("text", "")
    return ContextPacket(
        session_id=session_id,
        turn_id=turn_id,
        context_version=(
            session.context_version if context_version is None else context_version
        ),
        preference_version=preference.version,
        itinerary_version=itinerary.version if itinerary is not None else 0,
        preference=preference.payload,
        recent_turns=[
            ContextTurn(
                turn_id=item.id,
                sequence_no=item.sequence_no,
                kind=item.kind,
                status=item.status,
                context_version=item.context_version,
                content=item.content,
            )
            for item in recent
        ],
        user_text=user_text,
    )


def decide(packet: ContextPacket) -> AgentDecision:
    """Return a stable Mock travel lookup for every finalized text turn."""
    destination = str(packet.preference.get("destination") or "未指定目的地")
    return AgentDecision(
        context_version=packet.context_version,
        preference_version=packet.preference_version,
        tasks=[
            TaskPlan(
                task_type="mock_travel",
                payload={
                    "destination": destination,
                    "query": packet.user_text,
                    "mock": True,
                },
            )
        ],
    )


def task_idempotency_key(
    *,
    session_id: UUID,
    context_version: int,
    target_itinerary_version: int,
    task_type: str,
    payload: dict,
) -> str:
    canonical_payload = json.dumps(payload, ensure_ascii=True, sort_keys=True)
    source = (
        f"{session_id}:{context_version}:{target_itinerary_version}:"
        f"{task_type}:{canonical_payload}"
    )
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def build_itinerary_content(
    *,
    destination: str,
    context_version: int,
    tool_calls,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for tool_call in tool_calls:
        recommendations = (tool_call.output or {}).get("recommendations", [])
        if not isinstance(recommendations, list):
            continue
        for recommendation in recommendations:
            if not isinstance(recommendation, dict):
                continue
            items.append(
                {
                    "name": str(recommendation.get("name") or "未命名推荐"),
                    "type": str(recommendation.get("category") or "activity"),
                    "source_tool_call_ids": [str(tool_call.id)],
                }
            )
    return {
        "schema_version": 1,
        "destination": destination,
        "days": [{"day": 1, "items": items}],
        "generated_from_context_version": context_version,
    }


async def create_tasks_for_decision(
    database_session: AsyncSession,
    *,
    packet: ContextPacket,
    decision: AgentDecision,
) -> list[Task]:
    """Create fixed-version tasks; replaying the same plan returns existing rows."""
    created: list[Task] = []
    async with database_session.begin():
        session = await database_session.scalar(
            select(TravelSession)
            .where(TravelSession.id == packet.session_id)
            .with_for_update()
        )
        preference = await database_session.scalar(
            select(Preference).where(
                Preference.session_id == packet.session_id,
                Preference.status == "active",
            )
        )
        itinerary = await database_session.scalar(
            select(Itinerary).where(
                Itinerary.session_id == packet.session_id,
                Itinerary.status == "confirmed",
            )
        )
        itinerary_version = itinerary.version if itinerary is not None else 0
        if (
            session is None
            or preference is None
            or session.context_version != packet.context_version
            or preference.version != packet.preference_version
            or itinerary_version != packet.itinerary_version
            or decision.context_version != packet.context_version
            or decision.preference_version != packet.preference_version
        ):
            return []

        for plan in decision.tasks:
            validate_json_value(plan.payload)
            key = task_idempotency_key(
                session_id=packet.session_id,
                context_version=packet.context_version,
                target_itinerary_version=packet.itinerary_version,
                task_type=plan.task_type,
                payload=plan.payload,
            )
            existing = await database_session.scalar(
                select(Task).where(
                    Task.session_id == packet.session_id,
                    Task.idempotency_key == key,
                )
            )
            if existing is not None:
                created.append(existing)
                continue

            task = Task(
                session_id=packet.session_id,
                turn_id=packet.turn_id,
                context_version=packet.context_version,
                target_preference_version=packet.preference_version,
                target_itinerary_version=packet.itinerary_version,
                task_type=plan.task_type,
                idempotency_key=key,
                deadline_at=datetime.now(timezone.utc) + timedelta(seconds=30),
                payload=plan.payload,
            )
            database_session.add(task)
            await database_session.flush()
            session.last_event_seq += 1
            database_session.add(
                EventOutbox(
                    session_id=packet.session_id,
                    event_seq=session.last_event_seq,
                    event_type="task.queued",
                    payload={
                        "task_id": str(task.id),
                        "turn_id": str(packet.turn_id),
                        "context_version": packet.context_version,
                        "target_preference_version": packet.preference_version,
                        "target_itinerary_version": packet.itinerary_version,
                        "task_type": task.task_type,
                    },
                    dedupe_key=f"task.queued:{task.id}",
                )
            )
            created.append(task)
    return created


async def compose_reply(
    database_session: AsyncSession,
    *,
    session_id: UUID,
    turn_id: UUID,
    context_version: int,
) -> ComposeResult:
    """Create one recoverable Agent reply from valid Mock task results."""
    async with database_session.begin():
        session = await database_session.scalar(
            select(TravelSession)
            .where(TravelSession.id == session_id)
            .with_for_update()
        )
        if session is None or session.context_version != context_version:
            return ComposeResult(reply=None)
        preference = await database_session.scalar(
            select(Preference).where(
                Preference.session_id == session_id,
                Preference.status == "active",
            )
        )
        if preference is None:
            return ComposeResult(reply=None)

        existing = await database_session.scalar(
            select(EventOutbox).where(
                EventOutbox.dedupe_key == f"agent.reply:{turn_id}:{context_version}"
            )
        )
        if existing is not None:
            return ComposeResult(
                reply=ReplyContext.model_validate(existing.payload["reply_context"])
            )

        tasks = list(
            (
                await database_session.scalars(
                    select(Task).where(
                        Task.session_id == session_id,
                        Task.turn_id == turn_id,
                        Task.context_version == context_version,
                        Task.target_preference_version == preference.version,
                        Task.status == "succeeded",
                    )
                )
            ).all()
        )
        if not tasks:
            return ComposeResult(reply=None)

        target_itinerary_versions = {
            task.target_itinerary_version for task in tasks
        }
        if None in target_itinerary_versions or len(target_itinerary_versions) != 1:
            return ComposeResult(reply=None)

        tool_calls = list(
            (
                await database_session.scalars(
                    select(ToolCall)
                    .where(
                        ToolCall.task_id.in_([task.id for task in tasks]),
                        ToolCall.session_id == session_id,
                        ToolCall.context_version == context_version,
                        ToolCall.target_preference_version == preference.version,
                        ToolCall.status == "succeeded",
                    )
                    .order_by(ToolCall.created_at)
                    .with_for_update()
                )
            ).all()
        )
        if len(tool_calls) != len(tasks):
            return ComposeResult(reply=None)

        expected_itinerary_version = target_itinerary_versions.pop()
        confirmed_itinerary = await database_session.scalar(
            select(Itinerary)
            .where(
                Itinerary.session_id == session_id,
                Itinerary.status == "confirmed",
            )
            .with_for_update()
        )
        current_itinerary_version = (
            confirmed_itinerary.version if confirmed_itinerary is not None else 0
        )
        if current_itinerary_version != expected_itinerary_version:
            for task in tasks:
                task.status = "discarded"
                tasks_discarded_total.inc()
                session.last_event_seq += 1
                database_session.add(
                    EventOutbox(
                        session_id=session_id,
                        event_seq=session.last_event_seq,
                        event_type="task.result.discarded",
                        payload={
                            "task_id": str(task.id),
                            "turn_id": str(task.turn_id) if task.turn_id else None,
                            "context_version": task.context_version,
                            "reason": "itinerary_version_conflict",
                        },
                        dedupe_key=f"task.discarded:{task.id}",
                    )
                )
            return ComposeResult(reply=None, itinerary_conflict=True)

        results = [tool_call.output or {} for tool_call in tool_calls]
        destination = str(preference.payload.get("destination") or "当前目的地")
        now = datetime.now(timezone.utc)
        if confirmed_itinerary is not None:
            confirmed_itinerary.status = "superseded"

        itinerary = Itinerary(
            session_id=session_id,
            version=expected_itinerary_version + 1,
            context_version=context_version,
            preference_version=preference.version,
            status="confirmed",
            content=build_itinerary_content(
                destination=destination,
                context_version=context_version,
                tool_calls=tool_calls,
            ),
            budget_summary={
                "currency": "N/A",
                "estimated_total": 0,
                "is_mock": True,
            },
            source_task_ids=[task.id for task in tasks],
            confirmed_at=now,
        )
        database_session.add(itinerary)
        await database_session.flush()
        session.last_event_seq += 1
        database_session.add(
            EventOutbox(
                session_id=session_id,
                event_seq=session.last_event_seq,
                event_type="itinerary.confirmed",
                payload={
                    "itinerary_id": str(itinerary.id),
                    "version": itinerary.version,
                    "context_version": context_version,
                    "preference_version": preference.version,
                    "source_task_ids": [str(task.id) for task in tasks],
                    "source_tool_call_ids": [str(tool_call.id) for tool_call in tool_calls],
                },
                dedupe_key=f"itinerary.confirmed:{turn_id}:{context_version}",
            )
        )
        reply = ReplyContext(
            message=f"已根据当前偏好整理 {destination} 的版本化旅行建议。",
            context_version=context_version,
            preference_version=preference.version,
            source_task_ids=[task.id for task in tasks],
            tool_results=results,
            itinerary_version=itinerary.version,
            source_tool_call_ids=[tool_call.id for tool_call in tool_calls],
        )
        next_sequence = await database_session.scalar(
            select(func.coalesce(func.max(Turn.sequence_no), 0) + 1).where(
                Turn.session_id == session_id
            )
        )
        reply_turn = Turn(
            session_id=session_id,
            sequence_no=int(next_sequence),
            kind="agent_reply",
            status="completed",
            context_version=context_version,
            parent_turn_id=turn_id,
            content={"reply_context": reply.model_dump(mode="json")},
            finalized_at=now,
            completed_at=now,
        )
        database_session.add(reply_turn)
        await database_session.flush()
        session.last_event_seq += 1
        database_session.add(
            EventOutbox(
                session_id=session_id,
                event_seq=session.last_event_seq,
                event_type="agent.reply.created",
                payload={
                    "turn_id": str(reply_turn.id),
                    "parent_turn_id": str(turn_id),
                    "context_version": context_version,
                    "reply_context": reply.model_dump(mode="json"),
                },
                dedupe_key=f"agent.reply:{turn_id}:{context_version}",
            )
        )
        return ComposeResult(reply=reply)


def event_to_redis_fields(event: EventOutbox) -> dict[str, str]:
    return {key: str(value) for key, value in _event_payload(event).items()}
