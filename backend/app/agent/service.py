from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import EventOutbox, Preference, Task, TravelSession, Turn

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
    *, session_id: UUID, context_version: int, task_type: str, payload: dict
) -> str:
    canonical_payload = json.dumps(payload, ensure_ascii=True, sort_keys=True)
    source = f"{session_id}:{context_version}:{task_type}:{canonical_payload}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


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
        if (
            session is None
            or preference is None
            or session.context_version != packet.context_version
            or preference.version != packet.preference_version
            or decision.context_version != packet.context_version
            or decision.preference_version != packet.preference_version
        ):
            return []

        for plan in decision.tasks:
            key = task_idempotency_key(
                session_id=packet.session_id,
                context_version=packet.context_version,
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
) -> ReplyContext | None:
    """Create one recoverable Agent reply from valid Mock task results."""
    async with database_session.begin():
        session = await database_session.scalar(
            select(TravelSession)
            .where(TravelSession.id == session_id)
            .with_for_update()
        )
        if session is None or session.context_version != context_version:
            return None
        preference = await database_session.scalar(
            select(Preference).where(
                Preference.session_id == session_id,
                Preference.status == "active",
            )
        )
        if preference is None:
            return None

        existing = await database_session.scalar(
            select(EventOutbox).where(
                EventOutbox.dedupe_key == f"agent.reply:{turn_id}:{context_version}"
            )
        )
        if existing is not None:
            return ReplyContext.model_validate(existing.payload["reply_context"])

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
            return None

        results = [task.result or {} for task in tasks]
        destination = str(preference.payload.get("destination") or "当前目的地")
        reply = ReplyContext(
            message=f"已根据当前偏好整理 {destination} 的 Mock 旅行建议。",
            context_version=context_version,
            preference_version=preference.version,
            source_task_ids=[task.id for task in tasks],
            tool_results=results,
        )
        next_sequence = await database_session.scalar(
            select(func.coalesce(func.max(Turn.sequence_no), 0) + 1).where(
                Turn.session_id == session_id
            )
        )
        now = datetime.now(timezone.utc)
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
        return reply


def event_to_redis_fields(event: EventOutbox) -> dict[str, str]:
    return {key: str(value) for key, value in _event_payload(event).items()}
