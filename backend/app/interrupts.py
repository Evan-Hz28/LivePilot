from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import EventOutbox, Task, TravelSession, Turn


@dataclass(frozen=True)
class InterruptResult:
    turn_id: UUID
    context_version: int
    event_seq: int
    cancelled_task_ids: tuple[UUID, ...]
    turn_status: str


async def _append_event(
    database_session: AsyncSession,
    *,
    session: TravelSession,
    event_type: str,
    payload: dict[str, object],
    dedupe_key: str,
) -> int:
    session.last_event_seq += 1
    database_session.add(
        EventOutbox(
            session_id=session.id,
            event_seq=session.last_event_seq,
            event_type=event_type,
            payload=payload,
            dedupe_key=dedupe_key,
        )
    )
    return session.last_event_seq


async def register_interrupt(
    database_session: AsyncSession,
    *,
    session_id: UUID,
    turn_id: UUID,
    playback_id: str | None,
    reason: str,
    occurred_at: datetime,
    client_event_id: str,
) -> InterruptResult:
    """Persist one interrupt and invalidate work tied to its current turn."""
    dedupe_key = f"agent.interrupt:{session_id}:{client_event_id}"

    async with database_session.begin():
        existing = await database_session.scalar(
            select(EventOutbox).where(EventOutbox.dedupe_key == dedupe_key)
        )
        if existing is not None:
            payload = existing.payload
            return InterruptResult(
                turn_id=UUID(str(payload["turn_id"])),
                context_version=int(payload["context_version"]),
                event_seq=existing.event_seq,
                cancelled_task_ids=tuple(
                    UUID(str(task_id))
                    for task_id in payload["cancelled_task_ids"]
                ),
                turn_status=str(payload["turn_status"]),
            )

        session = await database_session.scalar(
            select(TravelSession)
            .where(TravelSession.id == session_id)
            .with_for_update()
        )
        if session is None:
            raise LookupError("Session not found")

        turn = await database_session.scalar(
            select(Turn)
            .where(Turn.id == turn_id, Turn.session_id == session_id)
            .with_for_update()
        )
        if turn is None:
            raise ValueError("turn_id does not belong to the session")
        if turn.context_version != session.context_version:
            raise RuntimeError("Turn is no longer current")

        tasks = list(
            (
                await database_session.scalars(
                    select(Task)
                    .where(
                        Task.session_id == session_id,
                        Task.turn_id == turn_id,
                        Task.context_version == session.context_version,
                        Task.status.in_(("queued", "running")),
                    )
                    .with_for_update()
                )
            ).all()
        )

        session.context_version += 1
        turn.status = "interrupted"
        turn.interrupt_reason = reason
        cancelled_task_ids: list[UUID] = []
        for task in tasks:
            cancelled_task_ids.append(task.id)
            task.status = "cancel_requested"
            await _append_event(
                database_session,
                session=session,
                event_type="task.cancel_requested",
                payload={
                    "task_id": str(task.id),
                    "turn_id": str(turn_id),
                    "context_version": task.context_version,
                    "reason": reason,
                },
                dedupe_key=f"task.cancel_requested:{task.id}",
            )

        event_seq = await _append_event(
            database_session,
            session=session,
            event_type="agent.interrupt.accepted",
            payload={
                "turn_id": str(turn_id),
                "turn_status": turn.status,
                "context_version": session.context_version,
                "cancelled_task_ids": [str(task_id) for task_id in cancelled_task_ids],
                "playback_id": playback_id,
                "reason": reason,
                "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(),
                "client_event_id": client_event_id,
            },
            dedupe_key=dedupe_key,
        )
        return InterruptResult(
            turn_id=turn_id,
            context_version=session.context_version,
            event_seq=event_seq,
            cancelled_task_ids=tuple(cancelled_task_ids),
            turn_status=turn.status,
        )
