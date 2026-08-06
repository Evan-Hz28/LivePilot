import asyncio
import unittest
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from uuid import UUID

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.agent.service import build_context_packet
from app.agent.worker import process_compose_message, process_plan_message
from app.cancellation import task_cancel_key
from app.config import settings
from app.db import async_session_factory, engine
from app.main import app, get_realtime_redis
from app.models import EventOutbox, Preference, Task, TravelSession, Turn
from app.worker import process_task


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(
        self,
        key: str,
        value: str,
        *,
        ex: int,
        nx: bool = False,
    ) -> bool | None:
        del ex
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def exists(self, key: str) -> int:
        return int(key in self.values)

    async def getdel(self, key: str) -> str | None:
        return self.values.pop(key, None)


class Stage7InterruptTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        try:
            await asyncio.wait_for(self._database_is_available(), timeout=3)
        except Exception as error:  # pragma: no cover - environment-dependent
            self.skipTest(f"database unavailable: {error}")

        self.redis = FakeRedis()

        async def override() -> AsyncIterator[FakeRedis]:
            yield self.redis

        app.dependency_overrides[get_realtime_redis] = override
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        )
        self.session_ids: list[UUID] = []

    async def asyncTearDown(self) -> None:
        app.dependency_overrides.pop(get_realtime_redis, None)
        await self.client.aclose()
        if self.session_ids:
            async with async_session_factory() as database_session:
                await database_session.execute(
                    delete(EventOutbox).where(
                        EventOutbox.session_id.in_(self.session_ids)
                    )
                )
                await database_session.execute(
                    delete(Preference).where(
                        Preference.session_id.in_(self.session_ids)
                    )
                )
                await database_session.execute(
                    delete(Turn).where(Turn.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(Task).where(Task.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(TravelSession).where(
                        TravelSession.id.in_(self.session_ids)
                    )
                )
                await database_session.commit()
        await engine.dispose()

    async def _database_is_available(self) -> None:
        probe_engine = create_async_engine(
            settings.database_url,
            connect_args={"timeout": 2},
        )
        try:
            async with probe_engine.connect() as connection:
                await connection.scalar(select(EventOutbox.id).limit(1))
        finally:
            await probe_engine.dispose()

    async def _create_session(self) -> UUID:
        response = await self.client.post(
            "/v1/sessions",
            json={"preference": {"destination": "Kyoto"}},
        )
        self.assertEqual(response.status_code, 201)
        session_id = UUID(response.json()["session_id"])
        self.session_ids.append(session_id)
        return session_id

    async def _create_task(self, session_id: UUID) -> tuple[UUID, UUID]:
        finalized = await self.client.post(
            f"/v1/sessions/{session_id}/turns",
            json={"text": "安排文化旅行", "client_event_id": "stage7-turn"},
        )
        self.assertEqual(finalized.status_code, 202)
        turn_id = UUID(finalized.json()["turn_id"])
        async with async_session_factory() as database_session:
            packet = await build_context_packet(
                database_session,
                session_id=session_id,
                turn_id=turn_id,
            )
        task_ids = await process_plan_message(
            {"packet": packet.model_dump_json()}, redis=None
        )
        self.assertEqual(len(task_ids), 1)
        return turn_id, task_ids[0]

    def _interrupt_payload(self, turn_id: UUID) -> dict[str, str]:
        return {
            "turn_id": str(turn_id),
            "playback_id": "playback-1",
            "reason": "voice_stopped",
            "client_event_id": "stage7-interrupt-1",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }

    async def test_interrupt_is_idempotent_cancels_queued_work_and_blocks_reply(
        self,
    ) -> None:
        session_id = await self._create_session()
        turn_id, task_id = await self._create_task(session_id)
        payload = self._interrupt_payload(turn_id)

        first = await self.client.post(
            f"/v1/sessions/{session_id}/interrupt",
            json=payload,
        )
        second = await self.client.post(
            f"/v1/sessions/{session_id}/interrupt",
            json=payload,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json(), first.json())
        self.assertEqual(first.json()["context_version"], 2)
        self.assertEqual(first.json()["turn_status"], "interrupted")
        self.assertEqual(first.json()["cancelled_task_ids"], [str(task_id)])
        self.assertIn(task_cancel_key(task_id), self.redis.values)

        await process_task(task_id, self.redis)
        composed = await process_compose_message(
            {
                "task_id": str(task_id),
                "session_id": str(session_id),
                "turn_id": str(turn_id),
                "context_version": "1",
            }
        )
        self.assertFalse(composed)

        snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        self.assertEqual(snapshot["turns"][0]["status"], "interrupted")
        self.assertEqual(snapshot["tasks"][0]["status"], "cancelled")
        self.assertEqual(len(snapshot["turns"]), 1)
        self.assertEqual(snapshot["missed_events"][-1]["event_type"], "task.cancelled")
        self.assertIn(
            "agent.interrupt.accepted",
            [event["event_type"] for event in snapshot["missed_events"]],
        )

    async def test_interrupt_marks_running_work_cancel_requested(self) -> None:
        session_id = await self._create_session()
        turn_id, task_id = await self._create_task(session_id)
        async with async_session_factory() as database_session:
            async with database_session.begin():
                task = await database_session.get(Task, task_id)
                assert task is not None
                task.status = "running"
                task.started_at = datetime.now(timezone.utc)

        response = await self.client.post(
            f"/v1/sessions/{session_id}/interrupt",
            json=self._interrupt_payload(turn_id),
        )

        self.assertEqual(response.status_code, 200)
        snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        self.assertEqual(snapshot["tasks"][0]["status"], "cancel_requested")
        self.assertEqual(
            snapshot["missed_events"][-2]["event_type"],
            "task.cancel_requested",
        )

    async def test_resume_issues_a_new_epoch_and_rejects_the_old_token(self) -> None:
        session_id = await self._create_session()
        initial = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token",
            json={"device_id": "device-a"},
        )
        self.assertEqual(initial.status_code, 200)
        self.assertEqual(initial.json()["connection_epoch"], 1)

        resumed = await self.client.post(
            f"/v1/sessions/{session_id}/resume",
            json={
                "after_event_seq": 1,
                "previous_connection_epoch": 1,
                "device_id": "device-a",
            },
        )
        self.assertEqual(resumed.status_code, 200)
        resumed_body = resumed.json()
        self.assertEqual(resumed_body["connection_epoch"], 2)
        self.assertEqual(
            resumed_body["snapshot"]["session"]["realtime_connection_epoch"],
            2,
        )

        stale = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            json={"device_id": "device-a", "token": initial.json()["token"]},
        )
        self.assertEqual(stale.status_code, 401)

        conflict = await self.client.post(
            f"/v1/sessions/{session_id}/resume",
            json={
                "after_event_seq": 1,
                "previous_connection_epoch": 1,
                "device_id": "device-a",
            },
        )
        self.assertEqual(conflict.status_code, 409)
