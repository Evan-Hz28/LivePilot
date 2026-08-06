import asyncio
import unittest
from uuid import UUID

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.agent.service import build_context_packet
from app.agent.worker import process_compose_message, process_plan_message
from app.config import settings
from app.db import async_session_factory, engine
from app.main import app
from app.models import EventOutbox, Preference, Task, TravelSession, Turn
from app.worker import process_task


class Stage4AgentTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        try:
            await asyncio.wait_for(self._database_is_available(), timeout=3)
        except Exception as error:  # pragma: no cover - environment-dependent
            self.skipTest(f"database unavailable: {error}")
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        )
        self.session_ids: list[UUID] = []

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        if self.session_ids:
            async with async_session_factory() as database_session:
                await database_session.execute(
                    delete(EventOutbox).where(EventOutbox.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(Preference).where(Preference.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(Turn).where(Turn.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(Task).where(Task.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(TravelSession).where(TravelSession.id.in_(self.session_ids))
                )
                await database_session.commit()
        await engine.dispose()

    async def _database_is_available(self) -> None:
        probe_engine = create_async_engine(settings.database_url, connect_args={"timeout": 2})
        try:
            async with probe_engine.connect() as connection:
                await connection.scalar(select(EventOutbox.id).limit(1))
        finally:
            await probe_engine.dispose()

    async def _create_session(self, preference: dict) -> dict:
        response = await self.client.post("/v1/sessions", json={"preference": preference})
        self.assertEqual(response.status_code, 201)
        body = response.json()
        self.session_ids.append(UUID(body["session_id"]))
        return body

    async def _plan_task(self, session_id: UUID, turn_id: UUID) -> UUID:
        async with async_session_factory() as database_session:
            packet = await build_context_packet(
                database_session, session_id=session_id, turn_id=turn_id
            )
        task_ids = await process_plan_message(
            {"packet": packet.model_dump_json()}, redis=None
        )
        self.assertEqual(len(task_ids), 1)
        return task_ids[0]

    async def test_text_turn_mock_chain_is_idempotent_and_readable(self) -> None:
        created = await self._create_session({"destination": "Kyoto"})
        session_id = UUID(created["session_id"])
        response = await self.client.post(
            f"/v1/sessions/{session_id}/turns",
            json={"text": "安排两天文化旅行", "client_event_id": "turn-1"},
        )
        self.assertEqual(response.status_code, 202)
        finalized = response.json()
        self.assertEqual(finalized["context_version"], 1)
        replay = await self.client.post(
            f"/v1/sessions/{session_id}/turns",
            json={"text": "安排两天文化旅行", "client_event_id": "turn-1"},
        )
        self.assertEqual(replay.status_code, 202)
        self.assertEqual(replay.json(), finalized)

        task_id = await self._plan_task(session_id, UUID(finalized["turn_id"]))
        task_ids_again = await self._plan_task(session_id, UUID(finalized["turn_id"]))
        self.assertEqual(task_ids_again, task_id)

        await process_task(task_id)
        composed = await process_compose_message(
            {
                "task_id": str(task_id),
                "session_id": str(session_id),
                "turn_id": finalized["turn_id"],
                "context_version": "1",
            }
        )
        self.assertTrue(composed)

        snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        self.assertEqual([turn["kind"] for turn in snapshot["turns"]], ["user_text", "agent_reply"])
        self.assertEqual(snapshot["tasks"][0]["status"], "succeeded")
        self.assertEqual(snapshot["turns"][1]["content"]["reply_context"]["context_version"], 1)

    async def test_old_context_task_is_discarded_without_reply(self) -> None:
        created = await self._create_session({"destination": "Osaka"})
        session_id = UUID(created["session_id"])
        finalized = (
            await self.client.post(
                f"/v1/sessions/{session_id}/turns",
                json={"text": "找安静的景点", "client_event_id": "turn-2"},
            )
        ).json()
        task_id = await self._plan_task(session_id, UUID(finalized["turn_id"]))

        updated = await self.client.patch(
            f"/v1/sessions/{session_id}/preferences",
            json={
                "base_context_version": 1,
                "client_event_id": "pref-2",
                "patch": {"crowd_tolerance": "low"},
            },
        )
        self.assertEqual(updated.status_code, 200)
        await process_task(task_id)

        snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        self.assertEqual(snapshot["tasks"][0]["status"], "discarded")
        self.assertEqual(len(snapshot["turns"]), 1)
