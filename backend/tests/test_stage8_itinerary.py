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
from app.config import settings
from app.db import async_session_factory, engine
from app.main import app, get_realtime_redis
from app.models import (
    EventOutbox,
    Itinerary,
    Preference,
    Task,
    ToolCall,
    TravelSession,
    Turn,
)
from app.worker import process_task
from tests.auth import auth_headers
from tests.fakes import FakeRedis


class Stage8ItineraryTests(unittest.IsolatedAsyncioTestCase):
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
            headers=auth_headers(),
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
                    delete(ToolCall).where(ToolCall.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(Itinerary).where(
                        Itinerary.session_id.in_(self.session_ids)
                    )
                )
                await database_session.execute(
                    delete(Task).where(Task.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(Turn).where(Turn.session_id.in_(self.session_ids))
                )
                await database_session.execute(
                    delete(Preference).where(
                        Preference.session_id.in_(self.session_ids)
                    )
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

    async def _create_task(self, session_id: UUID, client_event_id: str) -> tuple[UUID, UUID]:
        finalized = await self.client.post(
            f"/v1/sessions/{session_id}/turns",
            json={"text": "安排文化旅行", "client_event_id": client_event_id},
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

    async def _compose(self, session_id: UUID, turn_id: UUID, context_version: int) -> bool:
        return await process_compose_message(
            {
                "session_id": str(session_id),
                "turn_id": str(turn_id),
                "context_version": str(context_version),
            }
        )

    async def test_tool_result_creates_a_versioned_recoverable_itinerary(self) -> None:
        session_id = await self._create_session()
        turn_id, task_id = await self._create_task(session_id, "stage8-turn-1")

        await process_task(task_id)
        self.assertTrue(await self._compose(session_id, turn_id, 1))

        snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        self.assertEqual(snapshot["tasks"][0]["target_itinerary_version"], 0)
        self.assertEqual(snapshot["tasks"][0]["status"], "succeeded")
        self.assertEqual(len(snapshot["tool_calls"]), 1)
        tool_call = snapshot["tool_calls"][0]
        self.assertEqual(tool_call["task_id"], str(task_id))
        self.assertEqual(tool_call["session_id"], str(session_id))
        self.assertEqual(tool_call["context_version"], 1)
        self.assertEqual(tool_call["target_preference_version"], 1)
        self.assertEqual(tool_call["status"], "succeeded")
        self.assertEqual(snapshot["itinerary"]["status"], "confirmed")
        self.assertEqual(snapshot["itinerary"]["version"], 1)
        self.assertEqual(snapshot["itinerary"]["content"]["destination"], "Kyoto")
        self.assertEqual(
            snapshot["itinerary"]["source_task_ids"], [str(task_id)]
        )
        self.assertIn(
            "itinerary.confirmed",
            [event["event_type"] for event in snapshot["missed_events"]],
        )

    async def test_stale_tool_audit_is_retained_without_creating_an_itinerary(self) -> None:
        session_id = await self._create_session()
        turn_id, task_id = await self._create_task(session_id, "stage8-turn-2")

        await process_task(task_id)
        updated = await self.client.patch(
            f"/v1/sessions/{session_id}/preferences",
            json={
                "base_context_version": 1,
                "client_event_id": "stage8-preference-2",
                "patch": {"crowd_tolerance": "low"},
            },
        )
        self.assertEqual(updated.status_code, 200)
        self.assertFalse(await self._compose(session_id, turn_id, 1))

        snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        self.assertEqual(snapshot["tasks"][0]["status"], "succeeded")
        self.assertEqual(snapshot["tool_calls"][0]["status"], "succeeded")
        self.assertEqual(snapshot["itinerary"]["status"], "not_created")

    async def test_itinerary_version_conflict_does_not_overwrite_current_result(self) -> None:
        session_id = await self._create_session()
        first_turn_id, first_task_id = await self._create_task(
            session_id,
            "stage8-turn-3a",
        )
        await process_task(first_task_id)
        self.assertTrue(await self._compose(session_id, first_turn_id, 1))

        second_turn_id, second_task_id = await self._create_task(
            session_id,
            "stage8-turn-3b",
        )
        await process_task(second_task_id)
        async with async_session_factory() as database_session:
            async with database_session.begin():
                confirmed = await database_session.scalar(
                    select(Itinerary)
                    .where(
                        Itinerary.session_id == session_id,
                        Itinerary.status == "confirmed",
                    )
                    .with_for_update()
                )
                assert confirmed is not None
                confirmed.status = "superseded"
                database_session.add(
                    Itinerary(
                        session_id=session_id,
                        version=2,
                        context_version=2,
                        preference_version=1,
                        status="confirmed",
                        content={"destination": "manual-current", "days": []},
                        budget_summary={"is_mock": True},
                        source_task_ids=[],
                        confirmed_at=datetime.now(timezone.utc),
                    )
                )

        self.assertFalse(await self._compose(session_id, second_turn_id, 2))

        snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        tasks_by_id = {task["task_id"]: task for task in snapshot["tasks"]}
        self.assertEqual(
            tasks_by_id[str(second_task_id)]["target_itinerary_version"],
            1,
        )
        self.assertEqual(tasks_by_id[str(second_task_id)]["status"], "discarded")
        replanned_tasks = [
            task
            for task in snapshot["tasks"]
            if task["task_id"] not in {str(first_task_id), str(second_task_id)}
        ]
        self.assertEqual(len(replanned_tasks), 1)
        self.assertEqual(replanned_tasks[0]["status"], "queued")
        self.assertEqual(replanned_tasks[0]["target_itinerary_version"], 2)
        second_tool_call = next(
            tool_call
            for tool_call in snapshot["tool_calls"]
            if tool_call["task_id"] == str(second_task_id)
        )
        self.assertEqual(second_tool_call["status"], "succeeded")
        self.assertEqual(snapshot["itinerary"]["version"], 2)
        self.assertEqual(
            snapshot["itinerary"]["content"]["destination"], "manual-current"
        )

        replanned_task_id = UUID(replanned_tasks[0]["task_id"])
        await process_task(replanned_task_id)
        self.assertTrue(await self._compose(session_id, second_turn_id, 2))
        final_snapshot = (
            await self.client.get(f"/v1/sessions/{session_id}/snapshot")
        ).json()
        self.assertEqual(final_snapshot["itinerary"]["version"], 3)
        self.assertEqual(
            final_snapshot["itinerary"]["source_task_ids"], [str(replanned_task_id)]
        )
