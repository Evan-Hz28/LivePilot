import asyncio
import unittest
from uuid import UUID

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from app.db import async_session_factory, engine
from app.main import app
from app.models import EventOutbox, Preference, Task, TravelSession, Turn


class SessionApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        try:
            await asyncio.wait_for(self._database_is_available(), timeout=3)
        except Exception as error:  # pragma: no cover - environment-dependent
            self.skipTest(f"database unavailable: {error}")

        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        )
        self._created_session_ids: list[UUID] = []

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        if self._created_session_ids:
            await self._delete_sessions(self._created_session_ids)
        # IsolatedAsyncioTestCase creates a fresh loop per test; do not retain
        # asyncpg connections bound to the previous loop in SQLAlchemy's pool.
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

    async def _delete_sessions(self, session_ids: list[UUID]) -> None:
        async with async_session_factory() as database_session:
            await database_session.execute(
                delete(EventOutbox).where(EventOutbox.session_id.in_(session_ids))
            )
            await database_session.execute(
                delete(Preference).where(Preference.session_id.in_(session_ids))
            )
            await database_session.execute(
                delete(Turn).where(Turn.session_id.in_(session_ids))
            )
            await database_session.execute(
                delete(Task).where(Task.session_id.in_(session_ids))
            )
            await database_session.execute(
                delete(TravelSession).where(TravelSession.id.in_(session_ids))
            )
            await database_session.commit()

    async def create_session(self) -> dict[str, object]:
        response = await self.client.post(
            "/v1/sessions",
            json={"preference": {"destination": "Kyoto"}},
        )
        self.assertEqual(response.status_code, 201)
        session = response.json()
        self._created_session_ids.append(UUID(session["session_id"]))
        return session

    async def test_create_session_and_snapshot(self) -> None:
        created = await self.create_session()

        self.assertEqual(created["context_version"], 0)
        self.assertEqual(created["preference_version"], 1)

        response = await self.client.get(
            f"/v1/sessions/{created['session_id']}/snapshot"
        )
        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertEqual(snapshot["session"]["last_event_seq"], 1)
        self.assertEqual(snapshot["active_preference"]["version"], 1)
        self.assertEqual(
            snapshot["active_preference"]["payload"]["destination"], "Kyoto"
        )
        self.assertEqual(snapshot["turns"], [])
        self.assertEqual(snapshot["tasks"], [])
        self.assertEqual(snapshot["itinerary"]["status"], "not_created")
        self.assertEqual(snapshot["missed_events"][0]["event_type"], "session.created")

    async def test_preference_update_increments_context_and_preference(self) -> None:
        created = await self.create_session()

        response = await self.client.patch(
            f"/v1/sessions/{created['session_id']}/preferences",
            json={
                "base_context_version": 0,
                "client_event_id": "client-event-1",
                "patch": {"budget": {"currency": "JPY", "max": 100000}},
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["context_version"], 1)
        self.assertEqual(response.json()["preference_version"], 2)
        self.assertEqual(response.json()["event_seq"], 2)

        snapshot = (
            await self.client.get(
                f"/v1/sessions/{created['session_id']}/snapshot?after_event_seq=1"
            )
        ).json()
        self.assertEqual(snapshot["session"]["context_version"], 1)
        self.assertEqual(snapshot["active_preference"]["version"], 2)
        self.assertEqual(
            snapshot["active_preference"]["payload"]["destination"], "Kyoto"
        )
        self.assertEqual(
            snapshot["active_preference"]["payload"]["budget"]["max"], 100000
        )
        self.assertEqual(len(snapshot["missed_events"]), 1)
        self.assertEqual(snapshot["missed_events"][0]["event_seq"], 2)

    async def test_stale_preference_update_returns_conflict(self) -> None:
        created = await self.create_session()
        session_id = created["session_id"]

        updated = await self.client.patch(
            f"/v1/sessions/{session_id}/preferences",
            json={
                "base_context_version": 0,
                "client_event_id": "client-event-1",
                "patch": {"crowd_tolerance": "low"},
            },
        )
        self.assertEqual(updated.status_code, 200)

        conflict = await self.client.patch(
            f"/v1/sessions/{session_id}/preferences",
            json={
                "base_context_version": 0,
                "client_event_id": "client-event-2",
                "patch": {"crowd_tolerance": "high"},
            },
        )
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json()["detail"]["context_version"], 1)
        self.assertEqual(conflict.json()["detail"]["preference_version"], 2)

    async def test_preference_update_is_idempotent_by_client_event_id(self) -> None:
        created = await self.create_session()
        session_id = created["session_id"]
        body = {
            "base_context_version": 0,
            "client_event_id": "client-event-1",
            "patch": {"interests": ["museum"]},
        }

        first = await self.client.patch(
            f"/v1/sessions/{session_id}/preferences", json=body
        )
        second = await self.client.patch(
            f"/v1/sessions/{session_id}/preferences", json=body
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json(), first.json())
