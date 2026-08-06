import asyncio
import json
import unittest
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from uuid import UUID

import httpx
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from app.db import async_session_factory, engine
from app.main import app, get_realtime_redis
from app.models import EventOutbox, Preference, Task, TravelSession, Turn


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(
        self,
        key: str,
        value: str,
        *,
        ex: int,
        nx: bool,
    ) -> bool | None:
        del ex
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    async def getdel(self, key: str) -> str | None:
        return self.values.pop(key, None)


class RealtimeTokenApiTests(unittest.IsolatedAsyncioTestCase):
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

    async def _create_session(self) -> str:
        response = await self.client.post(
            "/v1/sessions",
            json={"preference": {"destination": "Kyoto"}},
        )
        self.assertEqual(response.status_code, 201)
        session_id = response.json()["session_id"]
        self.session_ids.append(UUID(session_id))
        return session_id

    async def test_token_is_bound_to_epoch_and_consumed_once(self) -> None:
        session_id = await self._create_session()

        first = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token",
            json={"device_id": "device-a"},
        )
        second = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token",
            json={"device_id": "device-a"},
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        first_body = first.json()
        second_body = second.json()
        self.assertEqual(first_body["connection_epoch"], 1)
        self.assertEqual(second_body["connection_epoch"], 2)
        self.assertEqual(first_body["provider_config"]["provider"], "mock")
        self.assertNotIn("api_key", first_body["provider_config"])

        stale = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            json={"device_id": "device-a", "token": first_body["token"]},
        )
        self.assertEqual(stale.status_code, 401)

        redeemed = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            json={"device_id": "device-a", "token": second_body["token"]},
        )
        replay = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            json={"device_id": "device-a", "token": second_body["token"]},
        )
        self.assertEqual(redeemed.status_code, 200)
        self.assertEqual(redeemed.json()["connection_epoch"], 2)
        self.assertEqual(replay.status_code, 401)

    async def test_token_binding_rejects_wrong_device(self) -> None:
        session_id = await self._create_session()
        issued = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token",
            json={"device_id": "device-a"},
        )
        token = issued.json()["token"]

        wrong_device = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            json={"device_id": "device-b", "token": token},
        )
        replay = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            json={"device_id": "device-a", "token": token},
        )
        self.assertEqual(wrong_device.status_code, 401)
        self.assertEqual(replay.status_code, 401)

    async def test_expired_token_is_rejected(self) -> None:
        session_id = await self._create_session()
        issued = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token",
            json={"device_id": "device-a"},
        )
        token = issued.json()["token"]
        token_key = next(iter(self.redis.values))
        payload = json.loads(self.redis.values[token_key])
        payload["expires_at"] = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat()
        self.redis.values[token_key] = json.dumps(payload)

        expired = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            json={"device_id": "device-a", "token": token},
        )
        self.assertEqual(expired.status_code, 401)
