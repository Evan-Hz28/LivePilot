import asyncio
import unittest
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx
from starlette.datastructures import Headers, QueryParams
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from app.db import async_session_factory, engine
from app.main import app, get_realtime_redis, session_events
from app.models import EventOutbox, Preference, Task, TravelSession, Turn
from tests.auth import access_token, auth_headers
from tests.fakes import FakeRedis


class ClosingWebSocket:
    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = Headers(headers=headers)
        self.query_params = QueryParams()
        self.close_code: int | None = None
        self.close_reason: str | None = None

    async def close(self, code: int, reason: str | None = None) -> None:
        self.close_code = code
        self.close_reason = reason


class Stage9AuthenticationTests(unittest.IsolatedAsyncioTestCase):
    owner_subject = "stage9-owner"
    other_subject = "stage9-other"

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
            headers=auth_headers(self.owner_subject),
            json={"preference": {"destination": "Kyoto"}},
        )
        self.assertEqual(response.status_code, 201)
        session_id = UUID(response.json()["session_id"])
        self.session_ids.append(session_id)
        return session_id

    async def test_rejects_missing_invalid_and_expired_access_tokens(self) -> None:
        missing = await self.client.post("/v1/sessions", json={})
        expired = await self.client.post(
            "/v1/sessions",
            headers=auth_headers("expired", exp=0),
            json={},
        )
        wrong_issuer = await self.client.post(
            "/v1/sessions",
            headers=auth_headers("wrong-issuer", iss="https://elsewhere.example"),
            json={},
        )
        wrong_audience = await self.client.post(
            "/v1/sessions",
            headers=auth_headers("wrong-audience", aud="another-service"),
            json={},
        )
        missing_subject = await self.client.post(
            "/v1/sessions",
            headers=auth_headers("ignored", sub=""),
            json={},
        )
        tampered = await self.client.post(
            "/v1/sessions",
            headers={"Authorization": f"Bearer {access_token()}x"},
            json={},
        )

        for response in (
            missing,
            expired,
            wrong_issuer,
            wrong_audience,
            missing_subject,
            tampered,
        ):
            self.assertEqual(response.status_code, 401)
            self.assertEqual(response.headers["www-authenticate"], "Bearer")

    async def test_cross_user_session_operations_are_not_discoverable(self) -> None:
        session_id = await self._create_session()
        owner_headers = auth_headers(self.owner_subject)
        other_headers = auth_headers(self.other_subject)
        finalized = await self.client.post(
            f"/v1/sessions/{session_id}/turns",
            headers=owner_headers,
            json={"text": "安排文化旅行", "client_event_id": "stage9-owner-turn"},
        )
        self.assertEqual(finalized.status_code, 202)
        turn_id = finalized.json()["turn_id"]

        forbidden = [
            await self.client.get(
                f"/v1/sessions/{session_id}/snapshot",
                headers=other_headers,
            ),
            await self.client.patch(
                f"/v1/sessions/{session_id}/preferences",
                headers=other_headers,
                json={"base_context_version": 1, "patch": {"budget": 1}},
            ),
            await self.client.post(
                f"/v1/sessions/{session_id}/turns",
                headers=other_headers,
                json={"text": "跨用户写入", "client_event_id": "stage9-other-turn"},
            ),
            await self.client.post(
                f"/v1/sessions/{session_id}/realtime-token",
                headers=other_headers,
                json={"device_id": "other-device"},
            ),
            await self.client.post(
                f"/v1/sessions/{session_id}/resume",
                headers=other_headers,
                json={
                    "after_event_seq": 0,
                    "previous_connection_epoch": 0,
                    "device_id": "other-device",
                },
            ),
            await self.client.post(
                f"/v1/sessions/{session_id}/interrupt",
                headers=other_headers,
                json={
                    "turn_id": turn_id,
                    "client_event_id": "stage9-other-interrupt",
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                },
            ),
        ]
        nonexistent = await self.client.get(
            f"/v1/sessions/{uuid4()}/snapshot",
            headers=other_headers,
        )

        for response in forbidden:
            self.assertEqual(response.status_code, 404)
            self.assertEqual(response.json()["detail"], "Session not found")
        self.assertEqual(nonexistent.status_code, 404)
        self.assertEqual(nonexistent.json(), forbidden[0].json())

        issued = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token",
            headers=owner_headers,
            json={"device_id": "owner-device"},
        )
        self.assertEqual(issued.status_code, 200)
        token = issued.json()["token"]
        cross_user_redeem = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            headers=other_headers,
            json={"device_id": "owner-device", "token": token},
        )
        owner_redeem = await self.client.post(
            f"/v1/sessions/{session_id}/realtime-token/redeem",
            headers=owner_headers,
            json={"device_id": "owner-device", "token": token},
        )
        self.assertEqual(cross_user_redeem.status_code, 404)
        self.assertEqual(owner_redeem.status_code, 200)

    async def test_cross_user_websocket_handshake_is_rejected(self) -> None:
        session_id = await self._create_session()
        websocket = ClosingWebSocket(auth_headers(self.other_subject))

        await session_events(websocket, session_id)

        self.assertEqual(websocket.close_code, 1008)
        self.assertEqual(websocket.close_reason, "Session not found")
