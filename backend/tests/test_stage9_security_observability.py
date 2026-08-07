from __future__ import annotations

import asyncio
import logging
import tempfile
import unittest
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID

import httpx
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry import trace
from redis.exceptions import RedisError
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from app.agent.service import COMPOSE_STREAM, PLAN_STREAM, TRAVEL_TASK_STREAM
from app.agent.worker import process_compose_message, process_plan_message
from app.auth import AuthenticatedPrincipal
from app.config import Settings, settings
from app.db import async_session_factory, engine
from app.input_validation import validate_json_value
from app.logging import StructuredFormatter
from app.main import app, get_realtime_redis
from app.metrics import registry
from app.models import EventOutbox, Itinerary, Preference, Task, ToolCall, TravelSession, Turn
from app.observability import _otlp_traces_endpoint, bootstrap_observability, trace_scope
from app.outbox import publish_pending_events
from app.rate_limit import enforce_rate_limit
from app.worker import process_task
from tests.auth import auth_headers
from tests.fakes import FakeRedis


class BrokenRedis:
    async def eval(self, *args: object) -> int:
        del args
        raise RedisError("connection unavailable")


class SecurityBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_otlp_http_endpoint_accepts_collector_base_or_trace_path(self) -> None:
        self.assertEqual(
            _otlp_traces_endpoint("http://collector:4318"),
            "http://collector:4318/v1/traces",
        )
        self.assertEqual(
            _otlp_traces_endpoint("http://collector:4318/v1/traces"),
            "http://collector:4318/v1/traces",
        )

    async def test_http_body_limit_and_cors_allow_trace_headers(self) -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            oversized = await client.post(
                "/v1/sessions",
                content=b"x" * (settings.max_api_body_bytes + 1),
                headers={"Content-Type": "application/json"},
            )
            self.assertEqual(oversized.status_code, 413)
            self.assertEqual(oversized.json()["detail"], "Request body exceeds the allowed limit")

            preflight = await client.options(
                "/v1/sessions",
                headers={
                    "Origin": settings.allowed_cors_origins[0],
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "authorization,traceparent",
                },
            )
        self.assertEqual(preflight.status_code, 200)
        self.assertIn("authorization", preflight.headers["access-control-allow-headers"].lower())
        self.assertIn("traceparent", preflight.headers["access-control-allow-headers"].lower())

    async def test_rate_limit_returns_stable_429_and_503_responses(self) -> None:
        identity = AuthenticatedPrincipal(issuer="https://issuer.example", subject="user")
        redis = FakeRedis()
        await enforce_rate_limit(redis, bucket="test", limit=2, identity=identity)
        await enforce_rate_limit(redis, bucket="test", limit=2, identity=identity)
        with self.assertRaises(Exception) as limited:
            await enforce_rate_limit(redis, bucket="test", limit=2, identity=identity)
        error = limited.exception
        self.assertEqual(error.status_code, 429)
        self.assertEqual(error.detail, "Rate limit exceeded")
        self.assertEqual(error.headers, {"Retry-After": "60"})

        with self.assertRaises(Exception) as unavailable:
            await enforce_rate_limit(BrokenRedis(), bucket="test", limit=2, identity=identity)
        self.assertEqual(unavailable.exception.status_code, 503)
        self.assertEqual(unavailable.exception.detail, "Rate limit service unavailable")

    async def test_json_boundary_and_log_redaction(self) -> None:
        with self.assertRaises(Exception) as too_deep:
            validate_json_value({"a": {"b": {"c": 1}}}, depth=settings.max_json_depth)
        self.assertEqual(too_deep.exception.status_code, 422)

        formatter = StructuredFormatter()
        record = logging.LogRecord(
            "test",
            logging.ERROR,
            __file__,
            1,
            {"authorization": "Bearer top-secret", "payload": {"text": "private"}},
            (),
            None,
        )
        rendered = formatter.format(record)
        self.assertNotIn("top-secret", rendered)
        self.assertNotIn("private", rendered)
        self.assertIn("[REDACTED]", rendered)

    async def test_runtime_config_uses_secret_files_and_rejects_unsafe_roles(self) -> None:
        base = {
            "database_url": "postgresql+asyncpg://user:password@db/livepilot",
            "redis_url": "redis://redis/0",
        }
        with tempfile.TemporaryDirectory() as directory:
            secret_file = Path(directory) / "jwt"
            secret_file.write_text("file-secret-value", encoding="utf-8")
            loaded = Settings(
                **base,
                jwt_secret="environment-secret",
                jwt_secret_file=str(secret_file),
            )
            self.assertEqual(loaded.jwt_secret.get_secret_value(), "file-secret-value")
            self.assertNotIn("file-secret-value", repr(loaded))
            self.assertNotIn("password", repr(loaded))
            missing_file = Path(directory) / "missing"
            with self.assertRaisesRegex(RuntimeError, "JWT_SECRET_FILE") as error:
                Settings(**base, jwt_secret_file=str(missing_file))
            self.assertNotIn(str(missing_file), str(error.exception))

        production = Settings(
            **base,
            app_env="production",
            service_role="api",
            jwt_secret="x" * 32,
            jwt_issuer="https://issuer.example",
            jwt_audience="livepilot-web",
            trusted_cors_origins="https://app.example",
            otel_service_name="livepilot-api",
            otel_exporter_otlp_endpoint="https://otel.example/v1/traces",
            realtime_provider_mode="mock",
            tool_provider_mode="mock",
            realtime_provider_api_key="",
            weather_api_key="",
            map_api_key="",
        )
        production.validate_runtime_config()
        with self.assertRaisesRegex(RuntimeError, "SERVICE_ROLE"):
            production.validate_runtime_config(expected_service_role="task-worker")

        missing_realtime_secret = Settings(
            **base,
            service_role="api",
            realtime_provider_mode="real",
            realtime_provider_api_key="",
            tool_provider_mode="mock",
            weather_api_key="",
            map_api_key="",
        )
        with self.assertRaisesRegex(RuntimeError, "REALTIME_PROVIDER_API_KEY"):
            missing_realtime_secret.validate_runtime_config()

        wrong_role = Settings(
            **base,
            service_role="agent-worker",
            realtime_provider_mode="mock",
            tool_provider_mode="mock",
            realtime_provider_api_key="",
            weather_api_key="weather-secret",
            map_api_key="",
        )
        with self.assertRaisesRegex(RuntimeError, "TOOL_API_KEY") as error:
            wrong_role.validate_runtime_config()
        self.assertNotIn("weather-secret", str(error.exception))


class TracePropagationTests(unittest.IsolatedAsyncioTestCase):
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
            headers=auth_headers("trace-user"),
        )
        self.session_ids: list[UUID] = []

        bootstrap_observability(enable_exporter=False)
        self.exporter = InMemorySpanExporter()
        provider = trace.get_tracer_provider()
        provider.add_span_processor(SimpleSpanProcessor(self.exporter))

    async def asyncTearDown(self) -> None:
        app.dependency_overrides.pop(get_realtime_redis, None)
        await self.client.aclose()
        if self.session_ids:
            async with async_session_factory() as database_session:
                await database_session.execute(delete(EventOutbox).where(EventOutbox.session_id.in_(self.session_ids)))
                await database_session.execute(delete(ToolCall).where(ToolCall.session_id.in_(self.session_ids)))
                await database_session.execute(delete(Itinerary).where(Itinerary.session_id.in_(self.session_ids)))
                await database_session.execute(delete(Task).where(Task.session_id.in_(self.session_ids)))
                await database_session.execute(delete(Turn).where(Turn.session_id.in_(self.session_ids)))
                await database_session.execute(delete(Preference).where(Preference.session_id.in_(self.session_ids)))
                await database_session.execute(delete(TravelSession).where(TravelSession.id.in_(self.session_ids)))
                await database_session.commit()
        await engine.dispose()

    async def _database_is_available(self) -> None:
        probe_engine = create_async_engine(settings.database_url, connect_args={"timeout": 2})
        try:
            async with probe_engine.connect() as connection:
                await connection.scalar(select(EventOutbox.id).limit(1))
        finally:
            await probe_engine.dispose()

    async def test_text_turn_trace_reaches_both_workers_and_tool(self) -> None:
        created = await self.client.post("/v1/sessions", json={})
        self.assertEqual(created.status_code, 201)
        session_id = UUID(created.json()["session_id"])
        self.session_ids.append(session_id)

        with trace_scope("http.turn", {"route": "turns"}):
            turn = await self.client.post(
                f"/v1/sessions/{session_id}/turns",
                json={"text": "安排博物馆旅行", "client_event_id": "trace-turn"},
            )
        self.assertEqual(turn.status_code, 202)

        await publish_pending_events(self.redis)
        plan_fields = self.redis.streams[PLAN_STREAM][-1]
        self.assertRegex(plan_fields["traceparent"], r"^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")
        await process_plan_message(plan_fields, self.redis)

        await publish_pending_events(self.redis)
        task_fields = self.redis.streams[TRAVEL_TASK_STREAM][-1]
        self.assertIn("traceparent", task_fields)
        await process_task(UUID(task_fields["task_id"]), self.redis, task_fields)

        await publish_pending_events(self.redis)
        compose_fields = self.redis.streams[COMPOSE_STREAM][-1]
        self.assertIn("traceparent", compose_fields)
        self.assertTrue(await process_compose_message(compose_fields))

        names = {span.name for span in self.exporter.get_finished_spans()}
        self.assertTrue(
            {"http.turn", "outbox.publish", "agent.plan.consume", "task.consume", "tool.call", "agent.compose.consume"}.issubset(names)
        )
        trace_ids = {
            span.context.trace_id
            for span in self.exporter.get_finished_spans()
            if span.name in {"http.turn", "agent.plan.consume", "task.consume", "tool.call", "agent.compose.consume"}
        }
        self.assertEqual(len(trace_ids), 1)

    async def test_metrics_use_only_declared_low_cardinality_labels(self) -> None:
        names = {metric.name for metric in registry.collect()}
        self.assertIn("livepilot_task_completion_seconds", names)
        self.assertIn("livepilot_tool_call_seconds", names)
        self.assertIn("livepilot_outbox_publish_failures", names)
