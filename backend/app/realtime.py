from __future__ import annotations

import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import TravelSession

TOKEN_KEY_PREFIX = "livepilot:realtime-token:"


class RealtimeTokenError(Exception):
    """The token is missing, expired, consumed, or no longer current."""


class RealtimeTokenEpochError(RealtimeTokenError):
    """The client is attempting to resume a connection that is no longer current."""


@dataclass(frozen=True)
class RealtimeTokenGrant:
    token: str
    session_id: UUID
    device_id: str
    connection_epoch: int
    expires_at: datetime
    provider_config: dict[str, Any]


def _token_key(token: str) -> str:
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return f"{TOKEN_KEY_PREFIX}{digest}"


def _provider_config(session_id: UUID) -> dict[str, Any]:
    return {
        "provider": "mock",
        "adapter": "loopback",
        "data_channel_label": "livepilot.realtime",
        "ice_servers": [],
        "token_redeem_path": (
            f"/v1/sessions/{session_id}/realtime-token/redeem"
        ),
    }


async def issue_realtime_token(
    database_session: AsyncSession,
    redis: Redis,
    *,
    session_id: UUID,
    device_id: str,
    expected_connection_epoch: int | None = None,
) -> RealtimeTokenGrant:
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(
        seconds=settings.realtime_token_ttl_seconds
    )

    async with database_session.begin():
        session = await database_session.scalar(
            select(TravelSession)
            .where(TravelSession.id == session_id)
            .with_for_update()
        )
        if session is None:
            raise LookupError("Session not found")
        if (
            expected_connection_epoch is not None
            and session.realtime_connection_epoch != expected_connection_epoch
        ):
            raise RealtimeTokenEpochError("Realtime connection epoch is stale")
        session.realtime_connection_epoch += 1
        connection_epoch = session.realtime_connection_epoch

    payload = {
        "session_id": str(session_id),
        "device_id": device_id,
        "connection_epoch": connection_epoch,
        "expires_at": expires_at.isoformat(),
    }
    stored = await redis.set(
        _token_key(token),
        json.dumps(payload, ensure_ascii=True, sort_keys=True),
        ex=settings.realtime_token_ttl_seconds,
        nx=True,
    )
    if not stored:
        raise RuntimeError("Could not reserve realtime token")

    return RealtimeTokenGrant(
        token=token,
        session_id=session_id,
        device_id=device_id,
        connection_epoch=connection_epoch,
        expires_at=expires_at,
        provider_config=_provider_config(session_id),
    )


async def redeem_realtime_token(
    database_session: AsyncSession,
    redis: Redis,
    *,
    session_id: UUID,
    device_id: str,
    token: str,
) -> RealtimeTokenGrant:
    raw_payload = await redis.getdel(_token_key(token))
    if raw_payload is None:
        raise RealtimeTokenError("Realtime token is missing or already used")

    try:
        payload = json.loads(raw_payload)
        token_session_id = UUID(str(payload["session_id"]))
        token_device_id = str(payload["device_id"])
        connection_epoch = int(payload["connection_epoch"])
        expires_at = datetime.fromisoformat(str(payload["expires_at"]))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RealtimeTokenError("Realtime token is invalid") from error

    if token_session_id != session_id or token_device_id != device_id:
        raise RealtimeTokenError("Realtime token binding mismatch")
    if expires_at <= datetime.now(timezone.utc):
        raise RealtimeTokenError("Realtime token is expired")

    session = await database_session.get(TravelSession, session_id)
    if session is None or session.realtime_connection_epoch != connection_epoch:
        raise RealtimeTokenError("Realtime token epoch is stale")

    return RealtimeTokenGrant(
        token=token,
        session_id=session_id,
        device_id=device_id,
        connection_epoch=connection_epoch,
        expires_at=expires_at,
        provider_config=_provider_config(session_id),
    )
