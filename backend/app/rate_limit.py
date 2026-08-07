from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, status
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.auth import AuthenticatedPrincipal, CurrentPrincipal, SessionOwner
from app.config import settings
from app.main_dependencies import RealtimeRedis

RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_SCRIPT = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
"""


def _principal_hash(identity: AuthenticatedPrincipal) -> str:
    material = f"{identity.issuer}\x00{identity.subject}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:24]


def rate_limit_key(
    bucket: str,
    identity: AuthenticatedPrincipal,
    session_id: UUID | None = None,
) -> str:
    parts = ["livepilot", "rate", bucket, _principal_hash(identity)]
    if session_id is not None:
        parts.append(str(session_id))
    return ":".join(parts)


async def enforce_rate_limit(
    redis: Redis,
    *,
    bucket: str,
    limit: int,
    identity: AuthenticatedPrincipal,
    session_id: UUID | None = None,
) -> None:
    try:
        count = int(
            await redis.eval(
                RATE_LIMIT_SCRIPT,
                1,
                rate_limit_key(bucket, identity, session_id),
                RATE_LIMIT_WINDOW_SECONDS,
            )
        )
    except (RedisError, TypeError, ValueError) as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Rate limit service unavailable",
        ) from error
    if count > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(RATE_LIMIT_WINDOW_SECONDS)},
        )


RateLimitDependency = Callable[..., Awaitable[None]]


def principal_rate_limit(bucket: str, limit: int) -> RateLimitDependency:
    async def dependency(
        identity: CurrentPrincipal,
        redis: RealtimeRedis,
    ) -> None:
        await enforce_rate_limit(
            redis,
            bucket=bucket,
            limit=limit,
            identity=identity,
        )

    return dependency


def session_rate_limit(bucket: str, limit: int) -> RateLimitDependency:
    async def dependency(
        session_id: UUID,
        identity: SessionOwner,
        redis: RealtimeRedis,
    ) -> None:
        await enforce_rate_limit(
            redis,
            bucket=bucket,
            limit=limit,
            identity=identity,
        )
        await enforce_rate_limit(
            redis,
            bucket=bucket,
            limit=limit,
            identity=identity,
            session_id=session_id,
        )

    return dependency


CreateSessionRateLimit = Annotated[
    None,
    Depends(principal_rate_limit("session-create", settings.rate_limit_session_create)),
]
ReadSessionRateLimit = Annotated[
    None,
    Depends(session_rate_limit("session-read", settings.rate_limit_session_read)),
]
WriteSessionRateLimit = Annotated[
    None,
    Depends(session_rate_limit("session-write", settings.rate_limit_session_write)),
]
