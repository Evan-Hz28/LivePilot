from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import math
import time
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, HTTPException, WebSocket, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import async_session_factory
from app.models import Principal, TravelSession


class AuthenticationError(Exception):
    """The supplied access token cannot establish an authenticated identity."""


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    issuer: str
    subject: str


bearer_scheme = HTTPBearer(auto_error=False)


def _decode_base64url(value: str) -> bytes:
    try:
        encoded = value.encode("ascii")
        return base64.b64decode(
            encoded + b"=" * (-len(encoded) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError) as error:
        raise AuthenticationError from error


def _decode_json_object(value: str) -> dict[str, Any]:
    try:
        decoded = json.loads(_decode_base64url(value))
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AuthenticationError from error
    if not isinstance(decoded, dict):
        raise AuthenticationError
    return decoded


def _numeric_date(claims: dict[str, Any], name: str) -> float:
    value = claims.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AuthenticationError
    numeric_value = float(value)
    if not math.isfinite(numeric_value):
        raise AuthenticationError
    return numeric_value


def decode_access_token(token: str) -> AuthenticatedPrincipal:
    segments = token.split(".")
    if len(segments) != 3 or not all(segments):
        raise AuthenticationError
    header_segment, payload_segment, signature_segment = segments

    header = _decode_json_object(header_segment)
    claims = _decode_json_object(payload_segment)
    if header.get("alg") != "HS256":
        raise AuthenticationError

    signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
    expected_signature = hmac.new(
        settings.jwt_secret.get_secret_value().encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(_decode_base64url(signature_segment), expected_signature):
        raise AuthenticationError

    issuer = claims.get("iss")
    subject = claims.get("sub")
    audience = claims.get("aud")
    if not isinstance(issuer, str) or not issuer or len(issuer) > 512:
        raise AuthenticationError
    if not isinstance(subject, str) or not subject or len(subject) > 255:
        raise AuthenticationError
    if issuer != settings.effective_jwt_issuer:
        raise AuthenticationError
    if isinstance(audience, str):
        audiences = [audience]
    elif isinstance(audience, list) and all(isinstance(item, str) for item in audience):
        audiences = audience
    else:
        raise AuthenticationError
    if settings.jwt_audience not in audiences:
        raise AuthenticationError

    now = time.time()
    leeway = settings.jwt_clock_skew_seconds
    if _numeric_date(claims, "exp") <= now - leeway:
        raise AuthenticationError
    if "nbf" in claims and _numeric_date(claims, "nbf") > now + leeway:
        raise AuthenticationError

    return AuthenticatedPrincipal(issuer=issuer, subject=subject)


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_current_principal(
    credentials: Annotated[
        HTTPAuthorizationCredentials | None,
        Depends(bearer_scheme),
    ],
) -> AuthenticatedPrincipal:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _unauthorized()
    try:
        return decode_access_token(credentials.credentials)
    except AuthenticationError as error:
        raise _unauthorized() from error


CurrentPrincipal = Annotated[AuthenticatedPrincipal, Depends(get_current_principal)]


async def get_or_create_principal(
    database_session: AsyncSession,
    identity: AuthenticatedPrincipal,
) -> Principal:
    principal_id = await database_session.scalar(
        insert(Principal)
        .values(issuer=identity.issuer, subject=identity.subject)
        .on_conflict_do_nothing(index_elements=("issuer", "subject"))
        .returning(Principal.id)
    )
    if principal_id is not None:
        principal = await database_session.get(Principal, principal_id)
    else:
        principal = await database_session.scalar(
            select(Principal).where(
                Principal.issuer == identity.issuer,
                Principal.subject == identity.subject,
            )
        )
    if principal is None:
        raise RuntimeError("Authenticated principal could not be persisted")
    return principal


async def require_session_owner(
    session_id: UUID,
    identity: CurrentPrincipal,
) -> AuthenticatedPrincipal:
    async with async_session_factory() as database_session:
        owned_session_id = await database_session.scalar(
            select(TravelSession.id)
            .join(Principal, TravelSession.owner_principal_id == Principal.id)
            .where(
                TravelSession.id == session_id,
                Principal.issuer == identity.issuer,
                Principal.subject == identity.subject,
            )
        )
    if owned_session_id is None:
        # Do not disclose whether this session exists to a different principal.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    return identity


SessionOwner = Annotated[AuthenticatedPrincipal, Depends(require_session_owner)]


def authenticate_websocket(websocket: WebSocket) -> AuthenticatedPrincipal:
    authorization = websocket.headers.get("authorization")
    token: str | None = None
    if authorization:
        scheme, separator, candidate = authorization.partition(" ")
        if scheme.lower() == "bearer" and separator and candidate:
            token = candidate
    else:
        protocols = websocket.headers.get("sec-websocket-protocol", "").split(",")
        for protocol in protocols:
            candidate = protocol.strip()
            if candidate.startswith("bearer."):
                token = candidate.removeprefix("bearer.")
                break
    if token is None:
        raise AuthenticationError
    return decode_access_token(token)


async def websocket_session_is_owned(
    session_id: UUID,
    identity: AuthenticatedPrincipal,
) -> bool:
    async with async_session_factory() as database_session:
        return (
            await database_session.scalar(
                select(TravelSession.id)
                .join(Principal, TravelSession.owner_principal_id == Principal.id)
                .where(
                    TravelSession.id == session_id,
                    Principal.issuer == identity.issuer,
                    Principal.subject == identity.subject,
                )
            )
        ) is not None
