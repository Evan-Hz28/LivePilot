import base64
import hashlib
import hmac
import json
import time

from app.config import settings


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def access_token(subject: str = "test-user", **overrides: object) -> str:
    header = _base64url(b'{"alg":"HS256","typ":"JWT"}')
    claims: dict[str, object] = {
        "iss": settings.effective_jwt_issuer,
        "aud": settings.jwt_audience,
        "sub": subject,
        "exp": int(time.time()) + 300,
    }
    claims.update(overrides)
    payload = _base64url(
        json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signing_input = f"{header}.{payload}".encode("ascii")
    signature = hmac.new(
        settings.jwt_secret.get_secret_value().encode("utf-8"),
        signing_input,
        hashlib.sha256,
    ).digest()
    return f"{header}.{payload}.{_base64url(signature)}"


def auth_headers(subject: str = "test-user", **overrides: object) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token(subject, **overrides)}"}
