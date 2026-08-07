from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", populate_by_name=True
    )

    database_url: str
    redis_url: str
    app_env: str = "development"
    service_role: str = "api"
    frontend_origin: str | None = None
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    trusted_cors_origins: str | None = None
    realtime_token_ttl_seconds: int = Field(default=60, ge=1, le=60)
    rate_limit_session_create: int = Field(
        default=10,
        ge=1,
        le=10_000,
        validation_alias=AliasChoices(
            "RATE_LIMIT_SESSION_CREATE_PER_MINUTE", "RATE_LIMIT_SESSION_CREATE"
        ),
    )
    rate_limit_session_read: int = Field(
        default=120,
        ge=1,
        le=10_000,
        validation_alias=AliasChoices(
            "RATE_LIMIT_SESSION_READ_PER_MINUTE", "RATE_LIMIT_SESSION_READ"
        ),
    )
    rate_limit_session_write: int = Field(
        default=30,
        ge=1,
        le=10_000,
        validation_alias=AliasChoices(
            "RATE_LIMIT_SESSION_WRITE_PER_MINUTE", "RATE_LIMIT_SESSION_WRITE"
        ),
    )
    rate_limit_ws_connect: int = Field(
        default=20,
        ge=1,
        le=10_000,
        validation_alias=AliasChoices(
            "RATE_LIMIT_WS_CONNECT_PER_MINUTE", "RATE_LIMIT_WS_CONNECT"
        ),
    )
    max_api_body_bytes: int = Field(default=65_536, ge=1_024, le=1_048_576)
    max_json_depth: int = Field(default=12, ge=1, le=64)
    max_json_items: int = Field(default=200, ge=1, le=10_000)
    max_json_key_length: int = Field(default=128, ge=1, le=1_024)
    max_json_string_length: int = Field(default=20_000, ge=1, le=1_048_576)
    jwt_secret: SecretStr = SecretStr("change-me-development-secret")
    jwt_secret_file: str | None = Field(default=None, repr=False)
    jwt_issuer: str = ""
    jwt_audience: str = "livepilot-web"
    jwt_clock_skew_seconds: int = Field(default=0, ge=0, le=60)
    realtime_provider_mode: str = "mock"
    realtime_provider_api_key: SecretStr | None = None
    realtime_provider_api_key_file: str | None = Field(default=None, repr=False)
    tool_provider_mode: str = "mock"
    weather_api_key: SecretStr | None = None
    weather_api_key_file: str | None = Field(default=None, repr=False)
    map_api_key: SecretStr | None = None
    map_api_key_file: str | None = Field(default=None, repr=False)
    otel_service_name: str = "livepilot-api"
    otel_exporter_otlp_endpoint: str | None = None

    @model_validator(mode="after")
    def load_file_secrets(self) -> Settings:
        for field_name in (
            "jwt_secret",
            "realtime_provider_api_key",
            "weather_api_key",
            "map_api_key",
        ):
            file_name = f"{field_name}_file"
            secret_file = getattr(self, file_name)
            if secret_file:
                try:
                    value = Path(secret_file).read_text(encoding="utf-8").strip()
                except OSError:
                    raise RuntimeError(
                        f"invalid configuration: {file_name.upper()}"
                    ) from None
                if not value:
                    raise RuntimeError(
                        f"invalid configuration: {file_name.upper()}"
                    )
                setattr(self, field_name, SecretStr(value))
        return self

    @property
    def effective_jwt_issuer(self) -> str:
        return self.jwt_issuer or "livepilot-dev"

    def __repr__(self) -> str:
        return f"Settings(app_env={self.app_env!r}, service_role={self.service_role!r})"

    @property
    def allowed_cors_origins(self) -> list[str]:
        configured = self.trusted_cors_origins or self.frontend_origin or self.cors_origins
        origins = [origin.strip() for origin in configured.split(",") if origin.strip()]
        if not origins or "*" in origins:
            raise ValueError("CORS origins must be explicit")
        return origins

    def is_trusted_origin(self, origin: str) -> bool:
        return origin in self.allowed_cors_origins

    def validate_runtime_config(self, *, expected_service_role: str | None = None) -> None:
        if self.app_env not in {"development", "test", "production"}:
            raise RuntimeError("invalid configuration: APP_ENV")
        if self.service_role not in {"api", "agent-worker", "task-worker"}:
            raise RuntimeError("invalid configuration: SERVICE_ROLE")
        if expected_service_role is not None and self.service_role != expected_service_role:
            raise RuntimeError("invalid configuration: SERVICE_ROLE")
        if self.realtime_provider_mode not in {"mock", "real"}:
            raise RuntimeError("invalid configuration: REALTIME_PROVIDER_MODE")
        if self.tool_provider_mode not in {"mock", "real"}:
            raise RuntimeError("invalid configuration: TOOL_PROVIDER_MODE")
        try:
            origins = self.allowed_cors_origins
        except ValueError as error:
            raise RuntimeError("invalid configuration: TRUSTED_CORS_ORIGINS") from error

        def require_secret(name: str, secret: SecretStr | None) -> None:
            value = secret.get_secret_value() if secret is not None else ""
            if not value or value.lower().startswith("change-me"):
                raise RuntimeError(f"invalid configuration: {name}")

        def has_real_secret(secret: SecretStr | None) -> bool:
            value = secret.get_secret_value() if secret is not None else ""
            return bool(value and not value.lower().startswith("change-me"))

        if self.app_env == "production":
            require_secret("JWT_SECRET", self.jwt_secret)
            if len(self.jwt_secret.get_secret_value()) < 32:
                raise RuntimeError("invalid configuration: JWT_SECRET")
            if not self.jwt_issuer:
                raise RuntimeError("invalid configuration: JWT_ISSUER")
            if not self.jwt_audience:
                raise RuntimeError("invalid configuration: JWT_AUDIENCE")
            if any(
                urlparse(origin).scheme != "https" or not urlparse(origin).netloc
                for origin in origins
            ):
                raise RuntimeError("invalid configuration: TRUSTED_CORS_ORIGINS")
            if not self.otel_service_name:
                raise RuntimeError("invalid configuration: OTEL_SERVICE_NAME")
            endpoint = self.otel_exporter_otlp_endpoint
            if not endpoint or not urlparse(endpoint).scheme or not urlparse(endpoint).netloc:
                raise RuntimeError("invalid configuration: OTEL_EXPORTER_OTLP_ENDPOINT")

        if self.realtime_provider_mode == "real":
            if self.service_role != "api":
                raise RuntimeError("invalid configuration: REALTIME_PROVIDER_API_KEY")
            require_secret("REALTIME_PROVIDER_API_KEY", self.realtime_provider_api_key)
        elif has_real_secret(self.realtime_provider_api_key) and self.service_role != "api":
            raise RuntimeError("invalid configuration: REALTIME_PROVIDER_API_KEY")

        tool_secrets = (self.weather_api_key, self.map_api_key)
        if self.tool_provider_mode == "real":
            if self.service_role != "task-worker":
                raise RuntimeError("invalid configuration: TOOL_PROVIDER_MODE")
            for name, secret in zip(("WEATHER_API_KEY", "MAP_API_KEY"), tool_secrets):
                require_secret(name, secret)
        elif any(has_real_secret(secret) for secret in tool_secrets) and self.service_role != "task-worker":
            raise RuntimeError("invalid configuration: TOOL_API_KEY")


settings = Settings()


def validate_runtime_config(*, expected_service_role: str | None = None) -> None:
    settings.validate_runtime_config(expected_service_role=expected_service_role)
