from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    frontend_origin: str | None = None
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"

    @property
    def allowed_cors_origins(self) -> list[str]:
        configured = self.frontend_origin or self.cors_origins
        return [origin.strip() for origin in configured.split(",") if origin.strip()]


settings = Settings()
