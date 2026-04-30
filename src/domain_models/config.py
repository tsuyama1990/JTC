from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Application settings, mapping to environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # JQUANTS_REFRESH_TOKEN is absolutely required for the external API ingestion
    JQUANTS_REFRESH_TOKEN: str


# Create a global settings instance to be imported by other modules
# (In tests, this might fail if the env var isn't set, so we can initialize it lazily or catch it, but pydantic-settings handles it if it's in env).
def get_settings() -> AppSettings:
    return AppSettings() # type: ignore[call-arg]
