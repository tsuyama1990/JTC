from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    jquants_refresh_token: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

def get_settings() -> AppSettings:
    return AppSettings()  # type: ignore[call-arg]
