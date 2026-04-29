from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    JQUANTS_REFRESH_TOKEN: str

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

class APIConnectionError(Exception):
    """Exception raised for catastrophic network failures during API connection."""
