from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    JQUANTS_REFRESH_TOKEN: str = Field(min_length=1)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="forbid")
