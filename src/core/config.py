from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    DEFAULT_FEE: float = 0.001
    DEFAULT_SLIPPAGE: float = 0.000  # Default to 0 slippage
    DEFAULT_INITIAL_CASH: float = 1_000_000.0


settings = Settings()
