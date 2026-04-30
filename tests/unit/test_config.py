
import pytest
from pydantic import ValidationError

from src.core.config import AppSettings


def test_app_settings_loads_refresh_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "fake_token_123")
    settings = AppSettings() # type: ignore[call-arg]
    assert settings.JQUANTS_REFRESH_TOKEN == "fake_token_123" # noqa: S105

def test_app_settings_fails_without_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    # Also clear the env_file in case there is a global .env during CI
    monkeypatch.setattr("pydantic_settings.BaseSettings._settings_build_values", lambda *args, **kwargs: {})
    with pytest.raises(ValidationError):
        AppSettings() # type: ignore[call-arg]
