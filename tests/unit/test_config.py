import pytest
from pydantic import ValidationError

from src.core.config import AppSettings


def test_app_settings_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "dummy_token")
    settings = AppSettings() # type: ignore[call-arg]
    assert settings.JQUANTS_REFRESH_TOKEN == "dummy_token"

def test_app_settings_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    with pytest.raises(ValidationError):
        AppSettings() # type: ignore[call-arg]
