import pytest
from pydantic import ValidationError

from src.domain_models.config import AppSettings


def test_config_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JQUANTS_REFRESH_TOKEN", "test_token")
    settings = AppSettings()  # type: ignore[call-arg]
    assert settings.JQUANTS_REFRESH_TOKEN == "test_token"  # noqa: S105


def test_config_missing_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    with pytest.raises(ValidationError):
        AppSettings()  # type: ignore[call-arg]
