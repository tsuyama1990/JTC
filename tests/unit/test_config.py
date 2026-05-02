import os

import pytest
from pydantic import ValidationError

from src.core.config import AppSettings


def test_app_settings_valid() -> None:
    os.environ["JQUANTS_REFRESH_TOKEN"] = "test_token"
    settings = AppSettings()
    assert settings.JQUANTS_REFRESH_TOKEN == "test_token"
    del os.environ["JQUANTS_REFRESH_TOKEN"]


def test_app_settings_missing_token() -> None:
    if "JQUANTS_REFRESH_TOKEN" in os.environ:
        del os.environ["JQUANTS_REFRESH_TOKEN"]

    with pytest.raises(ValidationError):
        AppSettings()
