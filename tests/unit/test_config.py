import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src.domain_models.config import AppSettings


def test_app_settings_valid() -> None:
    """Test AppSettings with a valid JQUANTS_REFRESH_TOKEN."""
    with patch.dict(os.environ, {"JQUANTS_REFRESH_TOKEN": "valid_token"}):
        settings = AppSettings()  # type: ignore[call-arg]
        assert settings.JQUANTS_REFRESH_TOKEN == "valid_token"  # noqa: S105


def test_app_settings_missing_token() -> None:
    """Test AppSettings raises an error when JQUANTS_REFRESH_TOKEN is missing."""
    with patch.dict(os.environ, clear=True):
        with pytest.raises(ValidationError) as exc_info:
            AppSettings()  # type: ignore[call-arg]

        assert "JQUANTS_REFRESH_TOKEN" in str(exc_info.value)
