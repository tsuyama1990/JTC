import pytest
from pydantic import ValidationError

from src.domain_models.config import AppSettings


def test_scenario1_authentication_missing_token(monkeypatch):
    """
    GIVEN the user has deliberately NOT provided a valid JQUANTS_REFRESH_TOKEN in the .env file
    WHEN the user attempts to execute the primary data ingestion script
    THEN the highly strict Pydantic AppSettings configuration model must immediately and forcefully intercept
    AND throw a highly descriptive ValidationError
    AND system must terminate gracefully without making requests.
    """
    monkeypatch.delenv("JQUANTS_REFRESH_TOKEN", raising=False)
    with pytest.raises(ValidationError):
        AppSettings()
