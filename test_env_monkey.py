import pytest
from pydantic import ValidationError
from src.core.config import AppSettings

def test_config():
    # If a .env file exists in the directory where pytest is run,
    # pydantic_settings will load it.
    pass
