from pathlib import Path

import polars as pl
import pytest

from src.core.exceptions import StorageError
from src.storage.repository import DataRepository


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({
        "day_of_week": [1, 2, 3],
        "daily_return": [0.01, -0.02, 0.05]
    })

def test_save_and_query_repository(tmp_path: Path, sample_df: pl.DataFrame) -> None:
    file_path = tmp_path / "test_data.parquet"
    repo = DataRepository(file_path)

    # Test saving
    repo.save_processed_quotes(sample_df)
    assert file_path.exists()

    # Test querying
    result_df = repo.query_quotes("SELECT * FROM {table} WHERE day_of_week = 1")
    assert len(result_df) == 1
    assert result_df["daily_return"][0] == 0.01

def test_query_non_existent_file(tmp_path: Path) -> None:
    file_path = tmp_path / "non_existent.parquet"
    repo = DataRepository(file_path)

    with pytest.raises(StorageError, match="does not exist"):
        repo.query_quotes("SELECT * FROM {table}")

def test_save_empty_df_does_not_create_file(tmp_path: Path) -> None:
    file_path = tmp_path / "test_data.parquet"
    repo = DataRepository(file_path)

    empty_df = pl.DataFrame({"a": []})
    repo.save_processed_quotes(empty_df)
    assert not file_path.exists()
