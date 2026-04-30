from pathlib import Path

import duckdb
import polars as pl


class StorageRepository:
    def __init__(self, file_path: str) -> None:
        self.file_path = Path(file_path)
        # Ensure parent directory exists
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, df: pl.DataFrame) -> None:
        df.write_parquet(self.file_path)

    def query(self, sql_query: str) -> pl.DataFrame:
        con = duckdb.connect(database=":memory:")
        if self.file_path.exists():
            con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{self.file_path}')")  # noqa: S608

        result = con.execute(sql_query).pl()
        con.close()
        return result
