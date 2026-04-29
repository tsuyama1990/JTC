# J-Quants Stock Pipeline

This project implements a high-performance, resilient data pipeline for retrieving, transforming, and querying historical Japanese stock quote data using the J-Quants API, Polars, and DuckDB.

## Features

- **Robust Ingestion**: Connects to the J-Quants API with automatic authentication handling, pagination, and built-in exponential backoff for rate limiting and temporary errors.
- **High-Speed Transformation**: Leverages Polars to quickly calculate critical financial features including:
  - Day-of-the-week indicators
  - Month-start and month-end boundary flags
  - Intraday, overnight, and daily return metrics
- **Efficient Storage**: Persists processed datasets efficiently into column-oriented Parquet files.
- **Embedded Analytics**: Uses DuckDB to provide zero-copy SQL analytics capabilities directly against the Parquet files for immediate querying without memory bloat.

## Installation

Ensure you have [uv](https://github.com/astral-sh/uv) installed, then run:

```bash
uv sync
cp .env.example .env
# Edit .env and provide your JQUANTS_REFRESH_TOKEN
```

## Usage

### Interactive Tutorial (UAT)

To see the pipeline in action, launch the interactive Marimo notebook tutorial:

```bash
uv run marimo edit tutorials/UAT_AND_TUTORIAL.py
```

### Programmatic Usage

```python
from src.config.settings import get_settings
from src.ingestion.jquants_client import JQuantsClient
from src.transformation.feature_engine import convert_to_polars, compute_features
from src.storage.parquet_duckdb import save_to_parquet, init_duckdb_with_parquet

settings = get_settings()
client = JQuantsClient(settings.jquants_refresh_token)
quotes = client.get_historical_quotes(weeks=12)
df_raw = convert_to_polars(quotes)
df_features = compute_features(df_raw)
save_to_parquet(df_features, "data/quotes.parquet")
conn = init_duckdb_with_parquet("data/quotes.parquet", table_name="quotes")
```
