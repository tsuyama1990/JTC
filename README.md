# J-Quants Data ETL Pipeline

An automated data pipeline for extracting daily Japanese stock quotes from the J-Quants API, performing complex financial transformations using Polars, and storing the enriched data efficiently into Parquet files powered by DuckDB.

## Features
- **Robust API Ingestion**: Authenticates and retrieves the last 12 weeks of historical stock data, equipped with intelligent retry strategies (exponential backoff) using `tenacity` for resilience against rate limits and network errors.
- **Strict Data Contracts**: Enforces robust schema validation at the boundaries using Pydantic, ensuring data integrity across transformations.
- **High-Performance Transforms**: Utilizes `polars` to generate fast, memory-efficient feature engineering (e.g., intraday returns, day-of-week, month boundaries).
- **Embedded Analytics Storage**: Leverages DuckDB to query extremely compressed Parquet outputs directly, offering an abstraction for subsequent analytical layers.
- **Interactive UAT**: Provides a completely reactive User Acceptance Testing pipeline using Marimo notebooks.

## Installation

Ensure you have Python 3.12+ and `uv` installed.

1. Clone the repository and initialize the project:
   ```bash
   uv sync
   ```
2. Configure environment settings:
   - Make a copy of `.env.example` as `.env`.
   - Provide your J-Quants API token:
   ```env
   JQUANTS_REFRESH_TOKEN=your_real_refresh_token_here
   ```

## Usage

### Using the Python Client (ETL workflow)

```python
import polars as pl
from domain_models.config import AppSettings
from jquants_client import JQuantsClient
from transformers import process_quotes
from repository import QuotesRepository

# 1. Initialize API Client
settings = AppSettings()
client = JQuantsClient(settings)

# 2. Fetch data (e.g., Toyota: 7203)
raw_quotes = client.fetch_quotes("72030")

# 3. Transform using Polars
enriched_df = process_quotes(raw_quotes)

# 4. Save to Parquet
repo = QuotesRepository()
repo.save(enriched_df)

# 5. Query using DuckDB
result = repo.query("SELECT date, daily_return FROM 'processed_quotes.parquet' LIMIT 5")
print(result)
```

### Running the User Acceptance Testing (UAT) Notebook

We utilize `marimo` to provide a highly interactive, reactive tutorial environment.

```bash
uv run marimo edit tests/uat/uat_verification.py
```

## Directory Structure

- `src/domain_models/`: Pydantic definitions serving as system boundaries (config, raw quotes, enriched quotes).
- `src/jquants_client.py`: Robust extraction engine from external HTTP REST APIs.
- `src/transformers.py`: Mathematical feature engineering powered by Polars.
- `src/repository.py`: Efficient local Parquet storage backed by DuckDB querying.
- `tests/`: Extensive Test-Driven Development suites separated by `unit`, `e2e`, and `uat`.
- `data/`: Local storage directory designated for processed Parquet files.
