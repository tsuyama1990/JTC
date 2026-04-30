# Data Pipeline for Japanese Stock Market

This project implements a highly robust, reliable, and secure ETL (Extract, Transform, Load) data pipeline exclusively dedicated to fetching, transforming, and persistently storing Japanese stock market data via the J-Quants API. It forms the foundational layer for analyzing calendar anomalies within financial time series.

## Features
- **Data Ingestion (Extract)**: Securely authenticates and retrieves raw historical daily price quotes from the J-Quants API, supporting exponential backoff retry mechanisms for resilience against network and rate-limit issues.
- **Data Transformation (Transform)**: Uses Polars for high-performance mathematical and statistical feature engineering, enriching data with features like `daily_return`, `intraday_return`, `overnight_return`, `day_of_week`, and month boundary flags.
- **Data Storage (Load)**: Persists processed data locally using highly-compressed Parquet files and leverages DuckDB for robust, SQL-like query capability.
- **Strict Data Validation**: Implements Pydantic domain models to enforce strict schema adherence and critical market invariants (e.g., `high` >= `low`).

## Installation

Ensure you have [uv](https://github.com/astral-sh/uv) installed, then run the following command to synchronize dependencies:

```bash
uv sync
```

### Configuration
1. Copy `.env.example` to `.env`.
2. Add your active J-Quants Refresh Token to `.env`:
   ```env
   JQUANTS_REFRESH_TOKEN=your_refresh_token_here
   ```

## Usage

You can use the built-in modules directly in your Python applications. Here's a brief snippet to get you started:

```python
from src.domain_models.config import AppSettings
from src.jquants_client import JQuantsClient
from src.transformers import transform_quotes
from src.repository import QuoteRepository

def run_pipeline():
    # 1. Configuration Check
    settings = AppSettings()

    # 2. Data Ingestion
    client = JQuantsClient(refresh_token=settings.JQUANTS_REFRESH_TOKEN)
    raw_quotes = client.fetch_quotes()

    # 3. Data Transformation
    processed_df = transform_quotes(raw_quotes)

    # 4. Data Storage
    repo = QuoteRepository(data_dir="data")
    repo.save_processed_quotes(processed_df)

    # 5. Verify & Query
    result = repo.query_quotes("SELECT * FROM 'data/processed_quotes.parquet' LIMIT 5")
    print(result)

if __name__ == "__main__":
    run_pipeline()
```

## Structure
- `src/domain_models/`: Pydantic definitions for configurations and precise data validation constructs.
- `src/jquants_client.py`: API integration with retry capabilities.
- `src/transformers.py`: Data enrichment using Polars expressions.
- `src/repository.py`: Fast DuckDB queries and Parquet data persistence.
- `tests/`: Comprehensive unit and e2e integration testing suite with complete API mocks.
