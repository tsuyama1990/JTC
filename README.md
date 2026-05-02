# Japanese Stock Calendar Anomaly PoC

## Overview
This Proof of Concept (PoC) application establishes a highly reliable data pipeline dedicated to analyzing calendar anomalies within the Japanese equity market. The current phase (Cycle 01) focuses on fetching, transforming, and storing financial data securely and efficiently.

## Features
- **Automated Data Ingestion**: Seamlessly fetches the most recent 12 weeks of historical daily quotes directly from the live J-Quants API.
- **Resilient Pipeline**: Robust retry logic manages API rate limits (HTTP 429) and transient network failures gracefully without crashing.
- **Fast Transformations**: Utilizes Polars to compute statistical features such as day of week, month boundaries, and return metrics (daily, intraday, overnight).
- **Optimized Storage**: Saves the enriched dataset locally in compressed Parquet format, offering exceptional read performance.
- **SQL Analytics Ready**: Integrated DuckDB querying allows immediate, scalable SQL interaction with the processed Parquet files.

## Installation
Ensure you have Python 3.12+ and `uv` installed, then synchronize dependencies:
```bash
uv sync
```

## Usage
### Configuration
1. Copy the `.env.example` file to `.env`:
   ```bash
   cp .env.example .env
   ```
2. Open `.env` and set your J-Quants API Refresh Token:
   ```env
   JQUANTS_REFRESH_TOKEN=your_refresh_token_here
   ```

### Running the Pipeline
You can integrate this pipeline in your notebooks or python scripts. For example:

```python
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import transform_quotes
from src.storage.repository import StorageRepository
import os

# 1. Fetch
client = JQuantsClient(os.getenv("JQUANTS_REFRESH_TOKEN"))
raw_quotes = client.fetch_historical_quotes()

# 2. Transform
enriched_df = transform_quotes(raw_quotes)

# 3. Store
repo = StorageRepository()
repo.save_parquet(enriched_df, "data/processed_quotes.parquet")

# 4. Query
df = repo.query_duckdb("SELECT * FROM 'data/processed_quotes.parquet' LIMIT 5")
print(df)
```

## Structure
- `src/domain_models/`: Contains the strict Pydantic models for configuration and data quotes.
- `src/ingestion/`: API client to communicate securely with J-Quants.
- `src/processing/`: Polars-based analytics engine.
- `src/storage/`: Parquet saving capabilities and DuckDB integration.
