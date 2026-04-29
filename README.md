# Quantitative Pipeline (CYCLE01)

## Overview
This incredibly robust, highly optimized quantitative pipeline provides a foundational mechanism for automatically retrieving, completely cleaning, correctly enriching, and securely permanently storing historical daily Japanese stock price quotes.

## Features
- Integrates autonomously with the J-Quants API.
- Implements strict, highly mathematical Data Validation using Pydantic.
- Calculates comprehensive critical features (`daily_return`, `intraday_return`, `overnight_return`, `day_of_week`, `is_month_start`, `is_month_end`) using the highly performant Polars engine.
- Implements highly resilient permanent local storage using DuckDB perfectly operating over heavily compressed Parquet files.

## Installation
Ensure you have `uv` beautifully installed.

```bash
uv sync
```

## Usage
Currently exposed purely via core functional modules perfectly prepared for the subsequent integration cycle. Ensure your highly secret API keys are accurately configured securely in `.env`.

```python
import os
from src.ingestion.jquants_client import JQuantsClient
from src.processing.transformers import process_quotes
from src.storage.repository import QuotesRepository

token = os.environ.get("JQUANTS_REFRESH_TOKEN", "")

# 1. Extraction
client = JQuantsClient(token)
raw_quotes = client.fetch_historical_quotes_12_weeks()

# 2. Transformation
processed_quotes = process_quotes(raw_quotes)

# 3. Storage
repo = QuotesRepository("data/processed_quotes.parquet")
repo.save(processed_quotes)
```

## Directory Structure
- `src/domain_models/`: Highly strict configuration and domain type models.
- `src/ingestion/`: Highly resilient external API fetching and intelligent backoff.
- `src/processing/`: Highly optimized Polars transformations.
- `src/storage/`: DuckDB seamlessly querying Parquet files.
