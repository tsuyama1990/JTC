# J-Quants Stock Data ETL Pipeline

This project implements a highly robust ETL pipeline for fetching, transforming, and querying Japanese stock market data via the J-Quants API. It leverages Polars for hyper-fast mathematical transformations and Parquet/DuckDB for compressed, SQL-queriable data storage.

## Features
- **Data Ingestion**: Reliably fetches daily quotes from the J-Quants API, utilizing exponential backoff (`tenacity`) to gracefully handle rate limits.
- **Transformation Engine**: Computes daily returns, intraday returns, overnight returns, and extracts critical date features like month boundaries and days of the week using `Polars`.
- **Validation**: Strict schema enforcement across the entire data lifecycle via `Pydantic` guarantees absolute data integrity.
- **Storage**: Automatically compresses data into highly performant `.parquet` files.
- **Querying**: Seamless integration with `DuckDB` allowing you to perform standard SQL queries directly against the parquet files without loading them into memory.

## Setup Instructions

1. **Install dependencies**
   ```bash
   uv sync
   ```
2. **Configure Environment**
   You need a J-Quants API Refresh Token to use the live pipeline.
   Copy the `.env.example` file to a new file named `.env`:
   ```bash
   cp .env.example .env
   ```
   Open `.env` and paste your actual Refresh Token:
   `JQUANTS_REFRESH_TOKEN=your_real_token_here`

## Usage

You can run the User Acceptance Testing (UAT) script to see the entire pipeline execute from API ingestion to final DuckDB SQL queries:

```bash
PYTHONPATH=. uv run python tests/uat/uat_verification.py
```

### Testing the Codebase
To run the automated suite:
```bash
PYTHONPATH=. uv run pytest --cov=src
```
