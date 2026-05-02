# Japanese Equity Anomaly Backtesting Pipeline (PoC)

## Overview
This system establishes a highly robust ETL (Extract, Transform, Load) data pipeline specifically focused on continuously gathering, mathematically processing, and securely storing Japanese stock market data. This foundation powers subsequent quantitative backtesting to analyze and discover "Calendar Anomalies" within the financial timeseries.

## Features
- **Reliable Data Extraction**: Interacts directly with the J-Quants Live API to systematically gather daily market quotes seamlessly targeting the previous 12 trading weeks dynamically. Features built-in robust exponential backoff behavior mitigating typical HTTP constraints.
- **Advanced Feature Engineering**: Leverages native, optimized calculations via `Polars` handling complex numerical return logic (`daily_return`, `intraday_return`, `overnight_return`), week-day classification tags, and complex timeframe-gap metrics (`month_start` and `month_end`).
- **High-Performance Storage Repository**: Stores final enriched matrices in compressed `Parquet` format ensuring extremely fast, SQL-queriable IO powered by `DuckDB`.
- **Security by Design**: Complete separation of secrets via environment injection mechanisms protecting tokens from leakages during operations.

## Installation
Ensure `python >= 3.12` and the `uv` package manager are locally available.

1.  **Clone the repository.**
2.  **Install dependencies using UV:**
    ```bash
    uv sync
    ```
3. **Configure Environment Secrets:**
    Copy the `.env.example` file locally and input your active J-Quants Refresh Token.
    ```bash
    cp .env.example .env
    ```

## Usage

You can safely explore and execute the system offline to verify schema integration and transformations by launching the interactive `marimo` notebook in either "Mock Mode" (simulated dataset) or "Live Mode" (using your `.env` J-Quants API token).

```bash
uv run marimo edit tutorials/UAT_AND_TUTORIAL.py
```

## Directory Structure
- `src/core/`: Environment and specific domain exceptions definitions.
- `src/domain_models/`: Pydantic definitions strictly maintaining logical mathematical validations and schemas.
- `src/ingestion/`: J-Quants authenticating web-client featuring HTTP-request back-offs via Tenacity.
- `src/processing/`: Main analytical engine executing rapid series generation via Polars.
- `src/storage/`: DuckDB enabled Parquet file persistence interfaces.
