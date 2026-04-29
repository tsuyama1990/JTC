# Japanese Stock Calendar Anomaly Validation System

A robust, mathematically sound Proof of Concept (PoC) for ingesting, transforming, and analyzing calendar anomalies within Japanese equity markets. It leverages modern Python tools (Polars, DuckDB, vectorbt) to process live data from the J-Quants API, performing rigorous statistical tests and backtesting trading strategies.

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)

## Key Features

- **Automated Data Pipeline**: Seamlessly fetches the last 12 weeks of daily quotes from the J-Quants API with robust retry logic and error handling.
- **High-Performance Transformations**: Utilizes Polars to rapidly compute daily, intraday, and overnight returns, along with calendar features (day-of-week, month-start/end).
- **Zero-Config Storage**: Transparently saves processed data as heavily compressed Parquet files queryable instantly via DuckDB.
- **Rigorous Statistical Validation**: Employs `statsmodels` to scientifically test hypotheses regarding market returns on specific days.
- **Realistic Backtesting**: Integrates `vectorbt` to simulate calendar-based trading strategies, perfectly accounting for transaction fees and slippage.

## Architecture Overview

The system strictly decouples the volatile external API integration from the highly sensitive internal statistical logic using a layered approach enforcing strict Pydantic data schemas.

```mermaid
graph TD
    A[Environment Variables .env] -->|Loaded securely| B(Configuration Layer Pydantic)
    B --> C{Data Ingestion Layer}
    C <-->|HTTP Requests| D((J-Quants API))
    C -->|Raw Data| E(Transformation Layer Polars)
    E -->|Processed Data| F[(Storage Layer Parquet & DuckDB)]
    F -->|Query Data| G(Analysis Layer statsmodels)
    F -->|Query Data| H(Backtesting Layer vectorbt)
    G --> I[Statistical Results]
    H --> J[Performance Metrics]
```

## Prerequisites

- Python 3.12+
- `uv` (for rapid dependency management)
- Optional: A valid J-Quants API free-tier account (Refresh Token) for live execution.

## Installation & Setup

1. Clone the repository:
```bash
git clone <repository_url>
cd <repository_directory>
```

2. Install dependencies using `uv`:
```bash
uv sync
```

3. Configure Environment Variables:
```bash
cp .env.example .env
```
Edit the `.env` file and append your J-Quants API refresh token: `JQUANTS_REFRESH_TOKEN=your_token_here`.

## Usage

### Quick Start with Marimo

The entire UAT and tutorial suite is packaged in a single, reactive Marimo notebook. It supports both Mock Mode (no API key required) and Real Mode.

```bash
uv run marimo edit tutorials/UAT_AND_TUTORIAL.py
```

### Standard Execution

Execute the main pipeline directly to fetch data, run statistics, and output backtest metrics to the console.

```bash
uv run python main.py
```

## Development Workflow

The project strictly enforces code quality using `ruff` (max complexity 10) and `mypy` (strict mode). All configurations are defined in `pyproject.toml`.

- **Run Linters and Type Checkers**:
```bash
uv run ruff check .
uv run mypy .
```

- **Run Tests (with Coverage)**:
```bash
uv run pytest
```
*Note: The test suite aggressively mocks external API calls to guarantee sandbox resilience and rapid execution.*

## Project Structure

```text
src/
├── core/            # Pydantic configuration & exceptions
├── domain/          # Strict Pydantic models & schemas
├── ingestion/       # J-Quants API client and fetch logic
├── processing/      # Polars transformations and feature engineering
├── storage/         # DuckDB repository and Parquet file I/O
└── analysis/        # statsmodels and vectorbt integrations
tests/               # Pytest suite with strict mocking
tutorials/           # Marimo UAT notebooks
dev_documents/       # Architectural plans and specifications
```

## License

MIT
