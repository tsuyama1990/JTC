# CYCLE 01 SPECIFICATION: Data Pipeline Construction (ETL & Storage)

## 1. Summary
The objective of Cycle 01 is to establish the fundamental data pipeline for the Japanese Stock Calendar Anomaly Verification System. This cycle focuses on securely authenticating against the real J-Quants API (Free tier) to acquire data for a rolling 12-week window. Once the data is successfully ingested without violating rate limits, the module must utilize the Polars library to calculate core financial features, specifically the day-of-the-week flag (1-5), beginning and end-of-month flags, as well as calculate previous-day return, intraday return, and overnight return. Finally, the transformed dataset will be safely serialized and stored as local `.parquet` files, exposing an interface for DuckDB to query the data efficiently. This cycle mandates strict credential hygiene, robust exception handling for network calls using retry logic, and an initial setup of Pydantic-based domain models to strictly type check both API inputs and transformed outputs.

## 2. Infrastructure & Dependencies

### A. Project Secrets (`.env.example`)
External Services Discovered: J-Quants API.
The Coder must append the following to the `.env.example` file:
```env
# Target Project Secrets
JQUANTS_REFRESH_TOKEN=dummy_refresh_token_here
```

### B. System Configurations (`docker-compose.yml`)
There are no external complex services required like PostgreSQL or Redis since the architecture relies on local Parquet files and DuckDB. However, if any baseline variables are required for testing configuration, they should be set. Since `docker-compose.yml` does not exist yet, you must instruct the Coder to ignore this step for Cycle 01, relying purely on the local `.venv` and `pyproject.toml` configurations.

### C. Sandbox Resilience (CRITICAL TEST STRATEGY)
**Mandate Mocking:** The Coder MUST ensure that *all external API calls relying on the newly defined secrets in `.env.example` MUST be mocked in unit and integration tests (using `unittest.mock` or `pytest-mock`)*. The CI Sandbox will not possess the real API keys during the autonomous evaluation phase. If tests attempt real network calls to SaaS providers without valid `.env` values, the pipeline will fail and cause an infinite retry loop.
*Note:* A specific "Live Test" can be created but it must be marked appropriately (e.g., `@pytest.mark.skipif`) so it only runs when a valid token is present in the local environment, thereby preventing CI breakage.

## 3. System Architecture
The following structure outlines the files that must be created or modified during this cycle:

```text
thejtc/
├── dev_documents/
├── **src/**
│   ├── **__init__.py**
│   ├── **config.py**           # Pydantic BaseSettings for App/Environment Config
│   ├── **domain/**             # Pydantic models for type safety
│   │   ├── **__init__.py**
│   │   ├── **jquants.py**      # Raw J-Quants API response schemas
│   │   └── **features.py**     # Schemas for transformed data
│   ├── **ingestion/**          # API Clients and Authentication
│   │   ├── **__init__.py**
│   │   ├── **auth.py**         # Handles JQUANTS_REFRESH_TOKEN auth
│   │   └── **client.py**       # Fetches quotes using tenacity retries
│   ├── **transform/**          # Polars transformation logic
│   │   ├── **__init__.py**
│   │   └── **features.py**     # Calculates return metrics and calendar flags
│   ├── **storage/**            # Local Persistence and DuckDB interface
│   │   ├── **__init__.py**
│   │   └── **parquet_db.py**   # Save/Load Parquet and DuckDB querying
├── **tests/**
│   ├── **__init__.py**
│   ├── **conftest.py**         # Pytest fixtures, mock env setups
│   ├── **test_ingestion.py**   # Live & mocked API tests
│   ├── **test_transform.py**   # Unit tests for Polars transformations
│   └── **test_storage.py**     # DuckDB and Parquet tests (using tmp_path)
├── .env.example
├── pyproject.toml
└── README.md
```

## 4. Design Architecture
The system is fully designed by Pydantic-based schema.

### Domain Concepts
- **`config.py`**: Defines `AppSettings` inheriting from `pydantic_settings.BaseSettings` with strict validation (`extra="forbid"`). It ensures `JQUANTS_REFRESH_TOKEN` is loaded.
- **`domain/jquants.py`**: Defines `JQuantsQuote` mapping to the JSON fields returned by the daily quotes API (e.g., `Date`, `Code`, `Open`, `High`, `Low`, `Close`, `Volume`). This acts as the single source of truth for the raw ingestion boundary.
- **`domain/features.py`**: Defines `TransformedQuote`, which extends the raw data to include explicit fields for `day_of_week` (1=Monday, 5=Friday), `is_month_start` (boolean), `is_month_end` (boolean), `daily_return` (float), `intraday_return` (float), and `overnight_return` (float).

### Key Invariants, Constraints, and Validation Rules
- All Pydantic models must enforce strict type checking (`model_config = ConfigDict(extra='forbid')`).
- Time series data must never be allowed to contain null values in the essential price columns (`Open`, `Close`). Rows with missing prices should be either filled or dropped during the Polars transformation phase.
- The `JQUANTS_REFRESH_TOKEN` must be strictly validated for length (e.g., greater than 10 characters) to prevent dummy strings from passing validation in production code.

### Expected Consumers
- The primary consumer for `domain/features.py` and `storage/parquet_db.py` will be the Cycle 02 statistical testing engine.

## 5. Implementation Approach

1.  **Configuration and Domain Modeling**:
    -   Create `src/config.py` and define `AppSettings` utilizing `pydantic_settings`.
    -   Create `src/domain/jquants.py` and map out the JSON response structure of the J-Quants API daily quotes endpoint.
    -   Create `src/domain/features.py` to define the schema for the Polars dataframe output.
2.  **API Ingestion**:
    -   Implement `src/ingestion/auth.py` to exchange the `JQUANTS_REFRESH_TOKEN` for an `idToken`. Use `tenacity.Retrying` for robustness.
    -   Implement `src/ingestion/client.py`. Create a method that calculates a relative 12-week window (from today backwards) and iterates over the required endpoints to fetch daily quotes. Implement rate-limiting compliance (e.g., small sleeps or backoff) to satisfy the Free Tier limits. Ensure zero credentials are ever logged.
3.  **Data Transformation (Polars)**:
    -   Implement `src/transform/features.py`. Create a function that accepts raw quote data (as a list of Pydantic models or raw dicts).
    -   Use `pl.from_dicts()` to convert to a Polars DataFrame.
    -   Compute the required calendar flags using Polars' native datetime operations (`pl.col("Date").dt.weekday()`, etc.).
    -   Compute financial returns using Polars expressions (e.g., `(Close - Open) / Open` for intraday).
4.  **Storage Engine**:
    -   Implement `src/storage/parquet_db.py`.
    -   Provide a function to write the Polars DataFrame to a target `.parquet` file path.
    -   Provide a function utilizing `duckdb.execute("SELECT * FROM read_parquet(?)", [file_path]).pl()` to retrieve the data safely without SQL injection vulnerabilities.

## 6. Test Strategy

### Unit Testing Approach (Min 300 words)
The unit testing approach focuses on the deterministic logic within the system. For the configuration module, tests will employ `pytest-mock` to clear the environment dictionary (`mocker.patch.dict(os.environ, clear=True)`) and simulate specific variables to guarantee that `AppSettings` raises validation errors when the required J-Quants token is absent.
For the transformation module, `test_transform.py` will supply handcrafted, highly predictable Polars DataFrames into the `src/transform/features.py` module. It will explicitly verify that a known Monday returns a `day_of_week` value of 1, and that a Friday returns 5. Financial calculations will be tested against manual assertions (e.g., Open 100, Close 110 must yield an intraday return of exactly 0.10). By doing so, any changes to the math or temporal logic will immediately fail the suite. The `tenacity` retry logic in the ingestion layer will also be unit-tested by mocking the underlying `requests.get` to throw a `Timeout` exception twice before returning a valid 200 response, ensuring the system recovers correctly and raises the appropriate errors if max retries are exceeded.

### Integration Testing Approach (Min 300 words)
Integration testing verifies that the boundaries interact correctly. `test_storage.py` will test the integration between the Polars output and the DuckDB read logic. By utilizing the Pytest `tmp_path` fixture, a temporary Parquet file will be written using `storage/parquet_db.py`. A subsequent read request will be executed to guarantee that all schema definitions, column types, and data integrity survive the serialization loop. This entirely avoids mutating the user's permanent disk storage.
For external APIs, `test_ingestion.py` will mock the network response payloads utilizing `pytest-mock` to replicate complex J-Quants payloads, ensuring the pipeline can parse them into `JQuantsQuote` models successfully without requiring an API key. Finally, an isolated Live E2E test will be constructed, guarded by a `@pytest.mark.skipif` decorator that checks for a valid local environment variable. If present, it will conduct a genuine end-to-end pull of 12 weeks of data and ensure the final transformed DuckDB output contains rows, validating the entire Cycle 01 requirement against production reality.
