# CYCLE01: Data Pipeline Construction (ETL & Storage)

## Summary
The primary objective of CYCLE01 is to establish a robust, reliable data pipeline that retrieves, transforms, and stores historical daily stock quote data from the J-Quants API. This foundational cycle ensures that subsequent statistical and backtesting operations have access to high-quality, pre-processed datasets. Specifically, this cycle implements the Configuration Layer using Pydantic to securely manage the required environment variables (e.g., the J-Quants refresh token). It constructs the Ingestion Layer, capable of executing authenticated API calls to fetch the most recent 12 weeks of historical stock data, equipped with essential resilience features like retry logic to handle rate limiting and intermittent connectivity issues. Furthermore, it develops the Transformation Layer leveraging the high-performance Polars library. This layer computes critical financial features required for anomaly detection, including day-of-the-week flags, month-start and month-end indicators, as well as calculated daily, intraday, and overnight return metrics. Finally, CYCLE01 implements the Storage Layer, which persists these enriched Polars DataFrames into columnar Parquet files locally, and establishes a local DuckDB connection enabling fast, SQL-like analytical queries over the saved data. By the conclusion of this cycle, the system will autonomously ingest raw API data and produce analysis-ready datasets.

## Infrastructure & Dependencies

### A. Project Secrets (`.env.example`)
The Coder MUST append the following required external service configurations to the `.env.example` file.
```env
# Target Project Secrets
JQUANTS_REFRESH_TOKEN=
```

### B. System Configurations (`docker-compose.yml`)
The Coder MUST update the `docker-compose.yml` to include explicit local volume mounts for the data directory to ensure that DuckDB databases and Parquet files persist between container restarts.
```yaml
    volumes:
      - ./data:/app/data
```
Explicitly instruct the Coder to preserve valid YAML formatting and idempotency (do not overwrite existing agent configs).

### C. Sandbox Resilience (CRITICAL TEST STRATEGY)
**Mandate Mocking:** The Coder MUST explicitly ensure that *all external API calls relying on the newly defined secrets in `.env.example` MUST be mocked in unit and integration tests (using `unittest.mock` or `pytest-mock`)*.

*Why:* The Sandbox will not possess the real API keys during the autonomous evaluation phase. If tests attempt real network calls to SaaS providers without valid `.env` values, the pipeline will fail and cause an infinite retry loop. A separate, specific live-test fixture should be created that is explicitly ignored or skipped by default in the CI pipeline unless a real API key is detected.

## System Architecture
The file structure to create/modify in this cycle is defined below. Files marked in **bold** represent those that must be implemented or modified during this cycle.

```text
├── .env.example
├── pyproject.toml
├── src/
│   ├── __init__.py
│   ├── **config/**
│   │   ├── **__init__.py**
│   │   └── **settings.py**
│   ├── **domain/**
│   │   ├── **__init__.py**
│   │   ├── **raw_quote.py**
│   │   └── **transformed_quote.py**
│   ├── **ingestion/**
│   │   ├── **__init__.py**
│   │   └── **jquants_client.py**
│   ├── **transformation/**
│   │   ├── **__init__.py**
│   │   └── **feature_engine.py**
│   └── **storage/**
│       ├── **__init__.py**
│       └── **parquet_duckdb.py**
└── tests/
    ├── __init__.py
    ├── **conftest.py**
    ├── **test_ingestion.py**
    └── **test_transformation.py**
```

The data flow will proceed strictly from Configuration to Ingestion, then Transformation, and finally Storage, with Pydantic models serving as the unyielding data contracts between each stage.

## Design Architecture

This system relies on robust Pydantic-based schemas to define domain concepts and ensure data integrity.

- **`src/config/settings.py`**:
  - **Concept**: Centralised configuration management.
  - **Invariants**: Must load environment variables automatically. The `jquants_refresh_token` must be present when initialized for real data ingestion.
  - **Consumers/Producers**: Used universally by the `jquants_client` and other initialisation scripts.

- **`src/domain/raw_quote.py`**:
  - **Concept**: Represents the exact structure of a daily stock quote as returned by the J-Quants API.
  - **Constraints**: Includes strict types for `Date` (datetime or string depending on parsing), `Open`, `High`, `Low`, `Close`, and `Volume` (floats/integers).
  - **Validation**: Enforces non-negative values for volume and ensures prices are logically consistent (e.g., High >= Low).
  - **Consumers/Producers**: Produced by `jquants_client`, consumed by `feature_engine`.

- **`src/domain/transformed_quote.py`**:
  - *DEPRECATED/REMOVED for Performance:* Do not instantiate a `TransformedQuote` Pydantic model. To achieve optimal C-level execution speeds over millions of rows, the enriched representation must strictly exist as a native Polars DataFrame with an explicitly defined and validated Polars Schema.
  - **Constraints**: The Polars Schema must include the base attributes and append `day_of_week` (pl.Int8), `is_month_start` (pl.Boolean), `is_month_end` (pl.Boolean), `daily_return` (pl.Float64), `intraday_return` (pl.Float64), and `overnight_return` (pl.Float64).
  - **Validation**: Ensure no null divisions occur during return calculations.
  - **Consumers/Producers**: Produced by `feature_engine`, consumed by `parquet_duckdb`.

## Implementation Approach

1.  **Configuration Setup**: Begin by establishing `src/config/settings.py` using `pydantic-settings`. Define an `AppSettings` class that explicitly requires `JQUANTS_REFRESH_TOKEN`. Update `.env.example` accordingly.
2.  **Domain Schemas**: Implement `RawQuote` and `TransformedQuote` in `src/domain/`. Ensure robust Pydantic field validations are present.
3.  **API Client Construction**:
    - Implement `src/ingestion/jquants_client.py`.
    - Create a class `JQuantsClient` initialized with the refresh token.
    - Implement an internal method to exchange the refresh token for a short-lived session ID via the `/token/auth_user` and `/token/auth_refresh` endpoints.
    - Implement the `get_historical_quotes(weeks: int = 12)` method. This method must calculate the relative start and end dates dynamically, perform the API request, handle pagination if necessary, and parse the JSON responses into a list of `RawQuote` Pydantic models.
    - Wrap the HTTP requests (using `requests` or `httpx`) with a robust retry mechanism (e.g., using `tenacity`) to handle 429 Rate Limit or 50x errors.
4.  **Feature Transformation Engine**:
    - Implement `src/transformation/feature_engine.py`.
    - Accept the list of `RawQuote` objects and immediately convert them into a Polars DataFrame for high-speed manipulation.
    - Use native Polars expressions to calculate the required fields: `day_of_week`, `is_month_start`, `is_month_end`, `daily_return`, `intraday_return`, and `overnight_return`.
    - Strictly maintain and return the validated Polars DataFrame. DO NOT convert the dataframe back into row-by-row Pydantic models.
5.  **Storage Layer**:
    - Implement `src/storage/parquet_duckdb.py`.
    - Provide a method to accept the Polars DataFrame and save it directly to a local disk path (e.g., `data/processed_quotes.parquet`) using Polars' high-speed native parquet writer.
    - Provide a method to initialise a DuckDB connection and register the Parquet file, returning a queryable DuckDB connection object.

## Test Strategy

**Unit Testing Approach (Min 300 words):**
The unit testing strategy focuses heavily on isolating the complex business logic and external dependencies. For the `jquants_client`, tests will utilise `pytest-mock` or `responses` to intercept HTTP calls. We will mock the authentication exchange and the data retrieval endpoints. Tests will assert that the client constructs the correct URLs based on the 12-week relative date logic, properly includes the Authorization headers, and correctly parses a synthetic JSON response into `RawQuote` models. Furthermore, specific unit tests will simulate API rate-limiting errors (HTTP 429) to explicitly verify that the implemented retry logic functions correctly without crashing. For the `feature_engine`, unit tests will instantiate small, hardcoded Polars DataFrames with known edge cases (e.g., missing days, end-of-month transitions, zero-volume days). The tests will assert that the computed `day_of_week` strictly maps to the expected integer, the month-boundary flags trigger correctly, and the mathematical return formulas are accurate.

**Integration Testing Approach (Min 300 words):**
Integration testing will focus on the seamless data flow between the implemented layers. A primary integration test will wire together the `jquants_client` (using a mocked response or synthetic data generator), the `feature_engine`, and the `parquet_duckdb` storage module. The test will push a batch of data through the entire pipeline and then query the resulting DuckDB instance to assert that the exact number of rows and columns are present and correctly typed.
To comply with the **DB Rollback Rule**, tests interacting with the storage layer will use the `pytest` `tmp_path` fixture to dynamically generate temporary directories for Parquet file creation. An in-memory DuckDB connection will be instantiated to query these temporary files. Upon test completion, the `tmp_path` will be automatically destroyed, ensuring lightning-fast teardown and absolute prevention of persistent state bleeding between test runs. Additionally, a dedicated "Live API" integration test will be provided, strictly gated behind an environment variable check (e.g., `if not os.getenv("JQUANTS_REFRESH_TOKEN"): pytest.skip()`), allowing developers to perform E2E verification against the real service only when explicitly desired.
