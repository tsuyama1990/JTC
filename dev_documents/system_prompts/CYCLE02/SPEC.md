# CYCLE02: Backtesting and Statistical Validation (Analysis)

## Summary
The primary objective of CYCLE02 is to implement the analytical core of the system, verifying the existence of calendar anomalies using rigorous statistical methods and simulating the financial outcomes of these anomalies through algorithmic backtesting. Building upon the robust data pipeline established in CYCLE01, this cycle develops two main analytical engines. First, the Statistical Testing module leverages `statsmodels` to query the enriched datasets stored in DuckDB, explicitly testing whether the distribution of returns on specific days (e.g., Mondays) is statistically significantly different from the rest of the week. Second, the Backtesting Engine utilises the high-performance `vectorbt` library. It ingests the Polars DataFrames, translates specific calendar anomalies into trading signals (e.g., Buy on Monday Open, Sell on Friday Close), and executes a vectorised backtest. This simulation critically includes real-world constraints such as transaction fees and slippage to produce realistic performance metrics, including Sharpe Ratios, Win Rates, and Total Returns. Strict Pydantic models will govern the output of these engines, ensuring the results are highly structured and ready for presentation in the final User Acceptance Testing notebook.

## Infrastructure & Dependencies

### A. Project Secrets (`.env.example`)
There are no new external API secrets required for CYCLE02. The system will continue to utilize the `JQUANTS_REFRESH_TOKEN` established in CYCLE01 for any end-to-end runs.

### B. System Configurations (`docker-compose.yml`)
The Coder MUST update `docker-compose.yml` to expose the default Marimo notebook port. This allows the interactive UAT notebook to be accessed if the system is executed within a containerized environment.
```yaml
    ports:
      - "2718:2718"
```
Explicitly instruct the Coder to preserve valid YAML formatting and idempotency (do not overwrite existing agent configs).

### C. Sandbox Resilience (CRITICAL TEST STRATEGY)
**Mandate Mocking:** While CYCLE02 does not directly invoke external APIs, its integration tests depend on the outputs of CYCLE01. The Coder MUST ensure that all integration tests for CYCLE02 utilise *cached, mock Parquet files* or *synthetic DataFrames* rather than triggering the CYCLE01 live ingestion process. Attempting live ingestion during automated testing without a valid API key will cause pipeline failures.

## System Architecture
The file structure to create/modify in this cycle is defined below. Files marked in **bold** represent those that must be implemented or modified during this cycle.

```text
├── src/
│   ├── __init__.py
│   ├── config/
│   ├── domain/
│   ├── ingestion/
│   ├── transformation/
│   ├── storage/
│   ├── **analysis/**
│   │   ├── **__init__.py**
│   │   ├── **stats_tester.py**
│   │   └── **backtester.py**
│   └── **pipeline.py**
└── tests/
    ├── __init__.py
    ├── conftest.py
    └── **test_analysis.py**
└── tutorials/
    └── **UAT_AND_TUTORIAL.py**
```

The data flow begins at the Storage Layer (DuckDB/Parquet from CYCLE01), feeds into the Statistical Tester and Backtester, and outputs structured Pydantic results.

## Design Architecture

This cycle relies on Pydantic schemas to strictly type the analytical outputs.

- **`StatResult` (Internal to `stats_tester.py` or `domain`)**:
  - **Concept**: Represents the outcome of a statistical hypothesis test (e.g., T-test) evaluating return distributions.
  - **Constraints**: Contains floats for `t_statistic` and `p_value`, a boolean `is_significant`, and a string `tested_anomaly` (e.g., "Monday Effect").
  - **Validation**: Enforces that the `p_value` is strictly between 0.0 and 1.0.
  - **Consumers/Producers**: Produced by `stats_tester`, consumed by the UI/Notebook.

- **`BacktestMetrics` (Internal to `backtester.py` or `domain`)**:
  - **Concept**: A summary of trading simulation performance.
  - **Constraints**: Contains floats for `total_return`, `sharpe_ratio`, `win_rate`, and `max_drawdown`.
  - **Validation**: Ensures percentage fields like `win_rate` fall between 0.0 and 1.0 (or 0-100%).
  - **Consumers/Producers**: Produced by `backtester`, consumed by the UI/Notebook.

- **Pipeline Integration**:
  - The `pipeline.py` serves as the orchestrator, importing modules from CYCLE01 and CYCLE02. It acts as the Facade, providing a single entry point to execute `run_ingestion()`, `run_transformation()`, `run_statistical_test()`, and `run_backtest()`.

## Implementation Approach

1.  **Statistical Testing Module**:
    - Implement `src/analysis/stats_tester.py`.
    - Create a function/class that accepts a DuckDB connection and a target day of the week.
    - Execute a SQL query to split the data: one set containing returns for the target day, and another for all other days.
    - Utilise `scipy.stats` or `statsmodels` to perform an independent T-test (or ANOVA) comparing the means of the two sets.
    - Construct and return the `StatResult` Pydantic model based on the calculated T-statistic and P-value, setting `is_significant` to True if `p_value < 0.05`.

2.  **Backtesting Engine**:
    - Implement `src/analysis/backtester.py`.
    - Import `vectorbt`.
    - Accept the enriched Polars DataFrame. Explicitly perform a high-efficiency conversion to Pandas (`.to_pandas()`) as `vectorbt` internally requires Pandas/NumPy structures.
    - Implement a signal generation logic: Create boolean arrays for `entries` (e.g., True when `day_of_week == 1` at the Open) and `exits` (e.g., True when `day_of_week == 5` at the Close).
    - Initialise the `vbt.Portfolio.from_signals` object. Crucially, pass the price series and strictly include `fees=0.001` (0.1% transaction fee) and defined slippage parameters to simulate real-world trading friction.
    - Extract the performance metrics from the `Portfolio` object and map them back into the strict `BacktestMetrics` Pydantic model for boundary validation.

3.  **Pipeline Orchestration**:
    - Implement `src/pipeline.py` to provide high-level orchestrating functions that tie all modules together, allowing a user to run the entire E2E process with a single command.

4.  **UAT & Tutorial Notebook Creation**:
    - Create `tutorials/UAT_AND_TUTORIAL.py` using Marimo.
    - Implement the "Mock Mode" gracefully degrading fallback, ensuring the notebook can be run locally or in CI without J-Quants credentials by generating synthetic data if `JQUANTS_REFRESH_TOKEN` is absent.

## Test Strategy

**Unit Testing Approach (Min 300 words):**
The unit tests for the analysis module require precise, deterministic inputs to verify the mathematical accuracy of the engines. For the `stats_tester`, tests will not rely on random stock data. Instead, two synthetic arrays will be provided: one designed to be statistically identical to the other (ensuring `p_value > 0.05`), and another explicitly skewed to have a significantly higher mean (ensuring `p_value < 0.05`). The test will assert that the `statsmodels` integration correctly calculates these known outcomes and populates the `StatResult` model accurately.
For the `backtester`, a small, deterministic Pandas DataFrame containing 10 rows of mock price data will be generated. The signals will be hardcoded (e.g., Buy on day 2, Sell on day 4). The test will manually calculate the expected total return, including the exact deduction of the 0.1% transaction fee. The test will then assert that the `vectorbt` engine output perfectly matches this manual calculation, proving that fees, slippage, and signal alignment are correctly configured within the engine.

**Integration Testing Approach (Min 300 words):**
Integration testing for CYCLE02 must ensure that the outputs of the Transformation and Storage layers (built in CYCLE01) are perfectly compatible with the Analysis engines. A test fixture will load a pre-generated, static Parquet file containing transformed stock data. The test will initialize a transient, in-memory DuckDB connection to this file (adhering strictly to the **DB Rollback Rule** to ensure zero state leakage). The `stats_tester` will be executed against this DuckDB connection, verifying that SQL queries execute correctly and data types map flawlessly into the statistical functions.
Subsequently, the same Parquet data will be loaded into a Polars DataFrame and passed to the `backtester`. The test will verify that the conversion from Polars to Pandas (if required by `vectorbt`) functions without memory errors or index misalignment, and that a complete backtest simulation completes successfully. Finally, a high-level test against `pipeline.py` will verify that the orchestrator can sequentially call all modules using the mock data without unhandled exceptions.