# Master Plan: User Acceptance Testing and Tutorials

## Tutorial Strategy

The primary goal of the User Acceptance Testing (UAT) is to ensure that quantitative researchers and analysts can easily verify the calendar anomaly capabilities of the system. To achieve this, we will use **Marimo**, a modern reactive Python notebook framework.

We will provide a single, comprehensive tutorial file that acts as the executable testbed for all UAT scenarios outlined in Cycle 01 and Cycle 02. This ensures simplicity—users only need to open one file to interact with the entire pipeline.

The tutorial will support two execution modes:
1. **Mock Mode (Default/CI)**: For users testing the notebook without a valid J-Quants API key, the tutorial will gracefully degrade to using synthetic data or cached mock Parquet files. This prevents the notebook from crashing during automated testing or when shared publicly.
2. **Real Mode**: If a valid `.env` file with `JQUANTS_REFRESH_TOKEN` is detected, the tutorial will pull live historical data for the last 12 weeks from the J-Quants API, demonstrating the system's actual real-world capabilities.

## Tutorial Plan

A **SINGLE** Marimo notebook will be created at `tutorials/UAT_AND_TUTORIAL.py`.

It will contain the following structured sections (cells):

### 1. Quick Start: Environment & Authentication
- **Action**: Load the `AppSettings` via the Pydantic configuration model.
- **Verification**: Display a green success badge if the credentials load successfully (without showing the actual token), or display a clear instruction to create the `.env` file if they are missing.

### 2. Core: Data Ingestion and Transformation
- **Action**: Execute the data fetching logic (mock or real) to retrieve Japanese stock quotes.
- **Transformation**: Run the Polars transformation to append `day_of_week` and return metrics.
- **Verification**: Render the head of the Polars dataframe directly in the notebook to visually verify the column structures.
- **Action**: Save the output to a temporary Parquet file and immediately query it using DuckDB to show row counts.

### 3. Advanced: Statistical Analysis
- **Action**: Filter the dataframe for Mondays using Polars/DuckDB.
- **Analysis**: Pass the filtered data into the `statsmodels` engine.
- **Verification**: Render the `StatResult` Pydantic model showing the p-value and T-statistic, determining if Monday returns are anomalous.

### 4. Advanced: Algorithmic Backtesting
- **Action**: Define a simple "Buy Monday Open, Sell Friday Close" signal.
- **Simulation**: Pass the dataset and signals into the `vectorbt` backtesting engine with a 0.1% transaction fee applied.
- **Verification**: Render the `BacktestMetrics` model, explicitly displaying the Sharpe Ratio, Win Rate, and Total Return.

## Tutorial Validation

To validate the tutorial:
1. The developer must be able to run `uv run marimo edit tutorials/UAT_AND_TUTORIAL.py` without any initial syntax errors.
2. Every cell must execute sequentially without raising unhandled exceptions (especially `KeyError` or `ValueError` due to missing data).
3. The mock logic must successfully engage when the environment variable is omitted.
