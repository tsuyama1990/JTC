# CYCLE 02 UAT: Backtesting and Statistical Engine

## Test Scenarios

### UAT-C2-01: Statistical Significance Validation (Priority: High)
This scenario demonstrates that the system can mathematically verify an anomaly rather than relying on visual guesses. Using the historical dataset generated in Cycle 01, the user will execute the `statsmodels` pipeline against the data, comparing Monday returns to the baseline.
In the Marimo notebook (`tutorials/UAT_AND_TUTORIAL.py`), the user runs a cell targeting `day_of_week=1`. The expected output is a printed `StatResult` Pydantic model showing the p-value, t-statistic, and a boolean `is_significant` flag. This proves the system correctly isolates subsets of the Polars dataframe and feeds them into standard OLS regressions.

### UAT-C2-02: Algorithmic Backtesting Execution (Priority: Critical)
This scenario acts as the core deliverable for the quantitative researcher. It proves that the abstract anomalies can be converted into a simulated trading strategy. The user defines a simple strategy: "Buy on Monday Open, Sell on Friday Close".
The notebook executes the `vectorbt` backtesting engine. The user will be amazed to see a comprehensive statistical tear sheet output, including Sharpe Ratio, Total Return %, and Win Rate, all automatically calculating transaction fees. It proves the Polars -> VectorBT conversion logic works seamlessly on large datasets.

### UAT-C2-03: Empty Dataset Handling (Priority: Medium)
This scenario ensures robustness. If a user queries the DB for a stock ticker that has no data, or a date range where the market was closed, the backtesting engine must not crash with an obscure Pandas KeyError. Instead, it must gracefully fail with a clear validation error.

## Behavior Definitions

```gherkin
Feature: Statistical Analysis and Algorithmic Backtesting
  In order to prove a calendar anomaly works in reality
  As a quantitative researcher
  I want to run statistical tests and historical backtests with fees

  Scenario: Run a statistical T-Test on Monday returns
    Given a valid dataset of Japanese stock returns resides in DuckDB Parquet storage
    When I trigger the statistical analysis module for "day_of_week=1" (Monday)
    Then the system should compute an OLS or T-Test using statsmodels
    And output a valid "StatResult" model
    And the model must explicitly declare if the anomaly is statistically significant

  Scenario: Execute a VectorBT backtest with transaction costs
    Given a valid dataset of Japanese stock returns
    And a predefined entry signal for Monday and an exit signal for Friday
    When I trigger the backtesting engine with a fee rate of 0.1%
    Then VectorBT should process the portfolio from signals
    And the system should output a "BacktestMetrics" model
    And the model must include a valid Sharpe Ratio, Win Rate, and Total Return
    And the Total Return must reflect the deducted transaction fees

  Scenario: Handling empty data gracefully
    Given an empty Polars dataframe
    When I attempt to run the statistical analysis or backtest
    Then the system should raise a clear ValueError "Dataset is empty"
    And prevent downstream libraries from crashing with generic indexing errors
```
