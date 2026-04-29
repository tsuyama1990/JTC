# CYCLE01: User Acceptance Testing (UAT) Plan

## Test Scenarios

### Scenario ID: UAT-01-01
**Priority:** High
**Title:** Live API Ingestion and Authentication Degradation
**Description:** This scenario verifies that the system can successfully authenticate with the J-Quants API using valid credentials loaded from the environment, fetch the correct historical timeframe, and gracefully degrade or error handle when credentials are missing or invalid. In the UAT environment, the user will execute a Marimo notebook cell designed to initialize the `JQuantsClient`. The notebook must clearly show a successful connection status when a valid `.env` file is present. Conversely, if the `.env` file is hidden or the token is intentionally altered to be invalid, the cell execution must not crash the kernel. Instead, it must catch the authentication exception and present a clear, user-friendly markdown warning instructing the analyst on how to correctly configure the API keys. This ensures the system is robust against configuration errors, a common issue for non-developer end-users.

### Scenario ID: UAT-01-02
**Priority:** High
**Title:** Data Transformation and Feature Engineering Validation
**Description:** This scenario allows the quantitative analyst to visually verify that the Polars-based transformation engine has correctly appended the required analytical features to the raw stock quotes. Within the Marimo notebook, after the data fetching step, a cell will execute the `feature_engine` transformation. The notebook will render the head and tail of the resulting Polars DataFrame directly in the UI. The analyst will visually inspect specific columns: `day_of_week` (verifying it correctly maps dates to integers 1-5), `is_month_start`, `is_month_end`, and the return columns (`daily_return`, `intraday_return`, `overnight_return`). The UAT will also include an interactive filter inside the notebook, allowing the user to select a specific date and view the precise return calculations, ensuring transparency and building trust in the mathematical accuracy of the system.

### Scenario ID: UAT-01-03
**Priority:** Medium
**Title:** Persistent Storage and DuckDB Query Execution
**Description:** This scenario verifies the persistence layer. After data transformation, a notebook cell will trigger the storage module, saving the DataFrame to a local Parquet file. Immediately following this, the notebook will initialize an in-process DuckDB connection pointed at the newly created Parquet file. The analyst will execute a provided SQL cell containing an aggregate query (e.g., `SELECT day_of_week, AVG(daily_return) FROM quotes GROUP BY day_of_week`). The successful execution of this SQL query and the rendering of the grouped results will prove that the data pipeline has correctly formatted the data, saved it efficiently to disk, and established the necessary query interface required for the statistical analysis phase in Cycle 02.

## Behavior Definitions

```gherkin
Feature: J-Quants API Ingestion and Authentication
  As a quantitative analyst
  I want the system to securely authenticate and fetch live stock data
  So that I can base my anomaly research on accurate, up-to-date market information.

  Scenario: Successful data ingestion with valid credentials
    Given the system environment contains a valid `JQUANTS_REFRESH_TOKEN`
    When the user triggers the data ingestion module for the past 12 weeks
    Then the system should authenticate successfully with the J-Quants API
    And retrieve a dataset containing daily quotes
    And the dataset should not be empty
    And the latest date in the dataset should be within the last 5 trading days.

  Scenario: Graceful handling of missing credentials
    Given the system environment does NOT contain a `JQUANTS_REFRESH_TOKEN`
    When the user triggers the data ingestion module
    Then the system should catch the missing configuration error
    And output a user-friendly message prompting the creation of the `.env` file
    And the system kernel should not crash.

Feature: Feature Engineering for Calendar Anomalies
  As a quantitative researcher
  I want the raw stock data to be augmented with specific calendar and return features
  So that I can easily group and test data based on day-of-week or month-end effects.

  Scenario: Correct calculation of calendar flags and returns
    Given a raw quote dataset has been successfully ingested
    When the transformation engine processes the dataset
    Then the output dataset must contain a `day_of_week` column ranging from 1 to 5
    And it must contain boolean `is_month_start` and `is_month_end` columns
    And it must calculate `daily_return` as the percentage difference between the previous and current close
    And it must calculate `intraday_return` as the percentage difference between open and close.

Feature: Persistent Parquet Storage and DuckDB Integration
  As a data engineer
  I want the transformed data saved as a queryable Parquet file
  So that the analysis layer can efficiently run SQL aggregations without memory bloat.

  Scenario: Saving and querying transformed data
    Given the transformation engine has produced an enriched dataset
    When the storage module is invoked
    Then a `.parquet` file should be created on the local file system
    And the system must successfully establish a DuckDB connection to this file
    And executing a standard SQL `SELECT COUNT(*)` query via DuckDB must return the correct row count matching the ingested data.
```