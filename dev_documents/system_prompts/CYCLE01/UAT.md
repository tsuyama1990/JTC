# CYCLE 01 UAT: Data Pipeline Construction

## Test Scenarios

### UAT-C1-01: Secure Environment Loading (Priority: High)
This scenario ensures that the system refuses to run without secure credentials, preventing accidental unauthenticated bursts against the API. The system must immediately crash or throw a clear validation error if `.env` does not contain `JQUANTS_REFRESH_TOKEN`.
When a user without `.env` executes the ingestion script, they should see a highly informative Pydantic validation error rather than a generic network crash or null pointer exception.
When a user with a valid `.env` file executes the script, the configuration should parse silently and successfully load the tokens into the application's configuration singleton without exposing them in stdout.

### UAT-C1-02: End-to-End Ingestion and Transformation (Priority: Critical)
This scenario acts as the core tutorial for the data pipeline. It demonstrates the system fetching a rolling 12-week period of historical stock data, performing the required Polars transformations (like calculating the intraday returns), and dumping it to a localized Parquet file.
Users will execute a single block in a Marimo notebook (`tutorials/UAT_AND_TUTORIAL.py`). They will visually confirm the output dataframe contains the required columns: `day_of_week`, `is_month_start`, and the three return metrics. This scenario guarantees that the ingestion layer can gracefully handle the live API limits and that Polars correctly calculates edge cases over weekends.

### UAT-C1-03: DuckDB Query Validation (Priority: Medium)
This scenario demonstrates that the Parquet output from the previous scenario is queryable. Users will execute a DuckDB SQL command directly against the generated Parquet file to aggregate data (e.g., calculating average returns per day of the week) inside the Marimo notebook, validating the column schemas and data integrity.

## Behavior Definitions

```gherkin
Feature: Secure Data Ingestion and Transformation
  In order to analyze Japanese stock anomalies safely
  As a quantitative researcher
  I want the system to authenticate securely and transform API data reliably

  Scenario: Application refuses to start without credentials
    Given the environment variable "JQUANTS_REFRESH_TOKEN" is not set
    When I attempt to initialize the AppSettings
    Then the application should raise a Pydantic ValidationError
    And the process should terminate before any network calls are made

  Scenario: Successful live data ingestion and transformation
    Given a valid "JQUANTS_REFRESH_TOKEN" is set in the environment
    When I execute the ingestion pipeline for a 12-week window
    Then the system should authenticate successfully with the J-Quants API
    And fetch daily quotes without encountering a rate limit exception
    And transform the raw data using Polars
    And the resulting DataFrame should contain columns "day_of_week", "intraday_return", and "overnight_return"
    And save the final DataFrame to a local "data.parquet" file

  Scenario: DuckDB can read the generated Parquet file
    Given the pipeline has successfully generated a "data.parquet" file
    When I execute a DuckDB query "SELECT count(*) FROM read_parquet('data.parquet')"
    Then the result should be an integer greater than 0
    And the schema returned should match the TransformedQuote specifications
```
