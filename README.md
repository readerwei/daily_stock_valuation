# Project Zipline: Financial Data Analysis

This project contains Python scripts designed to fetch, calculate, and store a variety of financial data for a list of stock tickers.

## `get_eps.py` Script

The `get_eps.py` script is a powerful tool for gathering financial metrics for a predefined list of stocks. It fetches data from Yahoo Finance, performs calculations for historical data, and stores the results in a ClickHouse database.

### Features

- **Comprehensive Data Retrieval:** Fetches a wide range of financial data, including:
  - Trailing and forward Earnings Per Share (EPS)
  - Trailing and forward Price-to-Earnings (P/E) ratios
  - Analyst EPS estimates (low, high, and average for current and next quarter/year)
- **Historical P/E Range:** Calculates the 5-year historical P/E range for each stock based on historical price and earnings data.
- **ClickHouse Integration:** Stores the collected data in a ClickHouse database for future analysis. It automatically creates the database and table if they don't exist.
- **Configurable Ticker List:** The list of stock tickers is managed in the `zipline.yaml` file, making it easy to add or remove stocks.

### How to Use

1.  **Configure Tickers:** Add your desired stock tickers to the `zipline.yaml` file under the `custom_asset_list` key.
2.  **Run the Script:** Execute the script from your terminal:

    ```bash
    python get_eps.py
    ```

### Dependencies

The script relies on the following Python libraries:

- `yfinance`: For fetching financial data from Yahoo Finance.
- `PyYAML`: For reading the `zipline.yaml` configuration file.
- `pandas`: For data manipulation and calculations.
- `clickhouse-driver`: For connecting to and inserting data into the ClickHouse database.

### ClickHouse Setup

The script is configured to connect to a ClickHouse database with the following settings:

- **Host:** `192.168.1.36`
- **Port:** `9000`
- **Username:** `default`
- **Password:** (none)
- **Database:** `default`
- **Table:** `stock_financial_data`

The script will automatically create the `stock_financial_data` table with the following schema if it doesn't exist:

```sql
CREATE TABLE default.stock_financial_data
(
    `ticker` String,
    `date` Date,
    `trailing_eps` Nullable(Float64),
    `forward_eps` Nullable(Float64),
    `trailing_pe` Nullable(Float64),
    `forward_pe` Nullable(Float64),
    `pe_range_low_5y` Nullable(Float64),
    `pe_range_high_5y` Nullable(Float64),
    `analyst_eps_range_low_0q` Nullable(Float64),
    `analyst_eps_range_high_0q` Nullable(Float64),
    `analyst_eps_range_low_p1q` Nullable(Float64),
    `analyst_eps_range_high_p1q` Nullable(Float64),
    `analyst_eps_range_low_0y` Nullable(Float64),
    `analyst_eps_range_high_0y` Nullable(Float64),
    `analyst_eps_range_low_p1y` Nullable(Float64),
    `analyst_eps_range_high_p1y` Nullable(Float64),
    `forward_pe_perc_25` Nullable(Float64),
    `forward_pe_perc_50` Nullable(Float64),
    `forward_pe_perc_75` Nullable(Float64),
    `estimated_forward_price_low` Nullable(Float64),
    `estimated_forward_price_high` Nullable(Float64),
    `peg_ratio` Nullable(Float64),
    `analyst_eps_range_avg_0q` Nullable(Float64),
    `analyst_eps_range_avg_p1q` Nullable(Float64),
    `analyst_eps_range_avg_0y` Nullable(Float64),
    `analyst_eps_range_avg_p1y` Nullable(Float64)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (ticker, date);
```

## `get_forward_pe_playwright.py` Script

The `get_forward_pe_playwright.py` script is designed to fetch historical quarterly forward P/E ratios for a list of stock tickers from gurufocus.com. It uses Playwright to navigate the website and BeautifulSoup to parse the data.

### Features

- **Historical Forward P/E:** Fetches quarterly forward P/E data.
- **ClickHouse Integration:** Stores the collected data in a ClickHouse database in the `stock_forward_pe_history` table.
- **Duplicate Handling:** Uses the `ReplacingMergeTree` engine in ClickHouse. After all data is inserted, the script runs an `OPTIMIZE TABLE` command to trigger the engine's deduplication process, ensuring that only the latest entry for each ticker and date is kept.
- **Flexible Ticker Input:** The script can be run for a single ticker provided as a command-line argument, or for all tickers listed in the `zipline.yaml` file if no argument is provided.
- **Manual Table Creation:** The table creation is handled by a command-line flag, preventing the table from being recreated on every run.

### How to Use

1.  **Create the Table (One-Time Setup):** Before running the script for the first time, create the `stock_forward_pe_history` table in ClickHouse by running the script with the `--create-table` flag:
    ```bash
    python get_forward_pe_playwright.py --create-table
    ```
2.  **Configure Tickers:** For multi-ticker runs, ensure your desired stock tickers are listed in the `zipline.yaml` file under the `custom_asset_list` key.
3.  **Run for All Tickers:** Execute the script without arguments to process all tickers from `zipline.yaml`:
    ```bash
    python get_forward_pe_playwright.py
    ```
4.  **Run for a Single Ticker:** Provide a ticker symbol as a command-line argument:
    ```bash
    python get_forward_pe_playwright.py AAPL
    ```

### Dependencies

The script relies on the following Python libraries:

- `playwright`: For web scraping.
- `beautifulsoup4`: For parsing HTML.
- `PyYAML`: For reading the `zipline.yaml` configuration file.
- `clickhouse-driver`: For connecting to and inserting data into the ClickHouse database.

### ClickHouse Setup

The script connects to the same ClickHouse database as `get_eps.py` but uses a different table. The `stock_forward_pe_history` table is created with the `ReplacingMergeTree` engine to handle duplicate data. The schema is as follows:

```sql
CREATE TABLE IF NOT EXISTS default.stock_forward_pe_history
(
    `ticker` String,
    `date` Date,
    `forward_pe` Float64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (ticker, date);
```