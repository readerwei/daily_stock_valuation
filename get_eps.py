# This script fetches financial data for a list of stock tickers from Yahoo Finance,
# calculates historical P/E ratios, and stores the data in a ClickHouse database.

import yfinance as yf
import yaml
import pandas as pd
from clickhouse_driver import Client
from datetime import date
import sys
import numpy as np

def clean_data(value):
    """Cleans numpy data types for ClickHouse insertion."""
    if isinstance(value, np.float64):
        if np.isnan(value):
            return None
        return float(value)
    return value

def get_clickhouse_client(host='192.168.1.36', port=9000, user='default', password=''):
    """Establishes a connection to the ClickHouse database."""
    return Client(host=host, port=port, user=user, password=password)

def create_stock_data_table(client, database='default', table='stock_financial_data'):
    """Creates the stock_financial_data table in ClickHouse if it doesn't exist."""
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database}"
    client.execute(create_database_query)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table}
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
        `analyst_eps_range_avg_0q` Nullable(Float64),
        `analyst_eps_range_low_p1q` Nullable(Float64),
        `analyst_eps_range_high_p1q` Nullable(Float64),
        `analyst_eps_range_avg_p1q` Nullable(Float64),
        `analyst_eps_range_low_0y` Nullable(Float64),
        `analyst_eps_range_high_0y` Nullable(Float64),
        `analyst_eps_range_avg_0y` Nullable(Float64),
        `analyst_eps_range_low_p1y` Nullable(Float64),
        `analyst_eps_range_high_p1y` Nullable(Float64),
        `analyst_eps_range_avg_p1y` Nullable(Float64),
        `forward_pe_perc_25` Nullable(Float64),
        `forward_pe_perc_75` Nullable(Float64),
        `estimated_forward_price_low` Nullable(Float64),
        `estimated_forward_price_high` Nullable(Float64),
        `peg_ratio` Nullable(Float64)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (ticker, date)
    """
    client.execute(create_table_query)

def get_historical_forward_pe_range_from_clickhouse(client, ticker):
    """Retrieves the 25th and 75th percentile of historical forward P/E from ClickHouse."""
    query = f"SELECT forward_pe FROM default.stock_forward_pe_history WHERE ticker = '{ticker}' AND forward_pe IS NOT NULL"
    try:
        result = client.execute(query)
        if not result:
            return None, None
        
        pe_estimates = [row[0] for row in result]
        if len(pe_estimates) < 4:
            return None, None

        low = np.percentile(pe_estimates, 25)
        high = np.percentile(pe_estimates, 75)
        return low, high
    except Exception as e:
        print(f"Error querying historical Forward PE for {ticker}: {e}")
        return None, None

def delete_data_for_date(client, date, database='default', table='stock_financial_data'):
    """Deletes data for a specific date to avoid duplicates."""
    delete_query = f"ALTER TABLE {database}.{table} DELETE WHERE date = toDate('{date.strftime('%Y-%m-%d')}')"
    client.execute(delete_query)

def insert_stock_data(client, data, database='default', table='stock_financial_data'):
    """Inserts a batch of stock data into the ClickHouse table."""
    client.execute(f"INSERT INTO {database}.{table} VALUES", data)

def get_historical_pe_range(ticker, period="5y"):
    """
    Calculates the historical P/E range for a given ticker over a specified period.
    It uses quarterly financial statements to calculate TTM EPS and then computes P/E.
    """
    try:
        stock = yf.Ticker(ticker)

        # Get quarterly income statement
        income_stmt = stock.quarterly_income_stmt
        if income_stmt is None or income_stmt.empty or 'Net Income' not in income_stmt.index:
            return None, None
        net_income = income_stmt.loc['Net Income']

        # Get quarterly shares outstanding from balance sheet
        balance_sheet = stock.quarterly_balance_sheet
        if balance_sheet is None or balance_sheet.empty or 'Share Issued' not in balance_sheet.index:
            return None, None
        shares_outstanding = balance_sheet.loc['Share Issued']
        
        # Align net_income and shares_outstanding
        common_index = net_income.index.intersection(shares_outstanding.index)
        net_income = net_income[common_index]
        shares_outstanding = shares_outstanding[common_index]

        if shares_outstanding.empty:
            return None, None

        # Calculate quarterly EPS
        eps = net_income / shares_outstanding
        eps = eps.sort_index()
        # Calculate TTM EPS
        ttm_eps = eps.rolling(window=4).sum().dropna()
        if ttm_eps.empty:
            return None, None
            
        # Localize ttm_eps index to UTC
        ttm_eps.index = ttm_eps.index.tz_localize('UTC')

        # Get historical prices
        hist = stock.history(period=period)
        if hist.empty:
            return None, None

        # Get end of quarter prices
        # end_of_quarter_prices = hist['Close'].resample('QE').last()
        end_of_quarter_prices = hist['Close']

        # Reindex ttm_eps to align with end_of_quarter_prices
        aligned_ttm_eps = ttm_eps.reindex(end_of_quarter_prices.index, method='pad')

        # Align data
        pe_data = pd.DataFrame({'price': end_of_quarter_prices, 'ttm_eps': aligned_ttm_eps})
        pe_data = pe_data.dropna()
        pe_data = pe_data[pe_data['ttm_eps'] > 0] # Exclude negative EPS for P/E calculation

        if pe_data.empty:
            return None, None

        # Calculate P/E
        pe_data['pe'] = pe_data['price'] / pe_data['ttm_eps']

        # Get P/E range
        pe_min = pe_data['pe'].min()
        pe_max = pe_data['pe'].max()

        return pe_min, pe_max
    except Exception as e:
        import traceback
        print(f"Error calculating P/E range for {ticker}:")
        traceback.print_exc()
        return None, None

def get_eps_data(tickers, pe_period="5y"):
    """
    Fetches a variety of financial data points for a list of tickers.
    This includes EPS, P/E, analyst estimates, and calculated price targets.
    """
    all_data = []
    client = get_clickhouse_client()
    for ticker in tickers:
        print(f"--- Fetching data for {ticker} ---")
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            pe_min, pe_max = get_historical_pe_range(ticker, period=pe_period)
            
            pe_perc_25, pe_perc_75 = get_historical_forward_pe_range_from_clickhouse(client, ticker)
            
            forward_eps = info.get('forwardEps')
            estimated_price_low = None
            estimated_price_high = None
            if forward_eps and pe_perc_25 is not None:
                estimated_price_low = forward_eps * pe_perc_25
            if forward_eps and pe_perc_75 is not None:
                estimated_price_high = forward_eps * pe_perc_75

            earnings_estimate = stock.earnings_estimate
            
            analyst_estimates = {}
            if earnings_estimate is not None and not earnings_estimate.empty:
                for period, estimates in earnings_estimate.iterrows():
                    period_name = period.replace('+', 'p')
                    analyst_estimates[f'analyst_eps_range_low_{period_name}'] = estimates.get('low')
                    analyst_estimates[f'analyst_eps_range_high_{period_name}'] = estimates.get('high')
                    analyst_estimates[f'analyst_eps_range_avg_{period_name}'] = estimates.get('avg')

            row = {
                'ticker': ticker,
                'date': date.today(),
                'trailing_eps': info.get('trailingEps'),
                'forward_eps': forward_eps,
                'trailing_pe': info.get('trailingPE'),
                'forward_pe': info.get('forwardPE'),
                'pe_range_low_5y': pe_min,
                'pe_range_high_5y': pe_max,
                'forward_pe_perc_25': pe_perc_25,
                'forward_pe_perc_75': pe_perc_75,
                'estimated_forward_price_low': estimated_price_low,
                'estimated_forward_price_high': estimated_price_high,
                **analyst_estimates,
                'peg_ratio': info.get('trailingPegRatio')
            }
            all_data.append(row)

        except Exception as e:
            print(f"Could not retrieve data for {ticker}: {e}")
            
    return all_data

def get_tickers_from_yaml(yaml_file):
    """Reads a list of tickers from a YAML configuration file."""
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    return [ticker.strip() for ticker in data['alpaca']['custom_asset_list'].split(',')]

if __name__ == "__main__":
    # --create-table flag allows for initial table setup without running the full script.
    if '--create-table' in sys.argv:
        print("--- Creating ClickHouse table ---")
        try:
            client = get_clickhouse_client()
            create_stock_data_table(client)
            print("--- Table creation complete ---")
        except Exception as e:
            print(f"Error creating ClickHouse table: {e}")
    else:
        # Main execution block
        yaml_file = 'zipline.yaml'
        tickers = get_tickers_from_yaml(yaml_file)
        if tickers:
            financial_data = get_eps_data(tickers, pe_period="5y")
            if financial_data:
                print("--- Deleting existing data for today's date ---")
                try:
                    client = get_clickhouse_client()
                    today = date.today()
                    delete_data_for_date(client, today)
                    print("--- Deletion complete ---")
                    
                    print("--- Inserting data into ClickHouse ---")
                    columns = [
                        'ticker', 'date', 'trailing_eps', 'forward_eps', 'trailing_pe', 'forward_pe',
                        'pe_range_low_5y', 'pe_range_high_5y',
                        'analyst_eps_range_low_0q', 'analyst_eps_range_high_0q', 'analyst_eps_range_avg_0q',
                        'analyst_eps_range_low_p1q', 'analyst_eps_range_high_p1q', 'analyst_eps_range_avg_p1q',
                        'analyst_eps_range_low_0y', 'analyst_eps_range_high_0y', 'analyst_eps_range_avg_0y',
                        'analyst_eps_range_low_p1y', 'analyst_eps_range_high_p1y', 'analyst_eps_range_avg_p1y',
                        'forward_pe_perc_25', 'forward_pe_perc_75',
                        'estimated_forward_price_low', 'estimated_forward_price_high',
                        'peg_ratio'
                    ]
                    
                    data_to_insert = []
                    for row_dict in financial_data:
                        # Ensure data is clean and in the correct order for insertion
                        row_list = [clean_data(row_dict.get(col)) for col in columns]
                        data_to_insert.append(row_list)
                    
                    insert_stock_data(client, data_to_insert)
                    print("--- Data insertion complete ---")
                except Exception as e:
                    print(f"Error during database operation: {e}")
