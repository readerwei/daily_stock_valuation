# This script fetches historical quarterly forward P/E ratios for stock tickers from gurufocus.com.
# It uses Playwright for web scraping and stores the data in a ClickHouse database.

import argparse
import json
import time
import random
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import yaml

def get_forward_pe_playwright(ticker):
    """
    Fetches and parses the quarterly forward PE history for a given ticker from gurufocus.com.
    It uses Playwright to navigate the website and BeautifulSoup to parse the HTML.
    """
    url = f"https://www.gurufocus.com/term/forward-pe-ratio/{ticker}"
    
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True) # Use a headless browser
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            page.goto(url, timeout=60000, wait_until='domcontentloaded')
            
            # Wait for a random time to mimic human behavior and avoid bot detection.
            time.sleep(random.uniform(3, 5))

            # Check if we've been blocked by Cloudflare.
            if "Attention Required!" in page.title():
                print("Cloudflare block detected. Unable to proceed.")
                browser.close()
                return None

            content = page.content()
            browser.close()
            
        except Exception as e:
            print(f"Error using Playwright to fetch data for {ticker}: {e}")
            return None

    soup = BeautifulSoup(content, 'html.parser')
    
    try:
        # The data is stored in a table with a specific thead id.
        thead = soup.find('thead', id='historical-quarter')
        if not thead:
            print("Error: Could not find thead with id 'historical-quarter'.")
            return None

        table = thead.find_parent('table')
        if not table:
            print("Error: Could not find parent table of thead.")
            return None

        all_trs = table.find_all('tr')
        
        if len(all_trs) < 3:
            print(f"Error: Found table but expected at least 3 rows, found {len(all_trs)}.")
            return None

        # Dates are in the second row, values are in the third.
        date_cells = all_trs[1].find_all('td')
        dates = [cell.text.strip() for cell in date_cells if cell.text.strip()]

        value_cells = all_trs[2].find_all('td')
        pe_values = [cell.text.strip() for cell in value_cells[1:]] # Skip the first cell which is a label

        result = []
        for i in range(len(dates)):
            try:
                result.append({
                    "date": dates[i],
                    "forward_pe": float(pe_values[i])
                })
            except (ValueError, IndexError):
                continue
        
        return result

    except Exception as e:
        print(f"An error occurred during parsing: {e}")
        return None

import calendar
from datetime import datetime
from clickhouse_driver import Client

def get_clickhouse_client(host='192.168.1.36', port=9000, user='default', password=''):
    """Establishes a connection to the ClickHouse database."""
    return Client(host=host, port=port, user=user, password=password)

def create_forward_pe_history_table(client, database='default', table='stock_forward_pe_history'):
    """Creates the forward P/E history table in ClickHouse with a ReplacingMergeTree engine to handle duplicates."""
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database}"
    client.execute(create_database_query)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table}
    (
        `ticker` String,
        `date` Date,
        `forward_pe` Float64
    )
    ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (ticker, date)
    """
    client.execute(create_table_query)
    print(f"Ensured table {table} exists with ReplacingMergeTree engine.")

def insert_forward_pe_history(client, ticker, pe_history, database='default', table='stock_forward_pe_history'):
    """Inserts the fetched forward P/E history into the ClickHouse table."""
    data_to_insert = []
    for item in pe_history:
        # Convert date string 'YYYY-MM' to the last day of the month.
        year, month = map(int, item['date'].split('-'))
        last_day = calendar.monthrange(year, month)[1]
        date_obj = datetime(year, month, last_day)
        data_to_insert.append({
            'ticker': ticker,
            'date': date_obj,
            'forward_pe': item['forward_pe']
        })

    if data_to_insert:
        client.execute(f"INSERT INTO {database}.{table} (ticker, date, forward_pe) VALUES", data_to_insert)
        print(f"Successfully inserted {len(data_to_insert)} records for {ticker} into ClickHouse.")

def optimize_table(client, database='default', table='stock_forward_pe_history'):
    """Optimizes the table to trigger the ReplacingMergeTree engine's deduplication."""
    print(f"Optimizing table {database}.{table} to enforce deduplication...")
    optimize_query = f"OPTIMIZE TABLE {database}.{table} FINAL"
    client.execute(optimize_query)
    print("Optimization complete.")

def get_tickers_from_yaml(yaml_file):
    """Reads a list of tickers from a YAML configuration file."""
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    return [ticker.strip() for ticker in data['alpaca']['custom_asset_list'].split(',')]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch quarterly forward PE history for a stock ticker using Playwright.")
    parser.add_argument("ticker", nargs='?', help="The stock ticker to fetch data for. If not provided, tickers from zipline.yaml will be used.", default=None, type=str)
    parser.add_argument("--create-table", action="store_true", help="Create the ClickHouse table and exit.")
    args = parser.parse_args()

    if args.create_table:
        try:
            client = get_clickhouse_client()
            create_forward_pe_history_table(client)
        except Exception as e:
            print(f"Error creating table: {e}")
        exit()

    tickers = []
    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        yaml_file = 'zipline.yaml'
        tickers = get_tickers_from_yaml(yaml_file)

    if not tickers:
        print("No tickers to process.")
    else:
        try:
            client = get_clickhouse_client()
            
            for ticker in tickers:
                print(f"--- Fetching data for {ticker} ---")
                pe_history = get_forward_pe_playwright(ticker)
                if pe_history:
                    print(json.dumps(pe_history, indent=2))
                    try:
                        insert_forward_pe_history(client, ticker, pe_history)
                    except Exception as e:
                        print(f"Error during ClickHouse insertion for {ticker}: {e}")
                
                # Add a random delay between requests to be a good web citizen.
                if len(tickers) > 1:
                    print("--- Waiting for next ticker ---")
                    time.sleep(random.uniform(5, 10))
            
            # Optimize the table once at the end to remove duplicates.
            optimize_table(client)

        except Exception as e:
            print(f"An error occurred: {e}")