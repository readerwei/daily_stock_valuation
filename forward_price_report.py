# This script generates a report on estimated forward prices for stocks.
# It retrieves data from a ClickHouse database, fetches current stock prices from Yahoo Finance,
# and then sends an email with the generated report.

import pandas as pd
from clickhouse_driver import Client
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import yfinance as yf
import os
from dotenv import load_dotenv

load_dotenv()

def get_clickhouse_client(host='192.168.1.36', port=9000, user='default', password=''):
    """Establishes a connection to the ClickHouse database."""
    return Client(host=host, port=port, user=user, password=password)

def get_forward_price_estimates():
    """Retrieves the latest estimated forward price ranges for all tickers from ClickHouse."""
    client = get_clickhouse_client()
    query = """
    SELECT
        ticker,
        argMax(estimated_forward_price_low, date) as estimated_forward_price_low,
        argMax(estimated_forward_price_high, date) as estimated_forward_price_high,
        max(date) as latest_date
    FROM default.stock_financial_data
    GROUP BY ticker
    HAVING estimated_forward_price_low IS NOT NULL AND estimated_forward_price_high IS NOT NULL
    ORDER BY ticker
    """
    result = client.execute(query, with_column_types=True)
    
    columns = [col[0] for col in result[1]]
    df = pd.DataFrame(result[0], columns=columns)
    return df

def add_current_price(df):
    """Fetches the current price for each ticker and determines its position relative to the estimated forward price range."""
    prices = []
    positions = []
    for index, row in df.iterrows():
        try:
            ticker_obj = yf.Ticker(row['ticker'])
            current_price = ticker_obj.history(period='1d')['Close'].iloc[0]
            prices.append(current_price)
            
            low = row['estimated_forward_price_low']
            high = row['estimated_forward_price_high']
            
            if current_price < low:
                positions.append('Below')
            elif current_price > high:
                positions.append('Above')
            else:
                positions.append('Within')

        except Exception as e:
            print(f"Could not get current price for {row['ticker']}: {e}")
            prices.append(None)
            positions.append(None)
            
    df['current_price'] = prices
    df['position'] = positions
    return df

def send_report_email(html_report):
    """Sends the generated HTML report via email."""
    USER = "quantwei.vpn@gmail.com"
    TO = "maiche.zhao@gmail.com"
    PASS = os.getenv("EMAIL_PASS")

    msg = MIMEMultipart(_subtype='related')
    msg["SUBJECT"] = f"Forward Price Estimate Report - {date.today()}"
    
    title = f"<h2><b>Estimated Forward Price Report for {date.today()}</b></h2>"
    part1 = MIMEText(title + html_report, 'html')
    msg.attach(part1)

    try:
        s = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        s.ehlo()
        s.login(USER, PASS)
        s.sendmail(USER, TO, msg.as_string())
        s.close()
        print("Report email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    print("--- Generating Forward Price Estimate Report ---")
    
    report_df = get_forward_price_estimates()
    
    if report_df.empty:
        print("No data found in ClickHouse. Exiting.")
    else:
        report_df = add_current_price(report_df)
        
        report_df = report_df.round(2)
        html_table = report_df.to_html(index=False)
        
        send_report_email(html_table)
        
        print("--- Report generation complete ---")
