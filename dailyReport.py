# This script generates a daily financial report using Zipline's pipeline API
# and sends it via email. It calculates various technical indicators and custom factors.

import zipline
import sys
import pandas as pd
import trading_calendars
from datetime import date, timedelta, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv

from zipline.data import bundles
from zipline.utils.calendars import get_calendar
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.data import USEquityPricing, EquityPricing
from zipline.pipeline.engine import SimplePipelineEngine

from zipline.pipeline.domain import US_EQUITIES
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.factors import Returns, AverageDollarVolume, RSI, BollingerBands, MACDSignal, VWAP

from talib import WILLR, MACD

load_dotenv()

# Custom Factor: Mean Reversion
class MeanReversion(CustomFactor):
    """Computes the ratio of the latest monthly return to the 12-month average,
       normalized by the standard deviation of monthly returns."""
    inputs = [Returns(window_length=10)]
    window_length = 120

    def compute(self, today, assets, out, monthly_returns):
        df = pd.DataFrame(monthly_returns)
        out[:] = df.iloc[-1].sub(df.mean()).div(df.std())

# Custom Factor: MACD Histogram
class MACDHist(CustomFactor):
    """Calculates the MACD signal line for each asset."""
    inputs = [USEquityPricing.close]
    window_length = 120
    def compute(self, today, assets, out, inputs):
        df = pd.DataFrame(inputs)
        for col_ix in df.columns:
            macd, signal, hist = MACD(df.loc[:, col_ix], 12, 26, 9)
            # out[col_ix] = hist.iloc[-1]
            out[col_ix] = signal.iloc[-1]

# Load the Zipline bundle for historical data.
bundle_name = 'alpaca_api'
bundle_data = bundles.load(bundle_name)

# Set the dataloader for the pipeline engine.
pricing_loader = USEquityPricingLoader.without_fx(bundle_data.equity_daily_bar_reader, bundle_data.adjustment_reader)

# Define the function for the get_loader parameter of the pipeline engine.
def choose_loader(column):

    if column not in USEquityPricing.columns:
        raise Exception('Column not in USEquityPricing')
    return pricing_loader

# Set the trading calendar to NYSE.
trading_calendar = get_calendar('NYSE')

# Create a Pipeline engine.
engine = SimplePipelineEngine(get_loader = choose_loader,
                              asset_finder = bundle_data.asset_finder)


# Define the universe of assets for the pipeline (top 10 by average dollar volume).
universe = AverageDollarVolume(window_length = 5).top(10)

# Create an empty Pipeline.
pipeline = Pipeline(domain=US_EQUITIES)

# Add built-in factors to the pipeline.
pipeline.add(AverageDollarVolume(window_length = 5), "Dollar Volume")
pipeline.add(Returns(window_length=2), "daily return")
pipeline.add(RSI(window_length=14), "RSI")
pipeline.add(MACDSignal(), "MACD")
pipeline.add(MACDHist(), 'macdhist')
pipeline.add(EquityPricing.close.latest, "close")
pipeline.add(BollingerBands(window_length=14, k=1.8), "Bollinger")

# Add custom factors to the pipeline.
pipeline.add(MeanReversion().top(2), "shorts")

# Set the start and end dates for the pipeline run.
# The end date is provided as a command-line argument.
end_date = pd.Timestamp(sys.argv[1], tz = 'utc')
end_date = end_date + timedelta(1)

start_date = end_date - timedelta(14)
while not trading_calendar.is_session(start_date):
    start_date -= timedelta(days=1)

# Run the pipeline to get the financial data.
pipeline_output = engine.run_pipeline(pipeline, start_date, end_date)

# Post-process the pipeline output.
pipeline_output.loc[:, 'Dollar Volume'] = pipeline_output.loc[:, 'Dollar Volume'] /1e9
pipeline_output.loc[:, 'daily return'] *= 100

# Extract the results for the end date, round values, and sort by RSI.
result = pipeline_output.loc[end_date]
result = result.round(2)
result.loc[:, 'Bollinger'] = result.Bollinger.apply(lambda x: tuple(round(val, 2) for val in x))
result.sort_values('RSI', inplace=True)

# Save the results to a pickle file for use by other scripts.
result.to_pickle("./daily.pkl")


# Email functionality to send the report.
import smtplib

USER = "quantwei.vpn@gmail.com"
TO = "maiche.zhao@gmail.com"
PASS = os.getenv("EMAIL_PASS") # Email password loaded from environment variables.

# Create the HTML content for the email.
text = result.to_html()
textSections = '<p>' + text + '</p>'
title ="<h2><b>Daily Summary for " + str(end_date.date()) + "</b></h2>"
part1 = MIMEText(title + textSections, 'html')

# Create a multipart message and attach the HTML content.
msg = MIMEMultipart(_subtype='related')
msg.attach(part1)
msg["SUBJECT"] = "Holdings Daily Report"

# Connect to the SMTP server and send the email.
s = smtplib.SMTP_SSL('smtp.gmail.com', 465)
s.ehlo()
s.login(USER, PASS)
s.sendmail(USER, TO, msg.as_string())
s.close()
