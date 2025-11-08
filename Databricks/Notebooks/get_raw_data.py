# Load of data from yahoo finance

import pandas as pd
import yfinance as yf
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns

# List of tickers (only stocks)

list_tickers = ['NVDA', 'META', 'AMZN', 'PANW', 'ORCL', 'AMD', 'GOOG', 'MSFT', 'AVGO']

# Dates Interval

start_date = "2014-01-01"
end_date = "2025-11-01"

# Function for downloading data of a list of assets

def get_assets(start_date, end_date, tickers):

    asset_data = {}

    for t in tickers:

        data = yf.download(t, start = start_date, end = end_date, auto_adjust = False)
        asset_data[t] = data[['Open', 'High', 'Low', 'Close', 'Volume']]

    assets = pd.concat(asset_data.values(), axis = 1, keys = asset_data.keys())

    expected_attrs = ['Open', 'High', 'Low', 'Close', 'Volume']

    
    # Must ensure we always end up with a 2-level MultiIndex: (Ticker, Attribute)

    if isinstance(assets.columns, pd.MultiIndex):

        if assets.columns.nlevels == 2:

            assets.columns.names = ['Ticker', 'Attribute']

        else:

            assets.columns = pd.MultiIndex.from_product([list(asset_data.keys()), expected_attrs], names=['Ticker', 'Attribute'])
    else:

        assets.columns = pd.MultiIndex.from_product([list(asset_data.keys()), expected_attrs], names=['Ticker', 'Attribute'])

    return assets



# Main

if __name__ == "__main__":

    raw_data = get_assets(start_date, end_date, list_tickers)

    raw_data.to_csv(r'C:\Users\ADMIN\Desktop\Documents\Projects\Full Stack Data Engineering Azure Project\Data Files\raw_data_ohlcv.csv')

    print("Raw data downloaded and saved successfully.")
