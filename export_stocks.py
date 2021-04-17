#!/usr/bin/env python
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime
from os import path
from time import sleep

# Add your Alpha Vantange API Key
API_KEY_ALPHAVANTAGE = ''
SYMBOLS_PATH = './data/clean/symbols'

# import warnings
# warnings.simplefilter("always")

# ----------------------------------------------------- EXPORT -----------------------------------------------------
def export(df_stock, s_ticker):
    s_filename = f"{s_ticker}.csv"
    if df_stock.empty:
        print("No data loaded yet to export.")
        return
    df_stock.to_csv(s_filename)
    return


def read_all_symbols(file_path):
    if path.isfile(file_path) == False:
        print('Error: Not a file' + file_path)
        import sys
        sys.exit(1)

    with open(file_path) as f:
        print(f"Reading symbols from the following file: {file_path}")
        line = f.readline()
        all_symbols = "".join(line.split()).split(',')
    print(f"Number of symbols read: {len(all_symbols)}")
    return all_symbols


# pylint: disable=too-many-branches
def main():
    """
    Read through a list of tickers and get the daily price and then output each ticker to a file
    """
    ts = TimeSeries(key=cfg.API_KEY_ALPHAVANTAGE, output_format='pandas')
    file_path = path.join(SYMBOLS_PATH, 'final_all_symbols.csv')
    s_tickers = read_all_symbols(file_path)

    file_path = path.join(SYMBOLS_PATH, 'bad_symbols.csv')
    bad_symbols_list = read_all_symbols(file_path)

    file_path = path.join(SYMBOLS_PATH, 'existing_symbols.csv')
    existing_symbols_list = read_all_symbols(file_path)

    if len(s_tickers) != 0 and len(bad_symbols_list) != 0 and existing_symbols_list:
        s_tickers = remove_tickers(s_tickers, bad_symbols_list)
        s_tickers = remove_tickers(s_tickers, existing_symbols_list)
        start_stocks(s_tickers, existing_symbols_list, bad_symbols_list)
    else:
        print("Error! empty stock list")


def remove_tickers(s_tickers, removal_list):
    for ticker in removal_list:
        if ticker in s_tickers:
            s_tickers.remove(ticker)
    return s_tickers


def start_stocks(s_tickers, existing_symbols_list, bad_symbols_list):
    ticker_count = 0
    for s_ticker in s_tickers:
        if ticker_count >= 499:
            print('quitting!')
            print(f"Last symbol: {s_ticker}")
            break
        df_stock = pd.DataFrame()
        s_interval = "1440min"

        try:
            df_stock, d_stock_metadata = ts.get_daily_adjusted(symbol=s_ticker, outputsize='full')
            df_stock.sort_index(ascending=True, inplace=True)
            ticker_count += 1
            print(f"Metadata: {d_stock_metadata}")
            #    print (f"Error {output_path} already exists exists: {file_exists}")
            export(df_stock, s_ticker)
            existing_symbols_list.append(s_ticker)
            break
            if ticker_count % 20 == 0:
                print(f"{str(ticker_count)} Ticker: {s_ticker}")
                print("Sleeping for 20 seconds")
                sleep(20)
        except ValueError as error:
            if str(error) not in 'Our standard API call frequency is 5 calls per minute and 500 calls per day':
                bad_symbols.append(s_ticker)
            print(str(error))
            print(f"Error with symbol: {s_ticker}")
            import sys
            print("Unexpected error:", sys.exc_info()[0])
        except:
            import sys
            print("Unexpected error:", sys.exc_info()[0])
            print(f"Error with symbol: {s_ticker}")
        sleep(20)

    date_time_obj = datetime.now()
    date_string = date_time_obj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    file_path = path.join(SYMBOLS_PATH, f"{date_string}_bad_symbols.csv")
    output_list(bad_symbols_list, file_path)
    file_path = path.join(SYMBOLS_PATH, f"{date_string}_existing_symbols.csv")
    output_list(existing_symbols_list, file_path)


def output_list(symbols_list, filename):
    print(f"Outputting {len(symbols_list)} symbols to {filename}")
    with open(filename, 'w+') as filehandle:
        for symbol in symbols_list:
            filehandle.write('%s,' % symbol)


if __name__ == "__main__":
    main()
