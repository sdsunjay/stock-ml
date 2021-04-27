#!/usr/bin/env python
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from datetime import datetime
from os import path, SEEK_END
from time import sleep
import sys
import traceback

# Add your Alpha Vantange API Key
from config import API_KEY_ALPHAVANTAGE

SYMBOLS_PATH = './new_symbols'

# import warnings
# warnings.simplefilter("always")

# ----------------------------------------------------- EXPORT -----------------------------------------------------
def export(df_stock, s_ticker):
    full_path = path.join(SYMBOLS_PATH, "data/")
    s_filename = path.join(full_path, f"{s_ticker}.csv")
    if df_stock.empty:
        print("No data loaded yet to export.")
        return
    df_stock.to_csv(s_filename)
    return


def read_all_symbols(file_path):
    ''' Read all symbols from a file
    Symbols all appear on one line and are comma delimited
    '''
    try:
        if path.isfile(file_path) == False:
            print('Error: Not a file' + file_path)
            return []

        with open(file_path) as f:
            print(f"Reading symbols from the following file: {file_path}")
            line = f.readline()
            all_symbols = "".join(line.split()).split(',')
        print(f"Number of symbols read: {len(all_symbols)}")
        return all_symbols
    except:
        print("Unexpected error in all symbols:", sys.exc_info()[0])
        print(f"Error with symbol: {s_ticker}")
        print(traceback.format_exc())
        print(sys.exc_info()[2])
        print("Unexpected error:", sys.exc_info()[0])
        return []


# pylint: disable=too-many-branches
def main():
    """
    Read through a list of tickers and get the daily price and then output each ticker to a file
    """
    file_path = path.join(SYMBOLS_PATH, 'final_all_symbols.csv')
    s_tickers = read_all_symbols(file_path)

    file_path = path.join(SYMBOLS_PATH, 'bad_symbols.csv')
    bad_symbols_list = read_all_symbols(file_path)

    file_path = path.join(SYMBOLS_PATH, 'existing_symbols.csv')
    existing_symbols_list = read_all_symbols(file_path)

    if len(s_tickers) != 0 and len(bad_symbols_list) != 0 and existing_symbols_list:
        s_tickers = get_unique_symbols(s_tickers)
        s_tickers = remove_tickers(s_tickers, bad_symbols_list)
        s_tickers = remove_tickers(s_tickers, existing_symbols_list)
        start_stocks(s_tickers, existing_symbols_list, bad_symbols_list)
    else:
        print("Error! empty stock list")


def get_unique_symbols(symbols):
    list_set = set(symbols)
    # convert the set to the list
    new_list = list(list_set)
    return new_list

def remove_tickers(s_tickers, removal_list):
    for ticker in removal_list:
        if ticker in s_tickers:
            s_tickers.remove(ticker)
    return s_tickers


def start_stocks(s_tickers, existing_symbols_list, bad_symbols_list):
    ticker_count = 0
    ts = TimeSeries(key=API_KEY_ALPHAVANTAGE, output_format='pandas')
    print(f"Total number of symbols left: {len(s_tickers)}")
    for s_ticker in s_tickers:
        ticker_count += 1
        if ticker_count >= 500:
            print('Quitting!')
            print(f"Last symbol: {s_ticker}")
            break
        df_stock = pd.DataFrame()
        s_interval = "1440min"

        try:
            df_stock, d_stock_metadata = ts.get_daily_adjusted(symbol=s_ticker, outputsize='full')
            df_stock.sort_index(ascending=True, inplace=True)
            export(df_stock, s_ticker)
            existing_symbols_list.append(s_ticker)
            if ticker_count % 50 == 0:
                print(f"{str(ticker_count)} Ticker: {s_ticker}")
                print("Sleeping for 3 seconds")
                sleep(3)
        except ValueError as error:
            if '500 calls per day' in str(error):
                print(f"Error: {error}\nQuitting!")
                print(f"{str(ticker_count)} tickers read.\nLast symbol: {s_ticker}")
                break
            bad_symbols_list.append(s_ticker)
            print(f"Error with symbol: {s_ticker}")
        except NameError as error:
            print("Name error:", sys.exc_info()[0])
            print(f"Error with symbol: {s_ticker}")
            print(traceback.format_exc())
            print(sys.exc_info()[2])
            print("Unexpected error:", sys.exc_info()[0])
        except:
            print(f"{str(ticker_count)} tickers read.\nLast symbol: {s_ticker}")
            print("Unexpected error:", sys.exc_info()[0])
            print(f"Error with symbol: {s_ticker}")
            print(traceback.format_exc())
            print(sys.exc_info()[2])
            print("Unexpected error:", sys.exc_info()[0])
            break
        sleep(16)

    date_time_obj = datetime.now()
    date_string = date_time_obj.strftime("%d-%m-%Y__%H:%M")
    file_path = path.join(SYMBOLS_PATH, f"{date_string}_bad_symbols.csv")
    output_list(bad_symbols_list, file_path)
    file_path = path.join(SYMBOLS_PATH, f"{date_string}_existing_symbols.csv")
    output_list(existing_symbols_list, file_path)


def output_list(symbols_list, filename):
    print(f"Outputting {len(symbols_list)} symbols to {filename}")
    with open(filename, 'w+') as filehandle:
        for symbol in symbols_list:
            filehandle.write('%s,' % symbol)
    with open(filename, 'rb+') as filehandle:
        filehandle.seek(-1, SEEK_END)
        filehandle.truncate()

if __name__ == "__main__":
    main()
