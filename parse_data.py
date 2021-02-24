import os
from os.path import basename
from datetime import datetime, timedelta
import hashlib
import pandas as pd
import yfinance as yf
import numpy as np

DATA_PATH = os.path.join('data')
RAW_DATA_PATH = os.path.join(DATA_PATH, 'raw')
CLEAN_DATA_DIR = os.path.join(DATA_PATH, 'clean')
CLEAN_DATA_PATH_TRAIN = os.path.join(CLEAN_DATA_DIR, 'train_data.txt')
CLEAN_DATA_PATH_TEST = os.path.join(CLEAN_DATA_DIR, 'test_data.txt')
CLEAN_TEST_DATA_DIR = os.path.join(CLEAN_DATA_DIR, 'test')
COLUMNS = ['symbol', 'date', 'open', 'high', 'low', 'close','volume', 'label']
INFO_COLUMNS = ['symbol', 'sector', 'industry']

# MIN_TRAIN_DATE = '2013-01-01'
# MAX_TRAIN_DATE = '2018-12-31'
MIN_TRAIN_DATE = '2019-01-01'
MAX_TRAIN_DATE = '2019-12-31'
MIN_TEST_DATE = '2017-01-01'
MAX_TEST_DATE = '2017-12-31'
TRAIN_YEARS = ['2013','2014','2015','2016','2017','2018']
BATCH_SIZE = 100
# TEST_YEARS = ['2017']
PERCENT_GAIN = 1.10

SYMBOL_INDEX = 0
DATE_INDEX = 1
OPEN_INDEX = 2
HIGH_INDEX = 3
LOW_INDEX = 4
CLOSE_INDEX = 5
VOLUME_INDEX = 6
N_DAYS = 14


def get_data_for_symbols(symbols):
    data = yf.download(  # or pdr.get_data_yahoo(...
        # tickers list or string as well
        tickers = symbols,

        # use "period" instead of start/end
        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # (optional, default is '1mo')
        period = "1d",

        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        interval = "1d",

        # group by ticker (to access via data['SPY'])
        # (optional, default is 'column')
        group_by = 'ticker',

        # adjust all OHLC automatically
        # (optional, default is False)
        auto_adjust = True,

        # download pre/post regular market hours data
        # (optional, default is False)
        prepost = False,

        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        threads = True,

        # proxy URL scheme use use when downloading?
        # (optional, default is None)
        proxy = None
    )

def help_get_sector_data(symbol, ticker):
    try:
        # access each ticker using (example)
        info = ticker.info
        sector = info['sector']
        industry = info['industry']
        # print(f"Symbol: {symbol} Sector: {sector}")
        return [symbol, sector, industry]
    except:
        # print("Unexpected error: ", sys.exc_info()[0])
        print(f"Unexpected error: {symbol}")
        return []

    # yf_symbol.history(period="5y")
    # goes to 2016 to 2021
    # we just need 2019, 2020, 2021 up to feb 22
    # will need to cut prices to 2 digits
    # add sector
    # return data
#                  Open        High         Low       Close     Volume  Dividends  Stock Splits
#Date
#2016-02-23   22.415623   22.438875   21.985448   22.018002  127770400        0.0           0.0
#2016-02-24   21.852907   22.410971   21.699438   22.345863  145022800        0.0           0.0
#2016-02-25   22.334236   22.499331   22.148214   22.499331  110330800        0.0           0.0
#2016-02-26   22.601640   22.792312   22.457474   22.534208  115964400        0.0           0.0
#2016-02-29   22.522586   22.841149   22.473756   22.483057  140865200        0.0           0.0

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))

def get_sector_data(symbols):
    list_of_lists = []
    batches = chunks(symbols, BATCH_SIZE)
    for batch in batches:
        new_batch = ' '.join(batch)
        # print(f"Batch of symbols: {batch}")
        tickers = yf.Tickers(new_batch)
        # ^ returns a named tuple of Ticker objects
        # print(f"New batch of symbols: {new_batch}")
        # print(f"tickers: {tickers}")
        ordered_dict = tickers.tickers._asdict()
        for symbol, ticker in ordered_dict.items():
            # print(key)
            # print(value)
            info = help_get_sector_data(symbol, ticker)
            # print(info)
            if info:
                list_of_lists.append(info)
        # for symbol in batch:
        #    info = help_get_sector_data(tickers, symbol.upper())

    # Create the pandas DataFrame
    return pd.DataFrame(list_of_lists, columns = INFO_COLUMNS)

    # df = df.T
    # print(f"Type: {type(df)}")
    # print(f"Length: {len(df)}")
    # result = df.head(10)
    # print("First 10 rows of the DataFrame:")
    # print(result)
    # return df

def fetch_test_data(filename):
    ''' Read stock data from a CSV file'''
    data = []
    with open(filename) as f:
        # read first line to remove header
        f.readline()
        for line in f:
            data.append(line.strip().split(','))
    return data

def fetch_data(year):
    ''' Read stock data from all TXT files in RAW_DATA_PATH'''
    data = []
    year_dir = 'NASDAQ_' + year
    dir_path = os.path.join(RAW_DATA_PATH, year_dir)
    if os.path.isdir(dir_path) == False:
        print(path + ' not found')
        return data
    print('Reading from: ' + dir_path)
    for filename in os.listdir(dir_path):
        path = os.path.join(dir_path, filename)

        if os.path.isfile(path) == False:
            print('Error: Not a file' + path)
            continue

        with open(path) as f:
            # read first line to remove header
            f.readline()
            for line in f:
                data.append(line.strip().split(','))
    return data

def should_buy_stock(today_price, future_price):
    """
        Check if todays price * PERCENT GAIN  is less than the future price
        if so, its a buy
    """
    if (float(today_price) * PERCENT_GAIN) < float(future_price):
        return True
    return False

def get_date(date_str):
    # example start date in file is 20180102
    return datetime.strptime(date_str, '%Y%m%d')

def make_features(data, start_date, end_date):
    """
    Label the data and remove some data where volume is 0
    """
    points = []
    # We know the data is sorted.
    print(f"Making features from data from {start_date} to {end_date}")
    flag = True
    for index in range(len(data)):
        point = data[index]
        todays_date = get_date(point[DATE_INDEX])
        # TODO(eriq): We skip the first point, but if we didn't we
        # would have to be careful on the label for the first point.
        # example start date: '2013-01-01'
        if (todays_date < datetime.strptime(start_date, "%Y-%m-%d")):
            print('Error: before start_date')
            print(str(point))
            break
        if (todays_date > datetime.strptime(end_date, "%Y-%m-%d")):
            print('Error: past end_date')
            print(str(point))
            break
        if (int(point[VOLUME_INDEX]) == 0):
            # print('Error: Volume is 0')
            continue
        future_date = todays_date + timedelta(days=N_DAYS)
        label = 0
        if (future_date > datetime.strptime(end_date, "%Y-%m-%d")):
            if(flag):
                print(f"Todays date of {todays_date} + {N_DAYS} days ({future_date}) is greater than {end_date}")
                flag = False
            continue

        if index + N_DAYS < len(data) and data[index + N_DAYS][OPEN_INDEX]:
            # price in the future
            future_price = data[index + N_DAYS][OPEN_INDEX]
        if point[OPEN_INDEX]:
            today_price = point[OPEN_INDEX]
        if future_price and today_price:
            if should_buy_stock(today_price, future_price):
                label = 1

        features = [point[SYMBOL_INDEX], todays_date,
                float(point[OPEN_INDEX]), float(point[HIGH_INDEX]),
                float(point[LOW_INDEX]), float(point[CLOSE_INDEX]),
                float(point[VOLUME_INDEX]), label]
        if (features[2] <= 0.0 or features[3] <= 0.0 or features[4] <= 0.0 or features[5] <= 0.0):
            continue
        points.append(features)
    return points

# can be deleted
def output_csv_to_file(features, train, test, symbol):

    # TEST
    print("Sucessfully Featureized: %d" % (len(features)))
    if not os.path.exists(CLEAN_DATA_DIR):
        os.makedirs(CLEAN_DATA_DIR)
    if not os.path.exists(CLEAN_TEST_DATA_DIR):
        os.makedirs(CLEAN_TEST_DATA_DIR)

    if train:
        path = CLEAN_DATA_PATH_TRAIN

    elif test:
        path = os.path.join(CLEAN_TEST_DATA_DIR, symbol + '_test_data.txt')

    print('Writing to: ' + path)
    with open(path, 'w') as file:
        file.write('\n'.join(['\t'.join([str(part) for part in point]) for point in features]))

# for training use CLEAN_DATA_PATH_TRAIN
# for testing use CLEAN_DATA_PATH_TEST
def output_to_file(features, path):

    print("Sucessfully Featureized: %d" % (len(features)))

    print('Writing to: ' + path)
    with open(path, 'w') as file:
        file.write('\n'.join(['\t'.join([str(part) for part in point]) for point in features]))

# only used if files are CSVs
# can be deleted
def build_test_data(start_date, end_date):

    if os.path.isdir(RAW_DATA_PATH) == False:
        print(RAW_DATA_PATH + ' not found')
        return data
    print('Raw data path: ' + RAW_DATA_PATH)
    for filename in os.listdir(RAW_DATA_PATH):
        path = os.path.join(RAW_DATA_PATH, filename)
        if os.path.isfile(path) == False:
            continue
        data = fetch_test_data(path)

        features = make_features(data, start_date, end_date)

        if not features:
            print('Error: features list is empty\n')
        else:
            output_to_file(features, False, True, filename.split('.')[0])

def output_to_csv(df, output_path):
    """
    Output dataframe to CSV
    """
    print(f"Outputting dataframe to {output_path}")
    df.to_csv(output_path, index=False, encoding='utf-8')

def create_df(features):
    """
    Create the DataFrame
    """
    df = pd.DataFrame(features, columns = COLUMNS)
    df = df[df.volume != 0]
    return df.sort_values(["date", "symbol"], ignore_index=True)

def build_data(years):
    """
    Create one CSV file for each year in years
    """
    list_of_df = []
    for year in years:
        # TODO: update start_date and end_date based on year
        data = fetch_data(year)
        print("Raw Data: %d" % (len(data)))
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        features = make_features(data, start_date, end_date)
        print("Feature Data: %d" % (len(features)))
        if not features:
            print('Error: features list is empty\n')
        else:
            list_of_df.append(create_df(features))
    return list_of_df

def first():
    years = ['2019']
    print(f"Getting data for the following years: {years}")
    list_of_df = build_data(years)
    df = pd.concat(list_of_df)
    df = df.dropna()
    return df
    # print(df.head())

def main():
    df = first()
    symbols = df.symbol.unique()
    print(f"Read {len(df.index)} rows ({len(symbols)} symbols) of data")
    # used for testing
    # symbols = ['aapl', 'baba', 'msft', 'lulu', 'dkng', 'pltr', 'pypl', 'gme']
    df1 = get_sector_data(symbols)
    print(df1.head())
    output_to_csv(df1, "NASDAQ_2019_INFO.csv")
    # TODO: Check if this is right, axis might be wrong
    result = pd.concat([df, df1], axis=1, join="inner")
    # TODO: fix this!
    output_to_csv(result, "NASDAQ_2019.csv")

if __name__ == '__main__':
    main()
