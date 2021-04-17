import os
from os.path import basename
from datetime import datetime, timedelta
import hashlib
import pandas as pd
import yfinance as yf
import numpy as np
from multiprocessing import Pool
import time
from all_symbols import read_all_symbols

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
BATCH_SIZE = 200
# TEST_YEARS = ['2017']
TEN_PERCENT_GAIN = 1.10
TWENTY_PERCENT_GAIN = 1.20
FOURTY_PERCENT_GAIN = 1.40

SYMBOL_INDEX = 0
DATE_INDEX = 1
OPEN_INDEX = 2
HIGH_INDEX = 3
LOW_INDEX = 4
CLOSE_INDEX = 5
VOLUME_INDEX = 6
N_DAYS = 5
NUM_PROCESSES = 2

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
    '''access sector and industry info on each ticker'''
    try:
        info = ticker.info
        time.sleep(1)
        return [symbol, info['sector'], info['industry']]
    except:
        print(f"Unexpected error: {symbol}")
        time.sleep(60)
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

def split_list_in_four(symbols):
    '''Split symbols list into batches and get sector and industry info foreach batch from yahoo finance API using separate threads'''
    p = Pool(processes=NUM_PROCESSES)
    symbol_batches = np.array_split(symbols, NUM_PROCESSES)
    pool_results = p.map(get_sector_data, symbol_batches)
    p.close()
    p.join()
    # merging parts processed by different processes
    df = pd.concat(pool_results, ignore_index=True)
    # print(df.head())
    return df

def get_sector_data(symbols):
    start_time = datetime.now()
    print(f"Getting sector data for {len(symbols)} symbols at {start_time}")
    list_of_lists = []
    fail_count = 0
    symbol_batches = np.array_split(symbols, 100)
    symbol_count = 0
    for symbol_batch in symbol_batches:
        symbols_string = ' '.join(symbol_batch)
        tickers = yf.Tickers(symbols_string)
        # ^ returns a named tuple of Ticker objects
        ordered_dict = tickers.tickers._asdict()
        for symbol, ticker in ordered_dict.items():
            info = help_get_sector_data(symbol, ticker)
            if info:
                list_of_lists.append(info)
            else:
                fail_count +=1
            if fail_count > 10 and fail_count % 5 == 0:
                print(f"Fail Count: {fail_count}")
                time.sleep(int(fail_count))
        time.sleep(60)
        symbol_count +=1
        if(symbol_count % 10 == 0):
            print(f"Symbols processed: {symbol_count}")


    # Create the pandas DataFrame
    df = pd.DataFrame(list_of_lists, columns = INFO_COLUMNS)
    # df.set_index('symbol')
    print(f"{fail_count} failed tickers")
    end_time = datetime.now()
    find_duration(start_time, end_time, year)
    time.sleep(60)
    return df

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
        print(f"{dir_path} not found")
        return data
    print(f"Reading from: {dir_path} directory")
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

def should_buy_stock(today_price, future_price, percent_gain):
    """
        Check if future price is greater than todays price * percent gain
        Example: todays price is $10. Future price is $13. Should return true
        because 10 * 1.2 = 12. 13 > 12. We want to buy this stock today because
        in the future it will be higher than the percent_gain
        if so, its a buy
    """
    if float(future_price) >= (float(today_price) * percent_gain):
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

        # todays_date = str(todays_date).replace("2013","2099")
        # todays_date = datetime.fromisoformat(todays_date)
        # print(f"Todays date: {todays_date}")
        # TODO(eriq): We skip the first point, but if we didn't we
        # would have to be careful on the label for the first point.
        # example start date: '2013-01-01'
        if (todays_date < datetime.strptime(start_date, "%Y-%m-%d")):
            print(f"Error: before start_date: {start_date}")
            print(str(point))
            break
        if (todays_date > datetime.strptime(end_date, "%Y-%m-%d")):
            print(f"Error: past end_date: {end_date}")
            print(str(point))
            break
        if (int(point[VOLUME_INDEX]) == 0):
            # print('Error: Volume is 0')
            continue
        future_date = todays_date + timedelta(days=N_DAYS)
        label = 0
        # if (future_date > datetime.strptime(end_date, "%Y-%m-%d")):
        #    if(flag):
        #        print(f"Todays date of {todays_date} + {N_DAYS} days ({future_date}) is greater than {end_date}")
        #        flag = False
            # continue

        if index + N_DAYS < len(data) and data[index + N_DAYS][OPEN_INDEX]:
            # price in the future
            future_price = data[index + N_DAYS][OPEN_INDEX]
        if point[OPEN_INDEX]:
            today_price = point[OPEN_INDEX]
        if future_price and today_price:
            if should_buy_stock(today_price, future_price, FOURTY_PERCENT_GAIN):
                label = 3
            elif should_buy_stock(today_price, future_price, TWENTY_PERCENT_GAIN):
                label = 2
            elif should_buy_stock(today_price, future_price, TEN_PERCENT_GAIN):
                label = 1
            else:
                label = 0

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

def output_to_csv(df, year):
    """
    Output dataframe to CSV
    """
    if not os.path.exists(CLEAN_DATA_DIR):
        os.makedirs(CLEAN_DATA_DIR)
    filename = f"NASDAQ_{year}.csv"
    output_path = os.path.join(CLEAN_DATA_DIR, filename)
    print(f"Outputting dataframe to {output_path}")
    df.to_csv(output_path, encoding='utf-8', index=False)

def create_df(features):
    """
    Create the DataFrame
    """
    df = pd.DataFrame(features, columns = COLUMNS)
    df = df[df.volume != 0]
    df.set_index("date")
    return df.sort_values(["date", "symbol"], ignore_index=False)

def get_broken_symbols_from_file(filename):
    with open(filename) as symbols_file:
        result = symbols_file.read().splitlines()
    print(f"Read {len(result)} broken symbols")
    return result

def get_and_output_sector():
    dir_path = "data/clean/symbols"
    symbols = read_all_symbols(dir_path)
    # filename = "error_symbols.txt"
    # list_of_broken_symbols = get_broken_symbols_from_file(filename)
    # clean_symbols = [ x for x in symbols if x not in list_of_broken_symbols ]
    # print(f"Number of symbols in list after cleaning: {len(clean_symbols)}")
    df = get_sector_data(symbols)
    # df = split_list_in_four(symbols)
    output_to_csv(df, 'all_years_info')

def print_to_file(year, symbols_list):
    filename = f"SYMBOLS_{year}.csv"
    with open(filename, 'w') as f:
        f.writelines("%s," % symbol for symbol in symbols_list)

def build_data(years):
    """
    Create one CSV file for each year in years
    """
    for year in years:
        start_time = datetime.now()
        data = fetch_data(year)
        print(f"{year} Raw Data: {len(data)}")
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        features = make_features(data, start_date, end_date)
        print("Feature Data: %d" % (len(features)))
        if not features:
            print('Error: features list is empty\n')
        else:
            df = create_df(features)
            symbols = df.symbol.unique()
            print(f"Read {len(df.index)} rows ({len(symbols)} symbols) of {year} data")
            # output_to_csv(df, year)
        end_time = datetime.now()
        find_duration(start_time, end_time, year)

def find_duration(start_time, end_time, year):
    duration = end_time - start_time # For build-in functions
    duration_in_s = duration.total_seconds() # Total number of seconds between dates
    print(f"{year} total time {duration_in_s}")


def main():
    start_time = datetime.now()
    years = ['2013', '2014', '2015', '2016', '2017', '2018', '2019']
    # years = ['2013']
    # print(f"Getting data for the following years: {years}")
    get_and_output_sector()
    # build_data(years)
    end_time = datetime.now()
    find_duration(start_time, end_time, 'All years')

if __name__ == '__main__':
    main()
