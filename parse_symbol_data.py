import os
from os.path import basename
from datetime import datetime, timedelta
import hashlib
import pandas as pd
import numpy as np
import time

DATA_PATH = os.path.join('new_symbols')
RAW_DATA_PATH = os.path.join(DATA_PATH, 'data')
CLEAN_DATA_DIR = os.path.join(DATA_PATH, 'clean')
COLUMNS = ['symbol', 'date', 'adjusted close', 'volume', 'label']

TEN_PERCENT_GAIN = 1.10
TWENTY_PERCENT_GAIN = 1.20
FOURTY_PERCENT_GAIN = 1.40

# symbol,0. date,1. open,2. high,3. low,4. close,5. adjusted close,6. volume,7. dividend amount,8. split coefficient
SYMBOL_INDEX = 0
DATE_INDEX = 1
OPEN_INDEX = 2
HIGH_INDEX = 3
LOW_INDEX = 4
CLOSE_INDEX = 5
ADJUSTED_CLOSE = 6
VOLUME_INDEX = 7
N_DAYS = 14


def get_number_of_files(dir_name):
    return len([name for name in os.listdir('.') if os.path.isfile(name)])

def fetch_data():
    ''' Read stock data from all CSV files in RAW_DATA_PATH'''
    data = []
    dir_path = RAW_DATA_PATH
    if os.path.isdir(dir_path) == False:
        print(f"{dir_path} not found")
        return data
    print(f"Reading from: {dir_path} directory")
    file_counter = 0
    features = []
    for filename in os.listdir(dir_path):
        if filename.endswith('.csv') == False:
            print(f"Error: Not a CSV file: {filename}")
            continue
        data = []

        path = os.path.join(dir_path, filename)

        if os.path.isfile(path) == False:
            print(f"Error: Not a file: {path}")
            continue

        with open(path) as f:
            symbol = os.path.splitext(filename)[0]
            file_counter += 1
            # read first line to remove header
            f.readline()
            for line in f:
                line = line.strip().split(',')
                line.insert(0, symbol)
                data.append(line)
            if file_counter % 1000 == 0:
                print(f"Read {file_counter} files. At {filename}")
        features = make_features(data, features)
    print(f"Read {file_counter} files (symbols)")

    return features

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
    # example start date in file is 2018-01-02
    return datetime.strptime(date_str, '%Y-%m-%d')



# symbol,0. date,1. open,2. high,3. low,4. close,5. adjusted close,6. volume,7. dividend amount,8. split coefficient

# It make sense to do this one file at a time
# so for all PLTR.csv, label it.
# then for all BABA.csv, label it.
# so on and so forth
def make_features(data, points):
    """
    Label the data and remove some data where volume is 0
    """

    # points = []
    # print("Making features from data")
    for index in range(len(data)):
        point = data[index]
        todays_date = get_date(point[DATE_INDEX])
        if (float(point[ADJUSTED_CLOSE]) <= 0.0):
            # print('Error: adjusted close is 0')
            continue
        if (float(point[VOLUME_INDEX]) <= 0.0):
            # print('Error: Volume is 0')
            continue
        if (index + N_DAYS) >= len(data):
            continue
        label = 0
        future_price = 0
        if index + N_DAYS < len(data) and data[index + N_DAYS][ADJUSTED_CLOSE]:
            # price N_DAYS in the future, counting only days market is open
            future_price = data[index + N_DAYS][ADJUSTED_CLOSE]
        if point[ADJUSTED_CLOSE]:
            today_price = point[ADJUSTED_CLOSE]
        if future_price and today_price:
            if should_buy_stock(today_price, future_price, TWENTY_PERCENT_GAIN):
                # print('Point: ' + str(point))
                # print('todays price: ' + str(today_price))
                # print('future price: ' + str(future_price))
                # print('Future point: ' + str(data[index + N_DAYS]))
                label = 2
            elif should_buy_stock(today_price, future_price, TEN_PERCENT_GAIN):
                label = 1

        # Add adjusted close
        features = [point[SYMBOL_INDEX], todays_date, float(point[ADJUSTED_CLOSE]), float(point[VOLUME_INDEX]), label]
        points.append(features)
    return points

def output_to_csv(df, filename):
    """
    Output dataframe to CSV
    """
    if not os.path.exists(CLEAN_DATA_DIR):
        os.makedirs(CLEAN_DATA_DIR)
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


def build_data():
    """
    Create one CSV file
    """
    start_time = datetime.now()
    features = fetch_data()
    print("Feature Data: %d" % (len(features)))
    if not features:
        print('Error: features list is empty\n')
    else:
        df = create_df(features)
        symbols = df.symbol.unique()
        print(f"Read {len(df.index)} rows ({len(symbols)} symbols) of data")
        split_and_output(df)
    end_time = datetime.now()
    find_duration(start_time, end_time)

def split_and_output(df):
    start_year = 1999
    end_year = 2021
    df = df.set_index(df['date'])
    df = df.sort_index()
    print(f"Outputting from {df.index.min()} to {df.index.max()}")
    year = start_year
    while year <= end_year:
        output_df = df[f"{str(year)}-01-01":f"{str(year)}-12-30"]
        output_to_csv(output_df, f"ALL_{year}.csv")
        year += 1

def find_duration(start_time, end_time):
    duration = end_time - start_time # For build-in functions
    duration_in_s = duration.total_seconds() # Total number of seconds between dates
    print(f"total time {duration_in_s}")


def main():
    start_time = datetime.now()
    build_data()
    end_time = datetime.now()
    find_duration(start_time, end_time)

if __name__ == '__main__':
    main()
