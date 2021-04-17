from matplotlib.dates import num2date
import os
from os.path import basename
from datetime import datetime, timedelta, timezone
import pandas as pd
from models import sma
from models import ets
from models import knn
from models import regression
# does not work because of pmdarima, which does not install on OSX
# from models import arima
from models import fbprophet
from models import neural_networks
COLUMNS = ['symbol', 'date', 'open', 'high', 'low', 'close','volume', 'label']
DATA_PATH = os.path.join('data/clean')
CLEAN_DATA_DIR = os.path.join(DATA_PATH, 'default')
INFO_COLUMNS = ['symbol', 'sector', 'industry']

def get_sector_and_industry_dict(path):
    df = pd.read_csv(path, usecols=INFO_COLUMNS)
    return df.set_index('symbol').T.to_dict('list')

def prediction_techniques(grouped, symbols, years):
    year = years[0]
    print(f"Year: {year}")

    for symbol in symbols:
        df = grouped.get_group(symbol)
        # 1440 minutes is equivalent to 1 day
        s_interval = "1440min"
        actions = ["sma"]
        df.rename(columns = {'close':'5. adjusted close'}, inplace = True)
        df.set_index('date')
        df.index = df.date
        # print(df.index[-1])
        # df.loc[df['symbol'] == 'AAPL']
        # df['5. adjusted close'] = df.loc['close'].copy()
        l_args=""
        print('SMA')
        sma.simple_moving_average("", symbol, df)
        # choice = input("Go to next model? (press any key to continue or q to quit )")
        # if choice == 'q':
        #    import sys
        #    sys.exit(0)
        print('ETS')
        ets.exponential_smoothing(l_args, symbol, df)
        print('KNN')
        knn.k_nearest_neighbors(l_args, symbol, df)
        print('LINEAR Regressions')
        regression.regression(l_args, symbol, df, regression.LINEAR)
        print('QUADRATIC Regressions')
        regression.regression(l_args, symbol, df, regression.QUADRATIC)
        print('CUBIC Regressions')
        regression.regression(l_args, symbol, df, regression.CUBIC)
        # arima.arima(l_args, symbol, df)
        fbprophet.fbprophet(l_args, symbol, df)
        neural_networks.mlp(l_args, symbol, df)
        neural_networks.rnn(l_args, symbol, df)
        neural_networks.lstm(l_args, symbol, df)

def read_from_csv(path):
    return pd.read_csv(path, usecols=COLUMNS)

def fetch_data(year):
    ''' Read stock data from CSV'''
    data = []
    filename = f"NASDAQ_{year}.csv"
    file_path = os.path.join(CLEAN_DATA_DIR, filename)
    if os.path.isfile(file_path):
        print(f"Reading from: {file_path}")
        return read_from_csv(file_path)
    else:
        print(f"Error: Attempted to read from {file_path}")
        return

def build_data(years):
    """
    Create one dataframe from each year in years
    """
    list_of_df = []
    for year in years:
        df = fetch_data(year)
        print(f"Read {len(df.index)} rows for {year}")
        list_of_df.append(fetch_data(year))
        return pd.concat(list_of_df)

def first():
    years = ['2018']
    print(f"Getting data for the following years: {years}")
    df = build_data(years)
    df.reset_index(drop=True, inplace=True)
    df['date'] = df.date.apply(lambda x: datetime.strptime(f"{x} 00:00:00", "%Y-%m-%d %H:%M:%S"))
    grouped = df.groupby('symbol')
    symbols = grouped.groups
    print(f"Read {len(symbols)} unique tickers")
    print("In testing phase, only using 'AAPL'")
    symbols = ["AAPL"]
    # print(df.head())
    # print(df.loc[df['symbol'] == 'AAPL'].head())
    # print(df.loc[df['symbol'] == 'AAPL'].head())
    # path = 'NASDAQ_2019_INFO.csv'
    # sector_and_industry_dict = get_sector_and_industry_dict(path)
    # df.loc[df['symbol'] == 'AAPL']
    prediction_techniques(grouped, symbols, years)

def main():
    df = first()

if __name__ == '__main__':
    main()
