import sys
import traceback
import pprint
import pymongo as pym
from pymongo import MongoClient
import datetime
import os
DATA_PATH = os.path.join('data')
RAW_DATA_PATH = os.path.join(DATA_PATH, 'raw')
CSV_RAW_DATA_PATH = os.path.join(RAW_DATA_PATH, 'csv')
TXT_RAW_DATA_PATH = os.path.join(RAW_DATA_PATH, 'txt')

# stocks.create_index([("symbol", pym.DESCENDING), ("symbol", pym.ASCENDING)], unique=True)
# stocks.createIndex( { symbol: symbol, date: date }, {unique: true} )

def output_db(db_stocks):

    # find all documents
    results = db_stocks.find()
    print()
    print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-')

    # display documents from collection
    for record in results:
        # print out the document
        print(record['symbol'] + ',' + str(record['date']) + ',' +
        str(record['close_price']) )

def store_csv_in_db(db_stocks, data):
    stock_list = []
    for line in data:
        # Empty dict
        try:
            stock = {}
            symbol = line[0]
            date = datetime.datetime.strptime(line[1], "%Y-%m-%d")
            close_price = line[2]
            high_price = line[3]
            low_price = line[4]
            open_price = line[5]
            volume = line[6]
            stock['date'] = date
            stock['symbol'] = symbol
            stock['open_price'] = float(open_price)
            stock['high_price'] = float(high_price)
            stock['close_price'] = float(close_price)
            stock['volume'] = int(volume)
            stock_list.append(stock)
        except ValueError:
            print("Could not convert data to a float in " + str(symbol))
            print("-"*60)
            traceback.print_exc(file=sys.stdout)
            print("-"*60)
            # traceback.print_stack()
            # print(repr(traceback.extract_stack()))
            # print(repr(traceback.format_stack()))
            return 0, symbol
        except Exception:
            print("Exception in user code:")
            print("-"*60)
            traceback.print_exc(file=sys.stdout)
            print("-"*60)
            return 0, symbol
    # result = db_stocks.insert_many(stock_list)
    # return len(result.inserted_ids)
    return len(stock_list), 'SUNJAY_FTW'
    #  print(str(result.inserted_ids))


def store_in_db(db_stocks, data):
    stock_list = []
    for line in data:
        # Empty dict
        stock = {}
        symbol = line[0]
        date = datetime.datetime.strptime(line[1], "%Y%m%d")
        open_price = line[2]
        high_price = line[3]
        low_price = line[4]
        close_price = line[5]
        volume = line[6]
        stock['date'] = date
        stock['symbol'] = symbol
        stock['open_price'] = float(open_price)
        stock['high_price'] = float(high_price)
        stock['close_price'] = float(close_price)
        stock['volume'] = int(volume)
        stock_list.append(stock)

    #print(str(stocks))
    # print(str(stock_list))
    result = db_stocks.insert_many(stock_list)
    # print(str(result.inserted_ids))
    return len(result.inserted_ids)

def fetch_data_from_txt(db_stocks):
    ''' Read stock data from all TXT files in RAW_DATA_PATH'''
    data = []
    result = 0
    if os.path.isdir(TXT_RAW_DATA_PATH) == False:
        print(RAW_DATA_PATH + ' not found')
        return data
    for dir_name in os.listdir(TXT_RAW_DATA_PATH):
        dir_path = os.path.join(TXT_RAW_DATA_PATH, dir_name)
        print('Dir path: ' + str(dir_path))
        for filename in os.listdir(dir_path):
            path = os.path.join(dir_path, filename)
            print('Filename: ' + str(path))

            if os.path.isfile(path) == False:
                print('Not file. Skipping ' + path)
                continue

            if not path.endswith('.txt'):
                print('Not txt file. Skipping ' + path)
                continue

            with open(path) as f:
                for line in f:
                    data.append(line.strip().split(','))
            result = store_in_db(db_stocks, data)
            if result == 0:
                print("Some Error Message")
                result = 0
            else:
                print('Stored: ' + str(result) +' lines')
                result = 0

def fetch_data_from_csv(db_stocks):
    ''' Read stock data from all CSV files in RAW_DATA_PATH'''
    data = []
    result = 0
    if os.path.isdir(CSV_RAW_DATA_PATH) == False:
        print(CSV_RAW_DATA_PATH + ' not found')
        return data
    for filename in os.listdir(CSV_RAW_DATA_PATH):
        path = os.path.join(CSV_RAW_DATA_PATH, filename)

        if os.path.isfile(path) == False:
            continue
        if not path.endswith('.csv'):
            print('Skipping ' + path)
            continue
        # if 'data/raw/csv/BRK.B.csv' not in path:
        #     print('Skipping ' + path)
        #     continue

        print('Filename: ' + str(path))
        with open(path) as f:
            # read first line to remove header
            f.readline()
            for line in f:
                data.append(line.strip().split(','))
        result, failed_stock = store_csv_in_db(db_stocks, data)
        if result == 0:
            print('Failed: ' + str(failed_stock))
            result = 0
            # sys.exit("Some Error Message")
        else:
            print('Stored: ' + str(result) +' lines')
            result = 0

def main():
    # client = MongoClient()
    client = MongoClient('localhost', 27017)
    db = client['test-stock-database']
    stocks = db.stocks
    fetch_data_from_csv(stocks)
    # fetch_data_from_txt(stocks)
    # db.stocks.remove({})
    # output_db(stocks)

    # pprint.pprint(stocks.find({"date": "2017-01-04 00:00:00"}))

if __name__ == '__main__':
    main()
