import pprint
import pymongo as pym
from pymongo import MongoClient
import datetime
import os
RAW_DATA_PATH = os.path.join('tmp')
# Stock symbol, Date, Open price, High Price, Low Price, Closing Price, Volume of shares
# AAON,20170103,33.45,33.7,32.95,33.35,137600
# FB,20170103,116.03,117.84,115.51,116.86,20663900
#

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

    #print(str(stocks))
    print(str(stock_list))
    return len(stock_list)
    # result = db_stocks.insert_many(stock_list)
    # print(str(result.inserted_ids))
    # return len(result.inserted_ids)

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
    if os.path.isdir(RAW_DATA_PATH) == False:
        print(RAW_DATA_PATH + ' not found')
        return data
    for filename in os.listdir(RAW_DATA_PATH):
        path = os.path.join(RAW_DATA_PATH, filename)

        if os.path.isfile(path) == False:
            continue

        if not path.endswith('.txt'):
            print('Skipping ' + path)
            continue

        print('Filename: ' + str(path))
        with open(path) as f:
            for line in f:
                data.append(line.strip().split(','))
            result += store_in_db(db_stocks, data)
    print('Result: ' + str(result))

def fetch_data_from_csv(db_stocks):
    ''' Read stock data from all CSV files in RAW_DATA_PATH'''
    data = []
    result = 0
    if os.path.isdir(RAW_DATA_PATH) == False:
        print(RAW_DATA_PATH + ' not found')
        return data
    for filename in os.listdir(RAW_DATA_PATH):
        path = os.path.join(RAW_DATA_PATH, filename)

        if os.path.isfile(path) == False:
            continue

        if not path.endswith('.csv'):
            print('Skipping ' + path)
            continue

        print('Filename: ' + str(path))
        with open(path) as f:
            # read first line to remove header
            f.readline()
            for line in f:
                data.append(line.strip().split(','))
            result += store_csv_in_db(db_stocks, data)
    print('Result: ' + str(result))

def main():
    # client = MongoClient()
    client = MongoClient('localhost', 27017)
    db = client['test-database']
    stocks = db.stocks
    # fetch_data_from_csv(stocks)
    fetch_data_from_txt(stocks)
    # db.stocks.remove({})
    # output_db(stocks)

    # pprint.pprint(stocks.find({"date": "2017-01-04 00:00:00"}))

if __name__ == '__main__':
    main()
