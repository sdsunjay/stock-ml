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
        stock['symbol']  = symbol
        stock['open_price'] = open_price
        stock['high_price'] = high_price
        stock['close_price'] = close_price
        stock['volume'] = volume
        stock_list.append(stock)

    #print(str(stocks))
    db_stocks.insert_many(stock_list)

def fetch_data(db_stocks):
    ''' Read stock data from all CSV files in RAW_DATA_PATH'''
    data = []
    if os.path.isdir(RAW_DATA_PATH) == False:
        print(RAW_DATA_PATH + ' not found')
        return data
    for filename in os.listdir(RAW_DATA_PATH):
        path = os.path.join(RAW_DATA_PATH, filename)

        if os.path.isfile(path) == False:
            continue

        with open(path) as f:
            for line in f:
                data.append(line.strip().split(','))
        store_in_db(db_stocks, data)
    return data


def main():
    # client = MongoClient()
    client = MongoClient('localhost', 27017)
    db = client['test-database']
    stocks = db.stocks
    fetch_data(stocks)
    # db.stocks.remove({})

    # pprint.pprint(stocks.find({"date": "2017-01-04 00:00:00"}))
    # find all documents
    results = stocks.find()
    print()
    print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-')

    # display documents from collection
    for record in results:
        # print out the document
        print(record['symbol'] + ',' + str(record['date']) + ',' +
        str(record['close_price']) )
if __name__ == '__main__':
    main()
