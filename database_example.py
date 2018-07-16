import pprint
import pymongo as pym
from pymongo import MongoClient
import datetime
# Stock symbol, Date, Open price, High Price, Low Price, Closing Price, Volume of shares
# AAON,20170103,33.45,33.7,32.95,33.35,137600
# FB,20170103,116.03,117.84,115.51,116.86,20663900
#

def insert(db):
    symbol = 'FB'
    open_price = 116.03
    high_price = 117.84
    low_price = 115.51
    close_price = 116.86
    volume = 20663900
    date = datetime.datetime.strptime("20170103", "%Y%m%d")

    collection = db['stocks']
    stock = {}
    stock['date'] = date
    stock['symbol']  = symbol
    stock['open_price'] = open_price
    stock['high_price'] = high_price
    stock['close_price'] = close_price
    stock['volume'] = volume
    stocks = db.stocks
    collection.insert(stock)
    # stocks.create_index([("symbol", pym.DESCENDING), ("symbol", pym.ASCENDING)], unique=True)
    # stocks.createIndex( { symbol: symbol, date: date }, {unique: true} )


def main():
    # client = MongoClient()
    client = MongoClient('localhost', 27017)
    db = client['test-database']
    stocks = db.stocks
    # db.stocks.remove({})
    # insert(db)
    # insert(db)

    # pprint.pprint(stocks.find({"date": "2017-01-04 00:00:00"}))
    # find all documents
    results = stocks.find()
    print()
    print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-')

    # display documents from collection
    for record in results:
        # print out the document
        print(record['symbol'] + ',',record['date'])
if __name__ == '__main__':
    main()
