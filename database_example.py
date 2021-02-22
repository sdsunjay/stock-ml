import sys
import traceback
import pprint
# from pymongo import MongoClient, errors
import pymongo
from pymongo.errors import BulkWriteError
import datetime
import os
DATA_PATH = os.path.join('data')
RAW_DATA_PATH = os.path.join(DATA_PATH, 'raw')
CSV_RAW_DATA_PATH = os.path.join(RAW_DATA_PATH, 'csv')
TXT_RAW_DATA_PATH = os.path.join(RAW_DATA_PATH, 'txt')

# stocks.create_index([("symbol", pym.DESCENDING), ("symbol", pym.ASCENDING)], unique=True)

def output_db(db_stocks):

    # find all documents
    results = db_stocks.find()
    print()
    print('+-+-+-+-+-+-+-+-+-+-+-+-+-+-')

    # display documents from collection
    for record in results:
        # print out the document
        print(record['Symbol'] + ',' + str(record['Date']) + ',' +
        str(record['Close']) )

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
            stock['Date'] = date
            stock['Symbol'] = symbol
            stock['Open'] = float(open_price)
            stock['High'] = float(high_price)
            stock['Low'] = float(low_price)
            stock['Close'] = float(close_price)
            stock['Volume'] = int(volume)
            if int(volume) != 0:
                stock_list.append(stock)
            else:
                print('Skipped: ' + str(date))
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
    try:
        result = db_stocks.insert_many(stock_list)
        return len(result.inserted_ids), symbol
    except BulkWriteError as bwe:
        pprint(bwe.details)
        return 0, symbol
    # return len(stock_list), 'SUNJAY_FTW'
    #  print(str(result.inserted_ids))


def store_in_db(db_stocks, data):
    stock_list = []
    for line in data:
        try:
            # Empty dict
            stock = {}
            symbol = line[0]
            date = datetime.datetime.strptime(line[1], "%Y%m%d")
            open_price = line[2]
            high_price = line[3]
            low_price = line[4]
            close_price = line[5]
            volume = line[6]
            stock['Date'] = date
            stock['Symbol'] = symbol
            stock['Open'] = float(open_price)
            stock['High'] = float(high_price)
            stock['Low'] = float(low_price)
            stock['Close'] = float(close_price)
            stock['Volume'] = int(volume)
            if int(volume) != 0:
                stock_list.append(stock)
        except ValueError:
            print("Could not convert data to a float in " + str(symbol))
            print("-"*60)
            traceback.print_exc(file=sys.stdout)
            print("-"*60)
            continue
            # traceback.print_stack()
            # print(repr(traceback.extract_stack()))
            # print(repr(traceback.format_stack()))
            # return 0, symbol
        except Exception:
            print("Exception in user code:")
            print("-"*60)
            traceback.print_exc(file=sys.stdout)
            print("-"*60)
            continue
            # return 0, symbol

    #print(str(stocks))
    # print(str(stock_list))
    try:
        if len(stock_list) == 0:
            print('Stock list is empty for '+ str(date))
            return 0
        else:
            result = db_stocks.insert_many(stock_list)
    except BulkWriteError as bwe:
        pprint(bwe.details)
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
                print("Error Occurred")
            else:
                print('Stored: ' + str(result) +' lines')
            result = 0
            data.clear()

def get_csv_symbols():
    ''' Get names of files (symbols) for all CSV files in RAW_DATA_PATH'''
    symbols = []
    result = 0
    if os.path.isdir(CSV_RAW_DATA_PATH) == False:
        print(CSV_RAW_DATA_PATH + ' not found')
        return data
    for filename in os.listdir(CSV_RAW_DATA_PATH):
        path = os.path.join(CSV_RAW_DATA_PATH, filename)

        if os.path.isfile(path) == False:
            continue
        if not filename.endswith('.csv'):
            print('Skipping ' + path)
            continue
        filename, file_extension = os.path.splitext(filename)
        symbols.append(filename)
        # if 'data/raw/csv/BRK.B.csv' not in path:
        #     print('Skipping ' + path)
        #     continue
    return symbols

def fetch_data_from_csv(db_stocks, symbol_list):
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
        filename, file_extension = os.path.splitext(filename)
        if filename not in symbol_list :
            print('Skipping ' + filename + '(Duplicate)')
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
            # sys.exit("Some Error Message")
        else:
            print('Stored: ' + str(result) +' lines')
        result = 0
        data.clear()

def get_symbols(db_stocks):
    symbols  = db_stocks.distinct( "Symbol" )
    return symbols

def fetch_column_from_db(db_stocks, symbol, column):
    column_list = []
    cursor = db_stocks.find( { 'symbol': symbol }, { column: 1, '_id':0 } )
    for row in cursor:
    #    print(str(row))
        column_list.append(row)
    # print(str(date_list))
    return column_list

def main():
    try:
        # Connection to Mongo DB
        client = pymongo.MongoClient('localhost', 27017)
        db = client['csv-stock-database']
        # fetch_data_from_txt(db.stocks)
        # db.stocks.remove({})
        # output_db(db.stocks)
        symbols = get_symbols(db.stocks)
        csv_symbols = get_csv_symbols()
        main_list = list(set(csv_symbols) - set(symbols))
        fetch_data_from_csv(db.stocks, main_list)
        # print(str(main_list))
        # date_dict_list = fetch_column_from_db(db.stocks, 'AAPL', 'date')
        # date_list = [f['date'] for f in date_dict_list]
        # print(str(date_list))
        # for symbol in symbols:
        #     date_dict_list = fetch_column_from_db(db.stocks, symbol, 'date')
        #     date_list = [f['date'] for f in date_dict_list]
        #    fetch_column_from_db(db.stocks, symbol, 'close_price')
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)

if __name__ == '__main__':
    main()
