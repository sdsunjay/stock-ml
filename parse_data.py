import os
from os.path import basename

DATA_PATH = os.path.join('data')
RAW_DATA_PATH = os.path.join(DATA_PATH, 'raw/csv')
CLEAN_DATA_DIR = os.path.join(DATA_PATH, 'clean')
CLEAN_DATA_PATH_TRAIN = os.path.join(CLEAN_DATA_DIR, 'train_data.txt')
CLEAN_TEST_DATA_DIR = os.path.join(CLEAN_DATA_DIR, 'test')

MIN_DATE = '2017-01-01'
MAX_DATE = '2018-12-31'

DATE_INDEX = 1
CLOSING_INDEX = 2
VOLUME_INDEX = 6
N_DAYS = 14

def fetch_test_data(filename):
    ''' Read stock data from a CSV file'''
    data = []
    with open(filename) as f:
        # read first line to remove header
        f.readline()
        for line in f:
            data.append(line.strip().split(','))
    return data

def fetch_training_data():
    ''' Read stock data from all CSV files in RAW_DATA_PATH'''
    data = []
    print('Reading Raw data path: ' + RAW_DATA_PATH)
    if os.path.isdir(RAW_DATA_PATH) == False:
        print(RAW_DATA_PATH + ' not found')
        return data
    for filename in os.listdir(RAW_DATA_PATH):
        path = os.path.join(RAW_DATA_PATH, filename)

        if os.path.isfile(path) == False:
            continue

        with open(path) as f:
            # read first line to remove header
            f.readline()
            for line in f:
                data.append(line.strip().split(','))
    return data

def calculate_average_volume(data, index):
    sum1 = 0.0
    for i in range(N_DAYS):
        if float(data[index - i][VOLUME_INDEX]) == 0.0:
            continue
        sum1+= float(data[index - i][VOLUME_INDEX])
    # print('Average volume: ' + str(float(sum1/30)))
    # print('Todays volume: ' + str(data[index][VOLUME_INDEX]))
    # if todays volume is greater than average volume
    # if float(data[index][VOLUME_INDEX]) > float(sum1/N_DAYS):
    #     return 1
    # else:
    #    return 0
    return float("{0:.2f}".format(sum1/N_DAYS))

def calculate_rsi(data, index):
    sum_gain = 0.0
    sum_loss = 0.0
    for i in range(N_DAYS):
        if float(data[index - i][CLOSING_INDEX]) == 0.0:
            continue
        if float(data[index - (i+1)][CLOSING_INDEX]) == 0.0:
            continue
        temp = float(data[index - i][CLOSING_INDEX]) - float(data[index - (i+1)][CLOSING_INDEX])
        if(temp < 0):
            sum_loss+= abs(temp)
        elif(temp > 0):
            sum_gain+= temp
    if sum_loss == 0:
        sum_loss = 1.0
    if sum_gain == 0:
        sum_gain = 1.0
    rs = sum_gain / sum_loss
    rsi = 100 - (100/(1+rs))
    return float("{0:.2f}".format(rsi))

def calculate_simple_moving_average(data, index, n_days):
    sum1 = 0.0
    for i in range(n_days):
        if float(data[index - i][CLOSING_INDEX]) == 0.0:
            continue
        sum1+= float(data[index - i][CLOSING_INDEX])
    return float("{0:.2f}".format(sum1/n_days))

def should_buy_stock(today_price, future_price):
    if (float(today_price)*1.05) < float(future_price):
        return True
    return False


def make_features(data, start_date, end_date):
    points = []

    # We know the data is sorted.
    for index in range(len(data)):
        point = data[index]

        # TODO(eriq): We skip the first point, but if we didn't we
        # would have to be careful on the label for the first point.
        if (point[DATE_INDEX] < start_date):
            continue
        if (point[DATE_INDEX] > end_date):
            continue
        if (int(point[VOLUME_INDEX]) == 0):
            continue

        label = 0
        if index + N_DAYS < len(data):
            # price in the future
            future_price = data[index + N_DAYS][CLOSING_INDEX]
        else:
            continue

        today_price = data[index][CLOSING_INDEX]

        if should_buy_stock(today_price, future_price):
            label = 1
        ten_day_ma = calculate_simple_moving_average(data, index, 10)
        twenty_five_day_ma = calculate_simple_moving_average(data, index, 25)
        n_day_momentum = float("{0:.2f}".format(float(data[index][CLOSING_INDEX]) - float(data[index - N_DAYS][CLOSING_INDEX])))
        rsi = calculate_rsi(data, index)
        # volume = calculate_average_volume(data, index)
        features = [float(point[CLOSING_INDEX]), float(point[VOLUME_INDEX]), ten_day_ma, twenty_five_day_ma, n_day_momentum, rsi, label]

        if (features[0] <= 0.0):
            continue

        points.append(features)

    return points

def output_to_file(features, train, test, symbol):

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
        # TEST
        # print("Raw Data: %d" % (len(data)))

        features = make_features(data, start_date, end_date)

        if not features:
            print('Error: features list is empty\n')
        else:
            output_to_file(features, False, True, filename.split('.')[0])

def build_train_data(start_date, end_date):

    data = fetch_training_data()
    # TEST
    print("Raw Data: %d" % (len(data)))

    features = make_features(data, start_date, end_date)

    if not features:
        print('Error: features list is empty\n')
    else:
        output_to_file(features, True, False, '')

def main(train, test):

    if train:
        print('Training: ' )
        start_date = MIN_DATE
        end_date = MAX_DATE
        build_train_data(start_date, end_date)

    if test:
        print('Testing: ' )
        start_date = '2018-05-01'
        end_date = '2018-06-01'
        build_test_data(start_date, end_date)

    print('Start Date: ' + start_date)
    print('End Date: ' + end_date)


if __name__ == '__main__':
    train = True
    test = True
    main(train, test)
