import os

DATA_PATH = os.path.join('data')
RAW_DATA_PATH = os.path.join(DATA_PATH, 'raw')
CLEAN_DATA_DIR = os.path.join(DATA_PATH, 'clean')
CLEAN_DATA_PATH = os.path.join(CLEAN_DATA_DIR, 'data.txt')

MIN_DATE = '2018-01-01'

DATE_INDEX = 1
CLOSING_INDEX = 2
VOLUME_INDEX = 6
N_DAYS = 14

def fetch_data():
    data = []
    if os.path.isdir(RAW_DATA_PATH) == False:
        print(RAW_DATA_PATH + ' not found')
        return data
    for filename in os.listdir(RAW_DATA_PATH):
        path = os.path.join(RAW_DATA_PATH, filename)

        with open(path, 'r') as file:
            file.readline()
            for line in file:
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
    return float(sum1/N_DAYS)


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

def make_features(data):
    points = []

    # We know the data is sorted.
    for index in range(len(data)):
        point = data[index]

        # TODO(eriq): We skip the first point, but if we didn't we
        # would have to be careful on the label for the first point.
        if (point[DATE_INDEX] < MIN_DATE):
            continue
        if (int(point[VOLUME_INDEX]) == 0):
            continue

        label = 0
        # yesterday = data[index - 1][CLOSING_INDEX]
        n_days_ago = data[index - N_DAYS][CLOSING_INDEX]
        today = data[index][CLOSING_INDEX]
        if float(today) > (float(n_days_ago)*1.05):
            label = 1
        ten_day_ma = calculate_simple_moving_average(data, index, 10)
        twenty_five_day_ma = calculate_simple_moving_average(data, index, 25)
        n_day_momentum = float("{0:.2f}".format(float(data[index][CLOSING_INDEX]) - float(data[index - N_DAYS][CLOSING_INDEX])))
        rsi = calculate_rsi(data, index)
        volume = calculate_average_volume(data, index)
        features = [float(point[CLOSING_INDEX]), float(volume), ten_day_ma, twenty_five_day_ma, n_day_momentum, rsi, label]

        if (features[0] <= 0.0):
            continue

        points.append(features)

    return points

def main():
    data = fetch_data()

    # TEST
    print("Raw Data: %d" % (len(data)))

    features = make_features(data)
    if not features:
        print('Error\n')
        return
    # TEST
    print("Featureized: %d" % (len(features)))
    if not os.path.exists(CLEAN_DATA_DIR):
        os.makedirs(CLEAN_DATA_DIR)

    with open(CLEAN_DATA_PATH, 'w') as file:
        file.write('\n'.join(['\t'.join([str(part) for part in point]) for point in features]))

if __name__ == '__main__':
    main()
