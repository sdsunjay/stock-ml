import os

DATA_PATH = os.path.join('data')
RAW_DATA_PATH = os.path.join(DATA_PATH, 'raw')
CLEAN_DATA_DIR = os.path.join(DATA_PATH, 'clean')
CLEAN_DATA_PATH = os.path.join(CLEAN_DATA_DIR, 'data.txt')

MIN_DATE = '2018-01-01'

DATE_INDEX = 1
CLOSING_INDEX = 2
VOLUME_INDEX = 6

def fetch_data():
    data = []

    for filename in os.listdir(RAW_DATA_PATH):
        path = os.path.join(RAW_DATA_PATH, filename)

        with open(path, 'r') as file:
            file.readline()
            for line in file:
                data.append(line.strip().split(','))

    return data

def make_features(data):
    points = []

    # We know the data is sorted.
    for index in range(len(data)):
        point = data[index]

        # TODO(eriq): We skip the first point, but if we didn't we
        # would have to be careful on the label for the first point.
        if (point[DATE_INDEX] < MIN_DATE):
            continue

        label = 0
        if (data[index - 1][CLOSING_INDEX] < data[index][CLOSING_INDEX]):
            label = 1

        features = [float(point[CLOSING_INDEX]), int(point[VOLUME_INDEX]), label]

        if (features[0] <= 0.0 or features[1] <= 0):
            continue

        points.append(features)

    return points

def main():
    data = fetch_data()

    # TEST
    print("Raw Data: %d" % (len(data)))

    features = make_features(data)

    # TEST
    print("Featureized: %d" % (len(features)))

    os.makedirs(CLEAN_DATA_DIR, exist_ok = True)

    with open(CLEAN_DATA_PATH, 'w') as file:
        file.write('\n'.join(['\t'.join([str(part) for part in point]) for point in features]))

if __name__ == '__main__':
    main()
