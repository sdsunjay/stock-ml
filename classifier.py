from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score

import parse_data

def main():
    data = []
    with open(parse_data.CLEAN_DATA_PATH, 'r') as file:
        for line in file:
            data.append(line.split("\t"))

    # Pulled off label.
    x = [[float(val) for val in point[0:-1]] for point in data]
    y = [int(point[-1]) for point in data]

    # TEST
    print(len(data))

    classy = LogisticRegression()

    accuracy = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'accuracy').mean()
    precision = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'precision').mean()
    recall = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'recall').mean()
    f1 = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'f1').mean()

    print('Accuracy: %f, Precision: %f, Recall: %f, F1: %f' % (accuracy, precision, recall, f1))

if __name__ == '__main__':
    main()
