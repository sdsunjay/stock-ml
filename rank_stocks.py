from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC

# import numpy as np
# import matplotlib.pyplot as plt
# from matplotlib.colors import ListedColormap
# from sklearn.model_selection import train_test_split
# from sklearn.preprocessing import StandardScaler
# from sklearn.datasets import make_moons, make_circles, make_classification

import parse_data
import os.path
from sklearn.externals import joblib

def fetch_data_from_file(filename):
    ''' Read stock data from a CSV file'''
    data = []
    # print('Reading from: ' + filename)
    with open(filename) as f:
        for line in f:
            data.append(line.split("\t"))
    return data

def main():
    clf = joblib.load('Logistic Regression.pkl')
    if os.path.isdir(parse_data.CLEAN_TEST_DATA_DIR) == False:
          print(parse_data.CLEAN_TEST_DATA_DIR + ' not found')
          return
    data = []
    for filename in os.listdir(parse_data.CLEAN_TEST_DATA_DIR):
        path = os.path.join(parse_data.CLEAN_TEST_DATA_DIR, filename)
        if os.path.isfile(path) == False:
            print('Error: Skipping ' + path)
            continue
        data = fetch_data_from_file(path)
        # Pulled off label.
        x = [[float(val) for val in point[0:-1]] for point in data]
        y = [int(point[-1]) for point in data]
        predictions = clf.predict(x)

        if 1 in predictions:
            print(filename)
            print(str(predictions))
   # Plan of attack:
   # read each stock file, get all the features
   # run the classifier on each stock file, given 2 weeks of data to predict
   # for. Did the stock actually go up by more than 5% or not?
   # get the accuracy for those two months
   # store all the accuracies
   # rank the stocks in order of accuracies

if __name__ == '__main__':
    main()
