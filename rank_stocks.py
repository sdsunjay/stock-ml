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

    names = ["Logistic Regression", "Nearest Neighbors", "RBF SVM", "RBF SVM .1 Gamma", "Decision Tree", "Random Forest", "Neural Net", "AdaBoost", "Naive Bayes", "QDA"]
    for name in names:
        print('Classifier: ' + name)
        classifier_path = 'classifiers/' + name + '.pkl'
        if os.path.isfile(classifier_path) == False:
            print('Error: Skipping ' + classifier_path)
            continue
        clf = joblib.load(classifier_path)
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
            dates = ['2018-06-01', '2018-06-04', '2018-06-05', '2018-06-06',
            '2018-06-07', '2018-06-08', '2018-06-11', '2018-06-12',
            '2018-06-13', '2018-06-14', '2018-06-15', '2018-06-18',
            '2018-06-19', '2018-06-20', '2018-06-21', '2018-06-22',
            '2018-06-25']
            if 1 in predictions:
                print(filename)
                print(str(dates))
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
