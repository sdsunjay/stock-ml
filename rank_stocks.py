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

def read_test_files(clf):
    ''' Read test files in test dir and make predictions using classifier (clf) '''
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
        dates = ['2018-05-01', '2018-05-02', '2018-05-03', '2018-05-04', '2018-05-07', '2018-05-08', '2018-05-09','2018-05-10', '2018-05-11', '2018-05-14', '2018-05-15', '2018-05-16', '2018-05-17', '2018-05-18', '2018-05-21', '2018-05-22', '2018-05-23', '2018-05-24', '2018-05-25', '2018-05-29', '2018-05-30', '2018-05-31', '2018-06-01']
        buy_prediction = 0
        sell_prediction = 0
        correct_buy_prediction = 0
        correct_sell_prediction = 0
        buy_accuracy = 0
        sell_accuracy = 0
        for i in range(len(predictions)):
            if predictions[i] == 1:
                buy_prediction += 1
                if predictions[i] == y[i]:
                    correct_buy_prediction +=1
            if buy_prediction != 0 and correct_buy_prediction !=0:
                buy_accuracy = correct_buy_prediction/buy_prediction
            if predictions[i] == 0:
                sell_prediction += 1
                if predictions[i] == y[i]:
                    correct_sell_prediction +=1
            if sell_prediction != 0 and correct_sell_prediction !=0:
                sell_accuracy = correct_sell_prediction/sell_prediction
        if(buy_accuracy > .80 and sell_accuracy > .80):
            symbol = filename.split('_')[0]
            print(symbol + ' Accuracy: ' + str(buy_accuracy) + ' ' + str(sell_accuracy))

def main():
    names = ["Logistic Regression", "Nearest Neighbors", "Decision Tree", "Random Forest", "Neural Net", "AdaBoost", "Naive Bayes", "QDA"]
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
        read_test_files(clf)
   # Plan of attack:
   # read each stock file, get all the features
   # run the classifier on each stock file, given 2 weeks of data to predict
   # for. Did the stock actually go up by more than 5% or not?
   # get the accuracy for those two months
   # store all the accuracies
   # rank the stocks in order of accuracies

if __name__ == '__main__':
    main()
