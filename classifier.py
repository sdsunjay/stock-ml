from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.model_selection import cross_val_score, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC

from sklearn.neural_network import MLPClassifier
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis

import parse_data
import os.path
import joblib

def train_classifier(name, classy, x, y):
    # Split into training and test
    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.10)
    # training
    classy.fit(X_train, y_train)
    accuracy = classy.score(X_test, y_test)
    print('Accuracy: ' + str(accuracy))
    joblib.dump(classy, 'classifiers/'+ str(name)+'.pkl')

def train_and_predict(classy, x, y):
    accuracy = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'accuracy').mean()
    precision = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'precision').mean()
    recall = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'recall').mean()
    f1 = cross_val_score(classy, X = x, y = y, cv = 10, scoring = 'f1').mean()

    print('Accuracy: %f, Precision: %f, Recall: %f, F1: %f' % (accuracy, precision, recall, f1))


def main():
    data = []
    if os.path.isfile(parse_data.CLEAN_DATA_PATH_TRAIN) == False:
          print(parse_data.CLEAN_DATA_PATH_TRAIN + ' not found')
          return
    with open(parse_data.CLEAN_DATA_PATH_TRAIN, 'r') as file:
        print('Reading from: ' + parse_data.CLEAN_DATA_PATH_TRAIN)
        for line in file:
            data.append(line.split("\t"))

    # Pulled off label.
    x = [[float(val) for val in point[0:-1]] for point in data]
    y = [int(point[-1]) for point in data]

    names = ["Logistic Regression", "Nearest Neighbors", "RBF SVM", "RBF SVM .1 Gamma", "Decision Tree", "Random Forest", "Neural Net", "AdaBoost", "Naive Bayes", "QDA"]

    classifiers = [
    LogisticRegression(),
    KNeighborsClassifier(3),
    SVC(gamma=2, C=1),
    SVC(kernel='rbf', C=1, gamma=0.10000000000000001),
    DecisionTreeClassifier(max_depth=5),
    RandomForestClassifier(max_depth=5, n_estimators=10, max_features=2),
    MLPClassifier(alpha=1),
    AdaBoostClassifier(),
    GaussianNB(),
    QuadraticDiscriminantAnalysis()]

    classifiers = [
    LogisticRegression(),
    KNeighborsClassifier(3),
    DecisionTreeClassifier(max_depth=5),
    RandomForestClassifier(max_depth=5, n_estimators=10, max_features=2),
    MLPClassifier(alpha=1),
    AdaBoostClassifier(),
    GaussianNB(),
    QuadraticDiscriminantAnalysis()]
    names = ["Logistic Regression", "Nearest Neighbors", "Decision Tree", "Random Forest", "Neural Net", "AdaBoost", "Naive Bayes", "QDA"]

    # names = ['Logistic Regression']
    # classifiers = [LogisticRegression()]

    # TEST
    print(len(data))
    # iterate over classifiers
    for name, clf in zip(names, classifiers):
        print(name)
        train_classifier(name, clf, x, y)

if __name__ == '__main__':
    main()
