# stock-ml
stock ml for fun and profit

| Classifier | Accuracy (%) |
| --- | --- |
| Logistic Regression | 82.95 |
| Nearest Neighbors | 78.47 |
| Decision Tree | 82.03 |
| Random Forest | 82.42 |
| Neural Net | 82.56 |
| AdaBoost | 82.51 |
| Naive Bayes | 82.17 |
| QDA | 79.53 |

## How to run

* Get data files (files with 'symbol', 'date', 'open', 'high', 'low', 'close','volume')
* `python parse_data.py` # Output pandas dataframe to files split by year
