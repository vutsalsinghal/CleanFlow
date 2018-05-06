#!/bin/bash

spark-submit test.py kmeans 1 3
spark-submit test.py kmeans 3 3
spark-submit test.py kmeans 5 3
spark-submit test.py kmeans 10 3

spark-submit test.py bisectingkmeans 1 4
spark-submit test.py bisectingkmeans 3 4
spark-submit test.py bisectingkmeans 5 4
spark-submit test.py bisectingkmeans 10 4

spark-submit test.py gaussian 1 5
spark-submit test.py gaussian 3 5
spark-submit test.py gaussian 5 5
spark-submit test.py gaussian 10 5
