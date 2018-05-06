import os
import sys
import time
from cleanflow.preprocessing import *
from cleanflow.exploratory import Outlier
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

starttime = time.time()

clusterType = sys.argv[1]
num_features = int(eval(sys.argv[2]))
clusters = int(eval(sys.argv[3]))

# Read datase
df = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load('311.csv')

# Pre-process dataframe
df = cleanColumnNames(df)
cols = ["Unique Key", "Incident Zip", "X Coordinate (State Plane)", "Y Coordinate (State Plane)", "Latitude", "Longitude"]
double_cols = ["X Coordinate (State Plane)", "Y Coordinate (State Plane)", "Latitude", "Longitude"]
for col in cols:
    df = drop_null(df, col)
for col in double_cols:
    df = cast_to_double(df, col)
df = cast_to_int(df, "Unique Key")
df = cast_to_int(df, "Incident Zip")

columns = ["Latitude"]

# Find outliers
outlier = Outlier.cluster_type(clusterType)
outlier.set_param(k = clusters)
outlier.fit(df, columns[:num_features])
summary = outlier.summary()
summary.show()

print("Cluster type: {0:s}, # features: {1:d}, k: {2:d}".format(clusterType, num_features, clusters))
print("Time: " + str(time.time() - starttime))

with open("results.md", "a") as res:
    res.write("Cluster type: {0:s}, # features: {1:d}, k: {2:d}".format(clusterType, num_features, clusters))
    res.write("\nTime: " + str(time.time() - starttime))
    res.write('\n\n')

res.close()
