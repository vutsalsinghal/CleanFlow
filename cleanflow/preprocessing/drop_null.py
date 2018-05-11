from cleanflow.assertions import *
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

def drop_null(df, column, summary=False):
    '''
    Drop rows that have null value in the given row

    Parameter
    ---------
    df     : dataframe
    column : column to be processed

    Return
    ------
    df     : dataframe
    summary: summary of action performed (type='pyspark.sql.dataframe.DataFrame')
    '''
    newDF = df.where(col(column).isNotNull())
    
    if summary:
        previousTotRows = df.count()
        newTotRows = newDF.count()
        summary = sqlContext.createDataFrame([(previousTotRows, newTotRows, previousTotRows-newTotRows)],['Previous Row Count', 'New Row Count','Rows affected'])
        return (newDF, summary)
    return newDF