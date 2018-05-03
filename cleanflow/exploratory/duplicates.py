from cleanflow.assertions import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

def check_duplicates(df, column):
    """
    Function to check no.of values that occur more than once.

    Parameter
    ---------
    df     : DataFrame to be processed.
    column : column in df for which we want to find duplicate values.
    """
    assert_cols_in_df(df, columns_provided=[column], columns_df=df.columns)
    df.createOrReplaceTempView("df")
    return sqlContext.sql("SELECT %s as DuplicateValue, COUNT(*) as Count FROM df GROUP BY %s HAVING COUNT(*)>1 order by Count desc"%(column, column))

def find_unique(df, column):
    """
    Function to find unique/distinct values in a column of a DataFrame.

    Parameter
    ---------
    df     : DataFrame to be processed.
    column : column in df for which we want to find duplicate values.
    """
    assert_cols_in_df(df, columns_provided=[column], columns_df=df.columns)
    df.createOrReplaceTempView("df")
    return sqlContext.sql('select distinct(%s) as UniqueValues,COUNT(*) as Count from df group by %s order by Count desc'%(column, column))