from functools import lru_cache
from pyspark.sql import SparkSession

"""
This file contains methods to read, write, get sparksession etc.
that are not directly involved with data cleaning but enhance the process to great extent
"""

@lru_cache(maxsize=None)
def get_spark():
    """
    Get instance of spark
    """
    return SparkSession.builder \
        .master("local") \
        .appName("cleanflow") \
        .config("spark.some.config.option", "config-value") \
        .getOrCreate()


def read_csv(path, sep=',', header='true', infer_schema='true'):
    """
    Read csv file
    """
    session = get_spark()
    return session.read \
        .options(header=header) \
        .options(delimiter=sep) \
        .options(inferSchema=infer_schema) \
        .csv(path)


def read_json(path, multiLine=True):
    """
    Read json file
    """
    session = get_spark()
    return session.read.json(path, multiLine=multiLine)


def write_csv(df, path):
    """
    Write to a csv file
    """
    df.toPandas.to_csv(path)


def write_json(df, path):
    """
    Write to a json file
    """
    df.toPandas.to_json(path)

def totChanges(old, new):
    '''
    Function to calculate total modification done on performing an action
    '''
    count = 0
    
    for column in range(len(new)):
        indiv_old = old[column].toPandas().set_index('UniqueValues').T.to_dict('records')[0]
        indiv_new = new[column].toPandas().set_index('UniqueValues').T.to_dict('records')[0]
        
        for n in indiv_new:
            if n in indiv_old:
                count += abs(indiv_new[n]-indiv_old[n])
            else:
                count += indiv_new[n]
        return count