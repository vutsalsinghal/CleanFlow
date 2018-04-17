from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

def check_duplicates(df, column, show=False):
    df.createOrReplaceTempView("df")
    if show:
        return sqlContext.sql("SELECT %s as DuplicateValue, COUNT(*) as Count FROM df GROUP BY %s HAVING COUNT(*)>1 order by Count desc"%(column, column))
    return sqlContext.sql('select count(%s) as TotalRows, count(distinct(%s)) as UniqueRows from df'%(column, column))

def find_unique(df, column):
    df.createOrReplaceTempView("df")
    return sqlContext.sql('select distinct(%s) as UniqueValues,COUNT(*) as Count from df group by %s order by Count desc'%(column, column))