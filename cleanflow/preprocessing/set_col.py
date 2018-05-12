from cleanflow.assertions import *
from cleanflow.utils import totChanges
from cleanflow.exploratory import find_unique
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

def set_col(df, columns, func, data_type, summary):
    dict_types = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}
    types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int', 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}
    
    try:
        function = udf(func, dict_types[data_type])
    except KeyError:
        assert False, "Error, data_type not recognized"

    assert_type_str_or_list(df, columns, "columns")

    # Filters all string columns in dataFrame
    valid_cols = [c for (c, t) in filter(lambda t:t[1]==types[data_type], df.dtypes)]

    if columns == "*":
        columns = valid_cols[:]

    if isinstance(columns, str):
        columns = [columns]

    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)
    col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))
    assert (col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % col_not_valids
    
    oldUnique = [find_unique(df, column=c) for c in columns]
    exprs = [function(col(c)).alias(c) if c in columns else c for (c, t) in df.dtypes]
    newDF = df.select(*exprs)

    if summary:
        newUnique = [find_unique(newDF, column=c) for c in columns]
        count = int(totChanges(oldUnique, newUnique))
        summary = sqlContext.createDataFrame([(count,)],['Total Cells Modified',])
        return (newDF, summary)
    return newDF