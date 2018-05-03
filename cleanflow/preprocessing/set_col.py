from cleanflow.assertions import *
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType

def set_col(df, columns, func, data_type):
    dict_types = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}
    types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int', 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}
    
    funct = udf(func, dict_types[data_type])
    assert_type_str_or_list(df, columns, "columns")
    valid_cols = [c for (c, t) in filter(lambda t:t[1]==types[data_type], df.dtypes)]

    if columns == "*":
        columns = valid_cols[:]

    if isinstance(columns, str):
        columns = [columns]

    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)

    col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

    assert (col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % col_not_valids
    exprs = [funct(col(c)).alias(c) if c in columns else c for (c, t) in df.dtypes]

    df = df.select(*exprs)
    return df