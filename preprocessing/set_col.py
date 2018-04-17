from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType

def set_col(df, columns, func, data_type):
    dict_types = {'string': StringType(), 'str': StringType(), 'integer': IntegerType(),'int': IntegerType(), 'float': FloatType(), 'double': DoubleType(), 'Double': DoubleType()}
    types = {'string': 'string', 'str': 'string', 'String': 'string', 'integer': 'int', 'int': 'int', 'float': 'float', 'double': 'double', 'Double': 'double'}
    
    function = udf(func, dict_types[data_type])
    new_df = [function(col(c)).alias(c) if c in columns else c for c in df.columns]
    return df.select(*new_df)