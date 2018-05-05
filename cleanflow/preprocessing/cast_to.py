from cleanflow.assertions import *
from cleanflow.preprocessing.drop_null import *
from pyspark.sql.types import *

def cast_to_int(df, columns):
    '''
    Convert a column type to integer, drop columns that are not convertible

    Parameters
    ----------
    df      : dataframe
    columns : columns to be casted

    reutrn df
    '''
    assert_type_str_or_list(df, columns, "columns")
    if type(columns) is str:
        df = df.withColumn(columns, df[columns].cast(IntegerType()))
        return drop_null(df, columns)
    else:
        for column in columns:
            df = df.withColumn(column, df[column].cast(IntegerType()))
        return drop_null(df, column)

def cast_to_double(df, columns):
    '''
    Convert a column type to double, drop columns that are not convertible

    Parameters
    ----------
    df      : dataframe
    columns : columns to be casted

    reutrn df
    '''
    assert_type_str_or_list(df, columns, "columns")
    
    if type(columns) is str:
        df = df.withColumn(columns, df[columns].cast(DoubleType()))
        return drop_null(df, columns)
    else:
        for column in columns:
            df = df.withColumn(column, df[column].cast(DoubleType()))
        return drop_null(df, column)

def cast_to_string(df, columns):
    '''
    Convert a column type to string, drop columns that are not convertible
    
    Parameters
    ----------
    df      : dataframe
    columns : columns to be casted

    reutrn df
    '''
    assert_type_str_or_list(df, columns, "columns")

    if type(columns) is str:
        df = df.withColumn(columns, df[columns].cast(StringType()))
        return drop_null(df, columns)
    else:
        for column in columns:
            df = df.withColumn(column, df[column].cast(StringType()))
        return drop_null(df, column)