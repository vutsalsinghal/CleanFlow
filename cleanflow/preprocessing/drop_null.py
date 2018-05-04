from pyspark.sql.functions import col

def drop_null(df, column):
    '''
    Drop rows that have null value in the given row

    Parameter
    ---------
    df     : dataframe
    column : column to be processed

    return df
    '''
    return df.where(col(column).isNotNull())