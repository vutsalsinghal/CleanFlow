from pyspark.sql.functions import trim, col

def trim_col(df, columns):
    """This methods cut left and right extra spaces in column strings provided by user.
    :param columns   list of column names of dataFrame.
    :return dataframe
    """
    new_df = [trim(col(c)).alias(c) if c in columns else c for c in df.columns]
    return df.select(*new_df)