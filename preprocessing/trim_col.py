from cleanflow.assertions import *
from pyspark.sql.functions import trim, col

def trim_col(df, columns="*"):
    """
    This methods cut left and right extra spaces in column strings provided by user.

    Parameters
    ----------
    df      : Dataframe
    columns : list of column names of dataFrame.
    
    return dataframe
    """
    assert_type_str_or_list(df, columns, "columns")
    valid_cols = [c for (c, t) in filter(lambda t:t[1] == 'string', df.dtypes)]

    # If None or [] is provided with column parameter:
    if columns == '*':
        columns = valid_cols

    if isinstance(columns, str):
        columns = [columns]

    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)
    exprs = [trim(col(c)).alias(c) if (c in columns) and (c in valid_cols) else c for (c, t) in df.dtypes]
    return df.select(*exprs)