from cleanflow.assertions import *

def drop_col(df, columns):
    """
    Function to remove specific column(s)

    Parameters
    ----------
    df
    column
    """
    assert_type_str_or_list(df, columns, "columns")

    if isinstance(columns, str):
        columns = [columns]

    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)
    exprs = filter(lambda c: c not in columns, df.columns)
    return df.select(*exprs)