import string
from cleanflow.assertions import *
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def rmSpChars(df, columns="*"):
    """
    This function remove special chars in string columns, such as: .$%()!&"#/ etc
    
    Parameters
    ----------
    columns : list of columns names to be processed.
                columns argument can be a string or a list of strings."""

    assert_type_str_or_list(df, columns, "columns")
    valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', df.dtypes)]

    # If None or [] is provided with column parameter:
    if columns == "*":
        columns = valid_cols[:]

    if isinstance(columns, str):
        columns = [columns]

    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)
    col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

    assert (col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % col_not_valids

    def rm_Sp_Chars(inputStr):
        for punct in (set(inputStr) & set(string.punctuation)):
            inputStr = inputStr.replace(punct, "")
        return inputStr

    function = udf(lambda cell: rm_Sp_Chars(cell) if cell is not None else cell, StringType())
    exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols)  else c for c in df.columns]
    return df.select(*exprs)