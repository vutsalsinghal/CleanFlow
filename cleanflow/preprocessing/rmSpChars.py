import re
import string
from cleanflow.assertions import assert_type_str_or_list, assert_cols_in_df
from cleanflow.utils import totChanges
from cleanflow.exploratory import find_unique
from pyspark.sql.functions import trim, col
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

def rmSpChars(df, columns="*", regex=None, summary=False):
    """
    This function remove special characters in string columns, such as: .$%()!&"#/
    You can also remove unwanted sub-string by specifying the regex in "regex" parameter!
    
    Parameters
    ----------
    df      : Dataframe to be processed
    columns : (optional - default *)list of names columns to be processed.
                argument can be a string or a list of strings.
    regex   : (optional - default None) string that contains the regular expression
    
    return df
    """
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

    # Columns that are present in user_input and valid_columns
    col_pool = [c for c in columns if c in valid_cols]

    def rm_Sp_Chars(inputStr, regex):
        if regex is None:
            for punct in (set(inputStr) & set(string.punctuation)):
                inputStr = inputStr.replace(punct, "") 
        else:
            for _ in set(inputStr):
                inputStr = re.sub(regex, '', inputStr)
        return inputStr

    function = udf(lambda cell: rm_Sp_Chars(cell, regex) if cell is not None else cell, StringType())
    
    oldUnique = [find_unique(df, column=c) for c in col_pool]
    
    exprs = [function(c).alias(c) if c in col_pool else c for c in df.columns]
    newDF = df.select(*exprs)

    if summary:
        newUnique = [find_unique(newDF, column=c) for c in col_pool]
        count = int(totChanges(oldUnique, newUnique))
        summary = sqlContext.createDataFrame([(count,)],['Total Cells Modified',])
        return (newDF, summary)
    return newDF