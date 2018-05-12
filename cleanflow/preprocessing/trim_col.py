from cleanflow.assertions import assert_type_str_or_list, assert_cols_in_df
from cleanflow.utils import totChanges
from cleanflow.exploratory import find_unique
from pyspark.sql.functions import trim, col
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

def trim_col(df, columns="*", summary=False):
    """
    This methods removes left and right extra spaces from StringType column(s).

    Parameters
    ----------
    df      : Dataframe
    columns : list of column names of dataFrame.
    
    return modifiedDF
    """
    assert_type_str_or_list(df, columns, "columns")
    valid_cols = [c for (c, t) in filter(lambda t:t[1] == 'string', df.dtypes)]

    # If None or [] is provided with column parameter:
    if columns == '*':
        columns = valid_cols

    if isinstance(columns, str):
        columns = [columns]

    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)
    
    # Columns that are present in user_input and valid_columns
    col_pool = [c for c in columns if c in valid_cols]
    
    oldUnique = [find_unique(df, column=c) for c in col_pool]
    exprs = [trim(col(c)).alias(c) if c in col_pool else c for (c, t) in df.dtypes]
    newDF = df.select(*exprs)

    if summary:
        newUnique = [find_unique(newDF, column=c) for c in col_pool]
        count = int(totChanges(oldUnique, newUnique))
        summary = sqlContext.createDataFrame([(count,)],['Total Cells Trimmed',])
        return (newDF, summary)
    return newDF