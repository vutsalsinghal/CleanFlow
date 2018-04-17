from pyspark.ml.feature import Imputer
from cleanflow.assertions import *

def impute_missing(df, columns, out_cols, strategy='mean'):
    """
    Imputes missing data from specified columns using the mean or median.

    Parameters
    ----------
    columns : List of columns to be analyze.
    out_cols: List of output columns with missing values imputed.
    strategy: String that specifies the way of computing missing data. Can be "mean" or "median"
    
    return  : Transformer object (DF with columns that has the imputed values).
    """

    # Check if columns to be process are in dataframe
    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)

    assert isinstance(columns, list), "Error: columns argument must be a list"
    assert isinstance(out_cols, list), "Error: out_cols argument must be a list"

    # Check if columns argument a string datatype:
    assert_type_str(df, strategy, "strategy")

    assert (strategy == "mean" or strategy == "median"), "Error: strategy has to be 'mean' or 'median'. 'mean' is default"

    imputer = Imputer(inputCols=columns, outputCols=out_cols)
    model = imputer.setStrategy(strategy).fit(df)
    df = model.transform(df)

    return df