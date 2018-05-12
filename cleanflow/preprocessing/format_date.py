from cleanflow.assertions import assert_cols_in_df
from pyspark.sql.functions import date_format, unix_timestamp


def format_date(df, columns, current_format, output_format):
    """
    
    Parameter
    ---------
    df             :      dataframe whose date column has to be modified
    columns        :     Name date columns to be transformed.
    current_format :   current date string format eg: dd-MM-yyy
    output_format  :    output date string format to be expected.
    """
    assert isinstance(current_format, str), "Error, current_format argument provided must be a string."
    assert isinstance(output_format, str), "Error, output_format argument provided must be a string."
    assert isinstance(columns, (str, list)), "Error, columns argument provided must be a list."

    if isinstance(columns, str):
        columns = [columns]

    assert_cols_in_df(df, columns_provided=columns, columns_df=df.columns)

    expressions = [date_format(unix_timestamp(c, current_format).cast("timestamp"), output_format).alias(c) if c in columns else c for c in df.columns]
    return df.select(*expressions)
