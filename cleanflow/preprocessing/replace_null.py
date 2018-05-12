def replace_null(df, value, columns="*"):
    """
    Replace nulls with specified value.
    
    Parameters
    ----------
    columns   : optional list of column names to consider. Columns specified in subset that do not have
                matching data type are ignored. For example, if value is a string, and subset contains a non-string column,
                then the non-string column is simply ignored.
    value     : Value to replace null values with. If the value is a dict, then subset is ignored and value
                must be a mapping from column name (string) to replacement value. The replacement
                value must be an int, long, float, or string.
    
    return df : dataframe with replaced null values.
    """
    if columns == "*":
        columns = None

    if isinstance(columns, str):
        columns = [columns]

    if columns is not None:
        assert isinstance(columns, list), "Error: columns argument must be a list"

    assert isinstance(value, (int, float, str, dict)), "Error: value argument must be an int, long, float, string, or dict"
    return df.fillna(value, subset=columns)