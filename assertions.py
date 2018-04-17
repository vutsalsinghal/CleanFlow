def assert_type_str_or_list(df, variable, name_arg):
    """This function asserts if variable is a string or a list dataType."""
    assert isinstance(variable, (str, list)), "Error: %s argument must be a string or a list." % name_arg

def assert_type_int_or_float(df, variable, name_arg):
    """This function asserts if variable is a string or a list dataType."""
    assert isinstance(variable, (int, float)), "Error: %s argument must be a int or a float." % name_arg

def assert_type_str(df, variable, name_arg):
    """This function asserts if variable is a string or a list dataType."""
    assert isinstance(variable, str), "Error: %s argument must be a string." % name_arg

def assert_cols_in_df(df, columns_provided, columns_df):
    """This function asserts if columns_provided exists in dataFrame.
    Inputs:
    columns_provided: the list of columns to be process.
    columns_df: list of columns's dataFrames
    """
    col_not_valids = (set([column for column in columns_provided]).difference(set([column for column in columns_df])))
    assert (col_not_valids == set()), 'Error: The following columns do not exits in dataFrame: %s' % col_not_valids