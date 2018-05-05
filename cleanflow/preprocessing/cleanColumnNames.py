def cleanColumnNames(df):
    '''
    Remove special characters such as !"#$%\'*,./:;<=>?@[\\]^`{|}~ from column names

    Parameter
    ---------
    df : data frame

    return df
    '''
    spChars = '!"#$%\'*,./:;<=>?@[\\]^`{|}~'
    for column in df.columns:
        oldName = column
        for punct in (set(column) & set(spChars)):
            column = column.replace(punct, "")
        df = df.withColumnRenamed(oldName,column)
    return df