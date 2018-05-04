import string

def cleanColumnNames(df):
    '''
    Remove special characters such as !"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ from column names

    Parameter
    ---------
    df : data frame

    return df
    '''
    for column in df.columns:
        oldName = column
        for punct in (set(column) & set(string.punctuation)):
            column = column.replace(punct, "")
        df = df.withColumnRenamed(oldName,column)
    return df