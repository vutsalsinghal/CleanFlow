def drop_col(df, columns):
    exprs = filter(lambda c: c not in columns, df.columns)
    return df.select(*exprs)