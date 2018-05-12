from .set_col import set_col

def upper_case(df, columns="*", summary=False):
    func = lambda cell: cell.upper() if cell is not None else cell

    if summary:
        return set_col(df, columns, func, 'string', True)
    return set_col(df, columns, func, 'string', False)