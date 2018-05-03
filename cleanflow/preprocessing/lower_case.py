from .set_col import set_col

def lower_case(df, columns="*"):
    func = lambda cell: cell.lower() if cell is not None else cell
    df = set_col(df, columns, func, 'string')
    return df