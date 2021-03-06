def remove_duplicates(df, columns=None):
	"""
	Remove duplicate values from specified columns.

	Parameters
	----------
	cols  : List of columns to make the comparison, this only  will consider this subset of columns,
	for dropping duplicates. The default behavior will only drop the identical rows.
	
	return: Return a new DataFrame with duplicate rows removed
	"""

	assert isinstance(cols, list), "Error, cols argument provided must be a list."
	newDF = df.drop_duplicates(columns)
	return newDF