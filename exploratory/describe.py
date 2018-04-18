def describe(df):
	'''
	Function to find count, mean, std, min, max of all integer fields of a DataFrame

	Parameters
	----------
	df     : Data frame to be described

	return : Description of DataFrame
	'''
	return df.toPandas().describe()