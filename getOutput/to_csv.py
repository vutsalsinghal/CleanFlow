def to_csv(df, path_name, singleFile=False, header=False, mode="overwrite", separator=",", *args, **kargs):
	"""
	Write dataframe to disk in CSV format.
	
	Parameters
	----------
	dataframe: The dataframe to be converted to CSV
	path_name: Path to write the DF and the name of the output CSV file.
	header   : True or False to include header
	separator: sets the single character as a separator for each field and value. If None is set,
		it uses the default value.

	if singleFile = False:
	
		# pyspark.sql.DataFrameWriter class is used! 		
		# (refer https://spark.apache.org/docs/latest/api/python/ \
				pyspark.sql.html?highlight=dataframe#pyspark.sql.DataFrameWriter.csv)
		
		mode : Specifies the behavior of the save operation when data already exists.
				"append": Append contents of this DataFrame to existing data.
				"overwrite" (default case): Overwrite existing data.
				"ignore": Silently ignore this operation if data already exists.
				"error": Throw an exception if data already exists.
	
	else: 

		# pandas.DataFrame.to_csv class is used								
		# (refer https://pandas.pydata.org/pandas-docs/stable/generated/ \
				pandas.DataFrame.to_csv.html)

		mode : str - Python write mode, default ‘w’
	
	
	return   : Dataframe in a CSV format in the specified path.
		"""

	assert isinstance(path_name, str), "Error: path_name argument must be a string."
	#assert header == "true" or header == "false", "Error header must be 'true' or 'false'."
	if singleFile:
		return df.toPandas().to_csv(path_name, header=True, *args, **kargs)
	return df.write.options(header=header).mode(mode).csv(path_name, sep=separator, *args, **kargs)