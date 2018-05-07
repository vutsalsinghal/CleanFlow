<div align="center">
    <img src="https://pyofey.pythonanywhere.com/static/cf_logo_compressed_scaled.png"><br><br>
 </div>

CleanFlow is a framework for cleaning, pre-processing and exploring data in a scalable and distributed manner. Being built on top of Apache Spark, it is highly scalable.

## Features
* Explore data
* Clean Data
* Get output in different formats

## Installation
`pip install CleanFlow`

## Sample usage

Start Pyspark session
```
$ pyspark

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.2.1
      /_/

Using Python version 3.5.2 (default, Nov 23 2017 16:37:01)
SparkSession available as 'spark'.
```
## Load data
```python
# DataFrame (df)
>>> df = sqlContext.read.format('csv').options(header='true',inferschema='true').load('sample.csv')
>>> type(df)

pyspark.sql.dataframe.DataFrame

>>> df.printSchema()
'''
root
 |-- summons_number: long (nullable = true)
 |-- issue_date: timestamp (nullable = true)
 |-- violation_code: integer (nullable = true)
 |-- violation_county: string (nullable = true)
 |-- violation_description: string (nullable = true)
 |-- violation_location: integer (nullable = true)
 |-- violation_precinct: integer (nullable = true)
 |-- violation_time: string (nullable = true)
 |-- time_first_observed: string (nullable = true)
 |-- meter_number: string (nullable = true)
 |-- issuer_code: integer (nullable = true)
 |-- issuer_command: string (nullable = true)
 |-- issuer_precinct: integer (nullable = true)
 |-- issuing_agency: string (nullable = true)
 |-- plate_id: string (nullable = true)
 |-- plate_type: string (nullable = true)
 |-- registration_state: string (nullable = true)
 |-- street_name: string (nullable = true)
 |-- vehicle_body_type: string (nullable = true)
 |-- vehicle_color: string (nullable = true)
 |-- vehicle_make: string (nullable = true)
 |-- vehicle_year: string (nullable = true)
 '''
 ```
 ## Explore Data
 ```python
 >>> from cleanflow.exploratory import describe
 >>> describe(df)
```
|       | summons_number | violation_code | violation_location | violation_precinct | issuer_code   | issuer_precinct |
|-------|----------------|----------------|--------------------|--------------------|---------------|-----------------|
| count | 1.000000e+05   | 100000.000000  | 100000.000000      | 100000.000000      | 100000.000000 | 100000.000000   |
| mean  | 6.046625e+09   | 36.468890      | 66.483920          | 66.483980          | 445461.251460 | 66.704720       |
| std   | 2.384666e+09   | 19.455201      | 34.810481          | 34.810365          | 199702.620407 | 71.608545       |
| min   | 1.119098e+09   | 1.000000       | -1.000000          | 0.000000           | 0.000000      | 0.000000        |
| 25%   | 7.014648e+09   | 21.000000      | 43.000000          | 43.000000          | 355455.000000 | 30.000000       |
| 50%   | 7.100271e+09   | 37.000000      | 66.000000          | 66.000000          | 361282.000000 | 62.000000       |
| 75%   | 7.451945e+09   | 41.000000      | 103.000000         | 103.000000         | 363040.000000 | 104.000000      |
| max   | 7.698377e+09   | 99.000000      | 803.000000         | 803.000000         | 999843.000000 | 992.000000      |
```python

>>> # Choose a subset of data
>>> df = df.select('summons_number', 'violation_code', 'violation_county', 'plate_type', 'vehicle_year')
>>> df.show(10)
'''
+--------------+--------------+----------------+----------+------------+
|summons_number|violation_code|violation_county|plate_type|vehicle_year|
+--------------+--------------+----------------+----------+------------+
|    1307964308|            14|            NY! |       PAS|        2008|
|    1362655727|            98|            BX$ |       PAS|        1999|
|    1363178234|            21|            NY$ |       COM|           0|
|    1365797030|            74|            K$! |       PAS|        1999|
|    1366529595|            38|             NY |       COM|        2005|
|    1366571757|            20|              NY|       COM|        2013|
|    1363178192|            21|              NY|       PAS|           0|
|    1362906062|            21|              BX|       PAS|        2008|
|    1367591351|            40|               K|       PAS|        2005|
|    1354042244|            20|              NY|       COM|           0|
+--------------+--------------+----------------+----------+------------+
'''
only showing top 10 rows
```

```python
>>> from cleanflow.exploratory import check_duplicates, find_unique
>>> check_duplicates(df, column='violation_county').show()
'''
+--------------+-----+
|DuplicateValue|Count|
+--------------+-----+
|             K|29123|
|             Q|25961|
|            BX|21203|
|            NY|20513|
|             R| 2728|
|          null|  467|
+--------------+-----+
'''
>>> find_unique(df, column='violation_county').show()
'''
+------------+-----+                                                            
|UniqueValues|Count|
+------------+-----+
|           K|29123|
|           Q|25961|
|          BX|21203|
|          NY|20513|
|           R| 2728|
|        null|  467|
|         NY |    1|
|        NY$ |    1|
|        K$! |    1|
|        BX$ |    1|
|        NY! |    1|
+------------+-----+
'''
```
## Clean data
```python
>>> import cleanflow.preprocessing as cfpr
>>> cfpr.rmSpChars(cfpr.trim_col(df)).show(10)
'''
+--------------+--------------+----------------+----------+------------+
|summons_number|violation_code|violation_county|plate_type|vehicle_year|
+--------------+--------------+----------------+----------+------------+
|    1307964308|            14|              NY|       PAS|        2008|
|    1362655727|            98|              BX|       PAS|        1999|
|    1363178234|            21|              NY|       COM|           0|
|    1365797030|            74|               K|       PAS|        1999|
|    1366529595|            38|              NY|       COM|        2005|
|    1366571757|            20|              NY|       COM|        2013|
|    1363178192|            21|              NY|       PAS|           0|
|    1362906062|            21|              BX|       PAS|        2008|
|    1367591351|            40|               K|       PAS|        2005|
|    1354042244|            20|              NY|       COM|           0|
+--------------+--------------+----------------+----------+------------+
'''
only showing top 10 rows

>>> cfpr.rmSpChars(trim_col(df), regex='[^A-Za-z0-9]+').show(10)
'''
+--------------+--------------+----------------+----------+------------+
|summons_number|violation_code|violation_county|plate_type|vehicle_year|
+--------------+--------------+----------------+----------+------------+
|    1307964308|            14|              NY|       PAS|        2008|
|    1362655727|            98|              BX|       PAS|        1999|
|    1363178234|            21|              NY|       COM|           0|
|    1365797030|            74|               K|       PAS|        1999|
|    1366529595|            38|              NY|       COM|        2005|
|    1366571757|            20|              NY|       COM|        2013|
|    1363178192|            21|              NY|       PAS|           0|
|    1362906062|            21|              BX|       PAS|        2008|
|    1367591351|            40|               K|       PAS|        2005|
|    1354042244|            20|              NY|       COM|           0|
+--------------+--------------+----------------+----------+------------+
'''
only showing top 10 rows
```

```python
>>> cfpr.upper_case(cfpr.lower_case(df), columns='violation_county').show(10)
'''
+--------------+--------------+----------------+----------+------------+
|summons_number|violation_code|violation_county|plate_type|vehicle_year|
+--------------+--------------+----------------+----------+------------+
|    1307964308|            14|            NY! |       pas|        2008|
|    1362655727|            98|            BX$ |       pas|        1999|
|    1363178234|            21|            NY$ |       com|           0|
|    1365797030|            74|            K$! |       pas|        1999|
|    1366529595|            38|             NY |       com|        2005|
|    1366571757|            20|              NY|       com|        2013|
|    1363178192|            21|              NY|       pas|           0|
|    1362906062|            21|              BX|       pas|        2008|
|    1367591351|            40|               K|       pas|        2005|
|    1354042244|            20|              NY|       com|           0|
+--------------+--------------+----------------+----------+------------+
'''
only showing top 10 rows
```

