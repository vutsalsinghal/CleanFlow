<div align="center">
  <img src="https://pyofey.pythonanywhere.com/static/cf_logo.png"><br><br>
</div>

CleanFlow is a framework for cleaning, pre-processing and exploring data in a scalable and distributed manner. Being built on top of Apache Spark, it is highly scalable.

## Features
* Explore data
* Clean Data
* Get output in different formats

## Installation
Use of virtualenv is strongly advised!

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
Load data
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
 Import CleanFlow library
 ```python
 >>> from cleanflow.exploratory import describe
 >>> describe(df)
```
| summons_number | violation_code | violation_location | violation_precinct | issuer_code | issuer_precinct | vehicle_year |
| --- | --- | --- | --- | --- | --- | --- |
| count | 1.014017e+06 | 1.014017e+06 | 1.014017e+06 | 1.014017e+06 | 1.014017e+06 | 1.014017e+06 | 1.014012e+06 |
| mean | 6.546786e+09 | 3.499229e+01 | 4.359312e+01 | 4.378075e+01 | 3.376824e+05 | 4.563472e+01 | 1.567131e+03 |
| std | 2.116996e+09 | 1.865028e+01 | 4.028745e+01 | 4.008113e+01 | 2.243409e+05 | 5.968456e+01 | 8.323606e+02 |
| min | 1.017773e+09 | 1.000000e+00 | -1.000000e+00 | 0.000000e+00 | 0.000000e+00 | 0.000000e+00 | 0.000000e+00 |
| 25% | 5.090658e+09 | 2.000000e+01 | 7.000000e+00 | 7.000000e+00 | 3.451280e+05 | 5.000000e+00 | 1.998000e+03 |
| 50% | 7.497423e+09 | 3.600000e+01 | 3.000000e+01 | 3.000000e+01 | 3.573230e+05 | 2.400000e+01 | 2.007000e+03 |
| 75% | 8.038442e+09 | 4.000000e+01 | 7.700000e+01 | 7.700000e+01 | 3.621810e+05 | 7.700000e+01 | 2.013000e+03 |
| max | 8.297500e+09 | 9.900000e+01 | 9.670000e+02 | 9.670000e+02 | 9.998430e+05 | 9.920000e+02 | 2.069000e+03 |

```python
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
>>> from cleanflow.preprocessing import trim_col, rmSpChars
>>> rmSpChars(trim_col(df)).show(10)
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

>>> rmSpChars(trim_col(df), regex='[^A-Za-z0-9]+').show(10)
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
>>> import cleanflow.preprocessing as cfpr
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
