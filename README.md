# CleanFlow

CleanFlow is a framework for cleaning, pre-processing and exploring data in a scalable and distributed manner. Being built on top of Apache Spark, it is highly scalable.


## Usage

Start Pyspark session
```
$ pyspark
```
Import data
```
>>> df = sqlContext.read.format('csv').options(header='true',inferschema='true').load('parking-violations-header.csv')
>>> type(df)

pyspark.sql.dataframe.DataFrame

>>> file.printSchema()

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
 ```
 Import CleanFlow library
 ```
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

```
>>> df = df.select('summons_number', 'violation_code', 'violation_county', 'plate_type', 'vehicle_year')
>>> df.show()

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
|    1359423576|            40|              BX|       PAS|        2001|
|    1358746333|            85|              BX|       PAS|           0|
|    1361067974|            40|               K|       PAS|           0|
|    1362335939|            40|              NY|       PAS|           0|
|    1362902056|            21|              BX|       PAS|        2003|
|    1362963860|            46|               K|       PAS|        2013|
|    1365462523|            78|              NY|       COM|        2003|
|    1366567511|            19|              NY|       COM|        2007|
|    1366720556|            46|              BX|       COM|        2008|
|    1367171477|            45|              NY|       COM|        1990|
+--------------+--------------+----------------+----------+------------+
only showing top 20 rows
```

Import functions

```
>>> from cleanflow.preprocessing import trim_col, rmSpChars
>>> rmSpChars(trim_col(df)).show()

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
|    1359423576|            40|              BX|       PAS|        2001|
|    1358746333|            85|              BX|       PAS|           0|
|    1361067974|            40|               K|       PAS|           0|
|    1362335939|            40|              NY|       PAS|           0|
|    1362902056|            21|              BX|       PAS|        2003|
|    1362963860|            46|               K|       PAS|        2013|
|    1365462523|            78|              NY|       COM|        2003|
|    1366567511|            19|              NY|       COM|        2007|
|    1366720556|            46|              BX|       COM|        2008|
|    1367171477|            45|              NY|       COM|        1990|
+--------------+--------------+----------------+----------+------------+
only showing top 20 rows
```
