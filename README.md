# Big Data Project of Taxi Trip in 2019 (Chicago)
in this repository will be used for the big data trainee program

# 1.About the Dataset 
Taxi trips reported to the City of Chicago in its role as a regulatory agency. 
Each Row is a trip
To protect privacy but allow for aggregate analyses, the Taxi ID is consistent for any given taxi 
medallion number but does not show the number, Census Tracts are suppressed in some cases, and times are rounded to the nearest 15 minutes.

###  Columns and Description

|Columns|Description|
|-------|-----------|
|Trip ID|A unique identifier for the trip.|
|Taxi ID|A unique identifier for the taxi.|
|Trip Start Timestamp|When the trip started, rounded to the nearest 15 minutes.|
|Trip End Timestamp|When the trip ended, rounded to the nearest 15 minutes.|
|Trip Seconds|Time of the trip in seconds.|
|Trip Miles|Distance of the trip in miles.|
|Pickup Census Tract|The Census Tract where the trip began. For privacy, this Census Tract is not shown for some trips. This column often will be blank for locations outside Chicago.|
|Dropoff Census Tract|The Census Tract where the trip ended. For privacy, this Census Tract is not shown for some trips. This column often will be blank for locations outside Chicago.|
|Pickup Community Area|The Community Area where the trip began. This column will be blank for locations outside Chicago.|
|Dropoff Community Area|The Community Area where the trip ended. This column will be blank for locations outside Chicago.|
|Fare|The fare for the trip.|
|Tips|The tip for the trip. Cash tips generally will not be recorded|
|Tolls|The tolls for the trip.
|Extras|Extra charges for the trip.|
|Trip Total|Total cost of the trip, the total of the previous columns.|
|Payment Type|Type of payment for the trip.|
|Company|The taxi company.|
|Pickup Centroid Latitude|The latitude of the center of the pickup census tract or the community area if the census tract has been hidden for privacy. This column often will be blank for locations outside Chicago.|
|Pickup Centroid Longitude|The longitude of the center of the pickup census tract or the community area if the census tract has been hidden for privacy. This column often will be blank for locations outside Chicago.|
|Pickup Centroid Location|The location of the center of the pickup census tract or the community area if the census tract has been hidden for privacy. This column often will be blank for locations outside Chicago.|
|Dropoff Centroid Longitude|The longitude of the center of the dropoff census tract or the community area if the census tract has been hidden for privacy. This column often will be blank for locations outside Chicago.|
|Dropoff Centroid Location|The location of the center of the dropoff census tract or the community area if the census tract has been hidden for privacy. This column often will be blank for locations outside Chicago.|

###  DATASET'S EDA 
EDA (Exploratory Data Analysis) is the stepping stone of Data Science, and the process involves 
investigating data and discovering underlying patterns in data. The EDA for this project is resolve 
with this code :
It receives a Dataframe
Obtain data to make some calculations for our firs stats in relation with the column Total Trip

``` SCALA
  println("There is " + tripsDF.count() + " Rows in the dataset")
  println("There is " + tripsDF.distinct().count() + "Unique Row in the dataset")
  println("there is "+ tripsDF.columns.size + " Number of Columns")
    val stas_df = tripsDF.select(
    mean("Trip Total").as("mean"),
    stddev("Trip Total").as("Stddev"),
    min("Trip Total").as("min"),
    max("Trip Total").as("max")
  )
  stas_df.show()
  
  
  +------------------+-----------------+---+-------+
|              mean|           Stddev|min|    max|
+------------------+-----------------+---+-------+
|18.176876191216515|60.90924601547143|0.0|9900.54|
+------------------+-----------------+---+-------+
  


```
Results:

* The number of rows analyzed were 16,477,365 rows and 23 columns.
* The number of unique Rows were 16,477,365
* we do not have duplicate Rows
* The mean of Total Trip were 18.1
* The Stddev of Total Trip were 60.1
* the min of Total Trip were 0
* the max of Total Trip were 9900.54


After that firs Analysis , we need to know the number of null values in columns
For this  exploratory analysis was made with a function that summarize numbers  of nulls by column in a given Dataframe. 
The function works as follows:


``` SCALA
}

def countColsNulls(columns: Array[String]): Array[Column] = {
  columns.map(c => {
    count(when(col(c).isNull, c)).alias(c)
  })
}

val cantidad_columnas = tripsDF.select(countCols(tripsDF.columns): _*)
val cantidad_Nulls = tripsDF.select(countColsNulls(tripsDF.columns): _*)
val Quantities = cantidad_columnas.union(cantidad_Nulls)

Quantities.show()

+--------+--------+--------------------+------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+--------+--------+--------+--------+----------+------------+--------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+
| Trip ID| Taxi ID|Trip Start Timestamp|Trip End Timestamp|Trip Seconds|Trip Miles|Pickup Census Tract|Dropoff Census Tract|Pickup Community Area|Dropoff Community Area|    Fare|    Tips|   Tolls|  Extras|Trip Total|Payment Type| Company|Pickup Centroid Latitude|Pickup Centroid Longitude|Pickup Centroid Location|Dropoff Centroid Latitude|Dropoff Centroid Longitude|Dropoff Centroid  Location|
+--------+--------+--------------------+------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+--------+--------+--------+--------+----------+------------+--------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+
|16477365|16477365|            16477365|          16476820|    16474629|  16476815|           10881829|            10784224|             15239379|              14818716|16475794|16475794|16217353|16475794|  16475794|    16477365|16477365|                15241413|                 15241413|                15241413|                 14877765|                  14877765|                  14877765|
|       0|       0|                   0|               545|        2736|       550|            5595536|             5693141|              1237986|               1658649|    1571|    1571|  260012|    1571|      1571|           0|       0|                 1235952|                  1235952|                 1235952|                  1599600|                   1599600|                   1599600|
+--------+--------+--------------------+------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+--------+--------+--------+--------+----------+------------+--------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+

```
Results 
* As a result we have a table that in first rows has the number of register and in the second
it has the count of the nulls
* most nulls are found in the columns that provide us with location coordinatesx|
### How to execute the solution 

### Data Preparation
To be able to work with the dataframe that we have obtained when reading our file that contains the data set, it is necessary to manipulate the following things:
* Data Types
* The characters inside the data

``` SCALA
val tripsdf2 = tripsDF
       .withColumn("Trip Miles",col("Trip Miles").cast(DoubleType))
       .withColumn("Trip Start Timestamp",unix_timestamp(col("Trip Start Timestamp"),"MM/dd/yyyy hh:mm:ss a").cast(TimestampType))
       .withColumn("Trip End Timestamp",unix_timestamp(col("Trip End Timestamp"),"MM/dd/yyyy hh:mm:ss a").cast(TimestampType))
       .withColumn("Trip Seconds",regexp_replace(col("Trip Seconds"), ",", ""))
       .withColumn("Trip Seconds",col("Trip Seconds").cast(IntegerType))

    tripsdf2.show()
    tripsdf2.printSchema()
    
    +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+-----+----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+
|             Trip ID|             Taxi ID|Trip Start Timestamp| Trip End Timestamp|Trip Seconds|Trip Miles|Pickup Census Tract|Dropoff Census Tract|Pickup Community Area|Dropoff Community Area| Fare|Tips|Tolls|Extras|Trip Total|Payment Type|             Company|Pickup Centroid Latitude|Pickup Centroid Longitude|Pickup Centroid Location|Dropoff Centroid Latitude|Dropoff Centroid Longitude|Dropoff Centroid  Location|
+--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+-----+----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+
|f24ef1302cd672bbc...|1801165e22d31b0d2...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         885|      3.45|               null|                null|                 null|                  null|12.75| 0.0|  0.0|   4.0|     16.75|        Cash|           Flash Cab|                    null|                     null|                    null|                     null|                      null|                      null|
|8e40b9c59af7b2897...|7a387c4ea03aea6f0...| 2019-01-01 00:00:00|2019-01-01 00:00:00|           8|       0.0|               null|                null|                 null|                  null| 3.25| 0.0|  0.0|   0.0|      3.25|        Cash|Chicago Carriage ...|                    null|                     null|                    null|                     null|                      null|                      null|
|189631d3891a33361...|d1d781b6807176ed5...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         960|       4.9|               null|                null|                 null|                  null|15.75|3.35|  0.0|   1.0|      20.1| Credit Card|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|
|e4941ac16f2fccf07...|6933327f9cc740e89...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         420|       1.5|               null|                null|                 null|                  null|  7.5| 0.0|  0.0|   0.0|       7.5|        Cash|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|
|57a104b6cf260f117...|6933327f9cc740e89...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         420|       0.5|               null|                null|                 null|                  null|  6.0| 1.0|  0.0|   1.0|       8.0| Credit Card|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|
|15926814373db081e...|d1d781b6807176ed5...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         180|       0.4|               null|                null|                 null|                  null| 4.25| 0.0|  0.0|   1.5|      5.75|        Cash|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|
|dbd155d142186f230...|fe08515b1e0b74cf1...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         360|       1.7|               null|                null|                 null|                  null|  6.8| 0.0|  0.0|   0.0|       6.8|        Cash|Taxi Affiliation ...|                    null|                     null|                    null|                     null|                      null|                      null|
|13196feb30c5c7172...|1091602ce46fc5b0a...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         300|       0.0|               null|                null|                 null|                  null|  5.8| 0.0|  0.0|   0.0|       5.8|        Cash|Taxi Affiliation ...|                    null|                     null|                    null|                     null|                      null|                      null|
|216a8d3bce9133a4e...|2e3d5a644579e645b...| 2019-01-01 00:00:00|2019-01-01 00:00:00|          60|       0.0|               null|                null|                 null|                  null|100.0| 0.0|  0.0|   0.0|     100.0| Credit Card|Taxi Affiliation ...|                    null|                     null|                    null|                     null|                      null|                      null|
|989984b01df0f0499...|ef32ded438e4b619e...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         840|       0.0|               null|                null|                 null|                  null|11.25| 5.0|  0.0|   0.0|     16.25| Credit Card|Blue Ribbon Taxi ...|                    null|                     null|                    null|                     null|                      null|                      null|
|6a167cec0bbeda22f...|2d617081d9c9a0014...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         780|       4.5|               null|                null|                 null|                  null|14.25| 0.0|  0.0|   0.0|     14.25|        Cash|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|
|6d66c3f140bb473de...|57dbcc9d59c073023...| 2019-01-01 00:00:00|2019-01-01 00:15:00|        1020|       0.1|               null|                null|                 null|                  null| 11.0| 0.0|  0.0|   3.0|      14.0|        Cash|Blue Ribbon Taxi ...|                    null|                     null|                    null|                     null|                      null|                      null|
|d140294626dc7df7b...|acf55d2e2793f815d...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         720|       0.0|               null|                null|                 null|                  null|  9.6| 0.0|  0.0|   0.0|       9.6|        Cash|Taxi Affiliation ...|                    null|                     null|                    null|                     null|                      null|                      null|
|78b1fe0ebda36f524...|11f73b08790612efe...| 2019-01-01 00:00:00|2019-01-01 00:30:00|        1680|       3.0|               null|                null|                 null|                  null| 14.5| 0.0|  0.0|   1.0|      15.5|        Cash|Choice Taxi Assoc...|                    null|                     null|                    null|                     null|                      null|                      null|
|11ea223ade60b882b...|13805c930c62981c6...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         780|       0.4|               null|                null|                 null|                  null| 7.75| 0.0|  0.0|   1.0|      8.75|        Cash| Top Cab Affiliation|                    null|                     null|                    null|                     null|                      null|                      null|
|3de7b95339c8eb885...|ef32ded438e4b619e...| 2019-01-01 00:00:00|2019-01-01 00:00:00|           0|       0.0|               null|                null|                 null|                  null| 3.25| 0.0|  0.0|   0.0|      3.25|        Cash|Blue Ribbon Taxi ...|                    null|                     null|                    null|                     null|                      null|                      null|
|e066e483f0fc2a583...|4794c36af4c1852d9...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         600|       0.0|        17031081402|         17031839100|                    8|                    32|  9.0| 3.0|  0.0|   1.0|      13.0| Credit Card|Blue Ribbon Taxi ...|            41.891971508|            -87.612945414|    POINT (-87.612945...|             41.880994471|             -87.632746489|      POINT (-87.632746...|
|83634d41b77f6d074...|1e3a48e9cf98c6228...| 2019-01-01 00:00:00|2019-01-01 00:15:00|        1200|       6.7|               null|                null|                 null|                  null|19.75| 0.0|  0.0|   0.0|     19.75|        Cash|Chicago Independents|                    null|                     null|                    null|                     null|                      null|                      null|
|6aa8db9c31843dcb3...|85c39e068db414d18...| 2019-01-01 00:00:00|2019-01-01 00:30:00|        1260|       0.6|               null|                null|                   77|                    28| 29.5| 0.0|  0.0|   0.0|      29.5|        Cash|Blue Ribbon Taxi ...|              41.9867118|            -87.663416405|    POINT (-87.663416...|             41.874005383|              -87.66351755|      POINT (-87.663517...|
|5431595d222a3e00a...|86b0677bb9bcda045...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         120|       0.3|        17031839100|         17031320400|                   32|                    32|  4.0| 1.0|  0.0|   1.0|       6.0| Credit Card|Star North Manage...|            41.880994471|            -87.632746489|    POINT (-87.632746...|             41.877406123|             -87.621971652|      POINT (-87.621971...|
+--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+-----+----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+
only showing top 20 rows

root
 |-- Trip ID: string (nullable = true)
 |-- Taxi ID: string (nullable = true)
 |-- Trip Start Timestamp: timestamp (nullable = true)
 |-- Trip End Timestamp: timestamp (nullable = true)
 |-- Trip Seconds: integer (nullable = true)
 |-- Trip Miles: double (nullable = true)
 |-- Pickup Census Tract: long (nullable = true)
 |-- Dropoff Census Tract: long (nullable = true)
 |-- Pickup Community Area: integer (nullable = true)
 |-- Dropoff Community Area: integer (nullable = true)
 |-- Fare: double (nullable = true)
 |-- Tips: double (nullable = true)
 |-- Tolls: double (nullable = true)
 |-- Extras: double (nullable = true)
 |-- Trip Total: double (nullable = true)
 |-- Payment Type: string (nullable = true)
 |-- Company: string (nullable = true)
 |-- Pickup Centroid Latitude: double (nullable = true)
 |-- Pickup Centroid Longitude: double (nullable = true)
 |-- Pickup Centroid Location: string (nullable = true)
 |-- Dropoff Centroid Latitude: double (nullable = true)
 |-- Dropoff Centroid Longitude: double (nullable = true)
 |-- Dropoff Centroid  Location: string (nullable = true)

```
Results:
* As a result to the applied code we can see our new schema that contains data types in date and time
* Characters that did not allow the cast from one data type to another have been eliminated

After that, new columns have also been created from the existing ones that will help us for the subsequent analysis.

```SCALA
        val tripsdf3 = tripsdf2
          .withColumn("Price X Minute",round(col("Fare")/col("Trip Seconds")*60,2))
          .withColumn("Price X Mile",round(col("Fare")/col("Trip Miles"),2))
          tripsdf3.show()

  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+-----+----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
  |             Trip ID|             Taxi ID|Trip Start Timestamp| Trip End Timestamp|Trip Seconds|Trip Miles|Pickup Census Tract|Dropoff Census Tract|Pickup Community Area|Dropoff Community Area| Fare|Tips|Tolls|Extras|Trip Total|Payment Type|             Company|Pickup Centroid Latitude|Pickup Centroid Longitude|Pickup Centroid Location|Dropoff Centroid Latitude|Dropoff Centroid Longitude|Dropoff Centroid  Location|Price X Minute|Price X Mile|
  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+-----+----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
  |f24ef1302cd672bbc...|1801165e22d31b0d2...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         885|      3.45|               null|                null|                 null|                  null|12.75| 0.0|  0.0|   4.0|     16.75|        Cash|           Flash Cab|                    null|                     null|                    null|                     null|                      null|                      null|          0.86|         3.7|
  |8e40b9c59af7b2897...|7a387c4ea03aea6f0...| 2019-01-01 00:00:00|2019-01-01 00:00:00|           8|       0.0|               null|                null|                 null|                  null| 3.25| 0.0|  0.0|   0.0|      3.25|        Cash|Chicago Carriage ...|                    null|                     null|                    null|                     null|                      null|                      null|         24.38|        null|
  |189631d3891a33361...|d1d781b6807176ed5...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         960|       4.9|               null|                null|                 null|                  null|15.75|3.35|  0.0|   1.0|      20.1| Credit Card|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|          0.98|        3.21|
  |e4941ac16f2fccf07...|6933327f9cc740e89...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         420|       1.5|               null|                null|                 null|                  null|  7.5| 0.0|  0.0|   0.0|       7.5|        Cash|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|          1.07|         5.0|
  |57a104b6cf260f117...|6933327f9cc740e89...| 2019-01-01 00:00:00|2019-01-01 00:15:00|         420|       0.5|               null|                null|                 null|                  null|  6.0| 1.0|  0.0|   1.0|       8.0| Credit Card|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|          0.86|        12.0|
  |15926814373db081e...|d1d781b6807176ed5...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         180|       0.4|               null|                null|                 null|                  null| 4.25| 0.0|  0.0|   1.5|      5.75|        Cash|Star North Manage...|                    null|                     null|                    null|                     null|                      null|                      null|          1.42|       10.63|
  |dbd155d142186f230...|fe08515b1e0b74cf1...| 2019-01-01 00:00:00|2019-01-01 00:00:00|         360|       1.7|               null|                null|                 null|                  null|  6.8| 0.0|  0.0|   0.0|       6.8|        Cash|Taxi Affiliation ...|                    null|                     null|                    null|                     null|                      null|                      null|          1.13|         4.0|

```
### Data Cleansing
The process of data cleansing was done by:

* Delete Rows that have null values in the following fields: Total Trip , Trip Miles and Trip Seconds
* Delete the columns that have values equal to zero in the following fields: Total Trip , Trip Miles and Trip Seconds

```SCALA
  val tripsdf4 = tripsdf3.filter(col("Trip Seconds").isNotNull && col("Trip Miles").isNotNull
    && col("Trip Total").isNotNull)

  val tripsdf5 = tripsdf4
    .filter(col("Trip Seconds") =!= 0 && col("Trip Miles") =!= 0
      && col("Trip Total") =!= 0)

```
We have already filtered our data but let's see how many fields have been filtered by each of the criteria

```SCALA
  val DfCleaned = tripsdf3.filter(col("Trip Seconds").isNull && col("Trip Miles").isNull
       && col("Trip Total").isNull)

  val dfCleaned_2 = tripsdf3
    .filter(col("Trip Seconds") === 0  && col("Trip Miles") === 0
      && col("Trip Total") === 0)

// Insights for trashed data
   DfCleaned.show()
   println("There is " + DfCleaned.count() + " Rows")
   dfCleaned_2.show()
   println("There is " + dfCleaned_2.count() + " Rows")

+--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+----+----+-----+------+----------+------------+------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
|             Trip ID|             Taxi ID|Trip Start Timestamp| Trip End Timestamp|Trip Seconds|Trip Miles|Pickup Census Tract|Dropoff Census Tract|Pickup Community Area|Dropoff Community Area|Fare|Tips|Tolls|Extras|Trip Total|Payment Type|     Company|Pickup Centroid Latitude|Pickup Centroid Longitude|Pickup Centroid Location|Dropoff Centroid Latitude|Dropoff Centroid Longitude|Dropoff Centroid  Location|Price X Minute|Price X Mile|
+--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+----+----+-----+------+----------+------------+------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
|264fa814de4994fe3...|f99d866ed2543d082...| 2019-01-17 08:00:00|2019-01-17 08:00:00|        null|      null|        17031839100|         17031281900|                   32|                    28|null|null| null|  null|      null|        Cash|City Service|            41.880994471|            -87.632746489|    POINT (-87.632746...|             41.879255084|             -87.642648998|      POINT (-87.642648...|          null|        null|
  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+----+----+-----+------+----------+------------+------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+

There is 1 Rows


  |             Trip ID|             Taxi ID|Trip Start Timestamp| Trip End Timestamp|Trip Seconds|Trip Miles|Pickup Census Tract|Dropoff Census Tract|Pickup Community Area|Dropoff Community Area|Fare|Tips|Tolls|Extras|Trip Total|Payment Type|             Company|Pickup Centroid Latitude|Pickup Centroid Longitude|Pickup Centroid Location|Dropoff Centroid Latitude|Dropoff Centroid Longitude|Dropoff Centroid  Location|Price X Minute|Price X Mile|
  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+----+----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
  |05b818461566782f5...|c4bbc24ad08741c15...| 2019-01-01 02:30:00|2019-01-01 02:30:00|           0|       0.0|        17031830700|         17031830700|                    3|                     3| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|            41.958055933|            -87.660389456|    POINT (-87.660389...|             41.958055933|             -87.660389456|      POINT (-87.660389...|          null|        null|
  |e0093bbda4a4c749a...|1e3a48e9cf98c6228...| 2019-01-01 04:30:00|2019-01-01 04:30:00|           0|       0.0|               null|                null|                 null|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Chicago Independents|                    null|                     null|                    null|                     null|                      null|                      null|          null|        null|
  |12a77573234acfe1f...|ac46c4b24441cc942...| 2019-01-01 09:30:00|2019-01-01 09:30:00|           0|       0.0|               null|                null|                    8|                     8| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Blue Ribbon Taxi ...|            41.899602111|            -87.633308037|    POINT (-87.633308...|             41.899602111|             -87.633308037|      POINT (-87.633308...|          null|        null|
  |d7701986942accd83...|e00149335c9dc8743...| 2019-01-01 16:30:00|2019-01-01 16:30:00|           0|       0.0|        17031980000|         17031980000|                   76|                    76| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|             41.97907082|            -87.903039661|    POINT (-87.903039...|              41.97907082|             -87.903039661|      POINT (-87.903039...|          null|        null|
  |5aff827b95fb88e90...|6d8a8aee1d12063ab...| 2019-01-01 16:45:00|2019-01-01 16:45:00|           0|       0.0|               null|                null|                    2|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|            42.001571027|            -87.695012589|    POINT (-87.695012...|                     null|                      null|                      null|          null|        null|
  |8ee968d5c9fe4012b...|73c236adbd209aa8b...| 2019-01-01 17:00:00|2019-01-01 17:00:00|           0|       0.0|               null|                null|                   11|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|            41.978829526|            -87.771166703|    POINT (-87.771166...|                     null|                      null|                      null|          null|        null|
  |0e882547f482d9eee...|73c236adbd209aa8b...| 2019-01-01 17:15:00|2019-01-01 17:15:00|           0|       0.0|               null|                null|                   28|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|            41.874005383|             -87.66351755|    POINT (-87.663517...|                     null|                      null|                      null|          null|        null|
  |111d04f07b2f7ddc4...|74f529987857c81bc...| 2019-01-01 19:45:00|2019-01-01 19:45:00|           0|       0.0|               null|                null|                 null|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|                    null|                     null|                    null|                     null|                      null|                      null|          null|        null|
  |01a6dafd065b68fca...|77c7e7dc3fdd00c2c...| 2019-01-01 21:00:00|2019-01-01 21:00:00|           0|       0.0|        17031320400|                null|                   32|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|            41.877406123|            -87.621971652|    POINT (-87.621971...|                     null|                      null|                      null|          null|        null|
  |382b5dc8ebc22a84e...|a9725437189ded1ca...| 2019-01-02 09:30:00|2019-01-02 09:30:00|           0|       0.0|               null|                null|                   49|                    49| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Blue Ribbon Taxi ...|            41.706587882|            -87.623366512|    POINT (-87.623366...|             41.706587882|             -87.623366512|      POINT (-87.623366...|          null|        null|
  |81cdd330d8caf43fa...|4dd67761f315fa120...| 2019-01-02 11:00:00|2019-01-02 11:00:00|           0|       0.0|        17031320400|         17031320400|                   32|                    32| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Star North Manage...|            41.877406123|            -87.621971652|    POINT (-87.621971...|             41.877406123|             -87.621971652|      POINT (-87.621971...|          null|        null|
  |3043c942896e6a74d...|f1baa96dafabbdebd...| 2019-01-02 12:15:00|2019-01-02 12:15:00|           0|       0.0|               null|                null|                 null|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|           Flash Cab|                    null|                     null|                    null|                     null|                      null|                      null|          null|        null|
  |3cfe6d79d85641d4f...|e34b35da07b018b91...| 2019-01-02 14:45:00|2019-01-02 14:45:00|           0|       0.0|        17031081401|         17031081401|                    8|                     8| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|KOAM Taxi Associa...|             41.89503345|            -87.619710672|    POINT (-87.619710...|              41.89503345|             -87.619710672|      POINT (-87.619710...|          null|        null|
  |b20decf8a3a11d3e0...|92eaa28167b878533...| 2019-01-02 21:45:00|2019-01-02 21:45:00|           0|       0.0|        17031980000|                null|                   76|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Blue Ribbon Taxi ...|             41.97907082|            -87.903039661|    POINT (-87.903039...|                     null|                      null|                      null|          null|        null|
  |70dff9be66d6fdcce...|14bbb3f71ff875f53...| 2019-01-02 22:00:00|2019-01-02 22:00:00|           0|       0.0|               null|                null|                   28|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|KOAM Taxi Associa...|            41.874005383|             -87.66351755|    POINT (-87.663517...|                     null|                      null|                      null|          null|        null|
  |476d5e96af9bbb586...|2f4a8c0bf7ab4ebc2...| 2019-01-02 22:15:00|2019-01-02 22:15:00|           0|       0.0|        17031839100|         17031839100|                   32|                    32| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|            41.880994471|            -87.632746489|    POINT (-87.632746...|             41.880994471|             -87.632746489|      POINT (-87.632746...|          null|        null|
  |960f12ab8739dbb5a...|c1762ee46d5dc423c...| 2019-01-03 03:00:00|2019-01-03 03:00:00|           0|       0.0|               null|                null|                 null|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|                    null|                     null|                    null|                     null|                      null|                      null|          null|        null|
  |37711f7d8e1258c62...|4dd67761f315fa120...| 2019-01-03 08:00:00|2019-01-03 08:00:00|           0|       0.0|        17031280100|         17031280100|                   28|                    28| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Star North Manage...|            41.885300022|            -87.642808466|    POINT (-87.642808...|             41.885300022|             -87.642808466|      POINT (-87.642808...|          null|        null|
  |e8100e2c42456a7c0...|131107e9f0f51399b...| 2019-01-03 09:00:00|2019-01-03 09:00:00|           0|       0.0|               null|                null|                   46|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Taxi Affiliation ...|            41.741242728|            -87.551428197|    POINT (-87.551428...|                     null|                      null|                      null|          null|        null|
  |f7f7f5a11d8a036b7...|d41ab2be597b82c3e...| 2019-01-03 14:15:00|2019-01-03 14:15:00|           0|       0.0|               null|                null|                 null|                  null| 0.0| 0.0|  0.0|   0.0|       0.0|        Cash|Choice Taxi Assoc...|                    null|                     null|                    null|                     null|                      null|                      null|          null|        null|
  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+----+----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
only showing top 20 rows

There is 5710 Rows
```
Results 
* As a result, it is obtained that by the first criterion only 1 has been eliminated.
* With the second criterion, 5710 have been eliminated

### Outliers
Outliers are data points in a dataset which stand far from other data points.
Treating outliers is one of the main steps in data preparation in data science
.The more the outliers you have in your dataset the more the skewness you have.

In this project we ar going to use  “approxquantile” to calculate IQR

If you arrange a dataset in order then the middle data pint is called Median ( or Q2 in the context of IQR).Then divide the dataset based on Median :

Q1 is the middle of the first half (25%).

Q3 is the middle of the second half(75%).

The interquartile range (IQR) is = Q3 – Q1

Once we found IQR,Q1,Q3 we compute the boundary and data points out of this boundary are potentially outliers:

lower boundary : Q1 – 1.5*IQR

upper boundary : Q3 + 1.5*IQR
    
```SCALA
     //Outliers
val quantiles = tripsdf5.stat.approxQuantile("Trip Total",
  Array(0.25, 0.75), 0.0)
val Q1 = quantiles(0)
val Q3 = quantiles(1)
val IQR = Q3 - Q1

val lowerRange = Q1 - 1.5 * IQR
val upperRange = Q3 + 1.5 * IQR

val outliers = tripsdf5.filter(s"Fare < $lowerRange or Fare > $upperRange")
outliers.show()
println("There is "+ outliers.count() + "atypical rows in relation to the Trip Total column")

  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+------+-----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
  |             Trip ID|             Taxi ID|Trip Start Timestamp| Trip End Timestamp|Trip Seconds|Trip Miles|Pickup Census Tract|Dropoff Census Tract|Pickup Community Area|Dropoff Community Area|  Fare| Tips|Tolls|Extras|Trip Total|Payment Type|             Company|Pickup Centroid Latitude|Pickup Centroid Longitude|Pickup Centroid Location|Dropoff Centroid Latitude|Dropoff Centroid Longitude|Dropoff Centroid  Location|Price X Minute|Price X Mile|
  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+------+-----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+
  |fcba761a77af45f37...|33c877ad242c484ce...| 2021-08-01 19:45:00|2019-08-01 20:30:00|        3180|      35.2|               null|                null|                   76|                    72| 84.75|18.75|  0.0|   8.5|     112.0| Credit Card|           U Taxicab|            41.980264315|            -87.913624596|    POINT (-87.913624...|             41.713148612|             -87.675075312|      POINT (-87.675075...|           1.6|        2.41|
  |fbe0689a2626def82...|33435b1dc7eaa3d31...| 2021-08-15 00:30:00|2019-08-15 01:15:00|        2878|     35.85|               null|         17031980000|                 null|                    76|  86.5|  0.0|  0.0|   0.0|      86.5|        Cash|        City Service|                    null|                     null|                    null|              41.97907082|             -87.903039661|      POINT (-87.903039...|           1.8|        2.41|
  |1691e7cda45d5e30d...|a35c437199f6c8815...| 2022-02-15 12:30:00|2019-02-15 13:15:00|        2725|     26.72|               null|                null|                 null|                    42| 64.25|  0.0|  0.0|   5.0|     69.75| Credit Card|            Sun Taxi|                    null|                     null|                    null|              41.77887686|             -87.594925439|      POINT (-87.594925...|          1.41|         2.4|
  |ff4f7e6304dfe7fb1...|c9e0b487e0d5aef1d...| 2021-08-22 23:00:00|2019-08-22 23:45:00|        2640|      38.4|        17031980100|                null|                   56|                  null|  90.5|  0.0|  0.0|  50.0|     140.5| Credit Card|Star North Manage...|            41.785998518|            -87.750934289|    POINT (-87.750934...|                     null|                      null|                      null|          2.06|        2.36|
  |fc5163eb78a0db541...|4a6999af87938af45...| 2021-08-02 00:30:00|2019-08-02 08:45:00|       29848|      0.38|        17031980000|         17031980000|                   76|                    76|169.25|  0.0|  0.0|   0.0|    169.25|        Cash|            Sun Taxi|             41.97907082|            -87.903039661|    POINT (-87.903039...|              41.97907082|             -87.903039661|      POINT (-87.903039...|          0.34|      445.39|
  |ffd936e4be9ee230c...|884655d853cbe41e1...| 2021-08-24 03:45:00|2019-08-24 04:15:00|        2035|      29.7|               null|                null|                   45|                    76| 81.27|  0.0|  0.0|   0.0|     81.27|        Cash|           Flash Cab|            41.744199535|            -87.586348318|    POINT (-87.586348...|             41.980264315|             -87.913624596|      POINT (-87.913624...|           2.4|        2.74|
  |fb1a8bb564fee5a90...|296cddcb1099b802d...| 2021-08-30 20:00:00|2019-08-30 20:45:00|        2201|      28.0|        17031980000|                null|                   76|                  null| 66.75|  0.0|  0.0|  38.5|    105.25|        Cash|Chicago Independents|             41.97907082|            -87.903039661|    POINT (-87.903039...|                     null|                      null|                      null|          1.82|        2.38|
  |ffd501aba39129119...|b753b6afd94c77370...| 2021-08-20 15:30:00|2019-08-20 17:00:00|        4920|      34.6|               null|                null|                   76|                    70| 86.25|  0.0|  0.0|  10.0|     96.25|        Cash| Top Cab Affiliation|            41.980264315|            -87.913624596|    POINT (-87.913624...|             41.745757713|             -87.708365704|      POINT (-87.708365...|          1.05|        2.49|
  |fb3633565079410ae...|fc8be1ea1e079a3aa...| 2021-08-08 20:00:00|2019-08-08 20:45:00|        2716|      24.9|               null|                null|                   76|                    39| 61.25|  0.0|  0.0|   6.0|     67.25|        Cash|Chicago Independents|            41.980264315|            -87.913624596|    POINT (-87.913624...|             41.808916283|             -87.596183344|      POINT (-87.596183...|          1.35|        2.46|
  |ffd14792a18a26f16...|c7cc8a8dd5c3f7358...| 2021-08-20 18:30:00|2019-08-20 19:00:00|        2640|       1.5|        17031980000|                null|                   76|                  null|  61.0| 10.0|  0.0|  36.5|     107.5| Credit Card|Taxi Affiliation ...|             41.97907082|            -87.903039661|    POINT (-87.903039...|                     null|                      null|                      null|          1.39|       40.67|
  |fd4a5835abbac7700...|b16c8dacd90adaad2...| 2021-08-13 15:00:00|2019-08-13 15:45:00|        3412|      25.6|        17031980000|         17031980100|                   76|                    56|  65.0|  0.0|  0.0|   8.0|      73.0|        Cash|Chicago Independents|             41.97907082|            -87.903039661|    POINT (-87.903039...|             41.785998518|             -87.750934289|      POINT (-87.750934...|          1.14|        2.54|
  |fd5e21ff80b593e9b...|735f3541fe218b30c...| 2021-08-24 20:30:00|2019-08-24 21:15:00|        2723|     26.07|               null|                null|                 null|                  null| 64.75|  0.0|  0.0|   0.0|     64.75|        Cash|        City Service|                    null|                     null|                    null|                     null|                      null|                      null|          1.43|        2.48|
  |fdd2a64705eb1afad...|971e9358b7f00c293...| 2021-08-28 20:15:00|2019-08-28 21:00:00|        2280|       1.8|               null|                null|                   76|                    56| 71.75|  0.0|  0.0|   8.0|     79.75| Credit Card|Blue Ribbon Taxi ...|            41.980264315|            -87.913624596|    POINT (-87.913624...|              41.79259236|             -87.769615453|      POINT (-87.769615...|          1.89|       39.86|
  |fb4fa24acaec50c82...|045270ae331ab6164...| 2021-08-18 07:15:00|2019-08-18 08:00:00|        2504|     24.62|               null|                null|                   31|                  null| 59.25|  0.0|  0.0|   0.0|     59.25|        Cash|           Flash Cab|            41.850266366|            -87.667569312|    POINT (-87.667569...|                     null|                      null|                      null|          1.42|        2.41|
  |fe2366d577631d660...|82bc059c3b13e9734...| 2021-08-28 22:45:00|2019-08-28 23:30:00|        2759|     28.82|               null|                null|                 null|                    43| 69.75|  2.0|  0.0|   4.0|     76.25| Credit Card|           Flash Cab|                    null|                     null|                    null|             41.761577908|             -87.572781987|      POINT (-87.572781...|          1.52|        2.42|
  |fc9e54889457c5ed8...|4243ab34318764eb5...| 2021-08-10 22:00:00|2019-08-10 22:30:00|        2031|      26.9|               null|                null|                   28|                  null| 65.25|  0.0|  1.5| 37.75|     104.5|        Cash|          Globe Taxi|            41.874005383|             -87.66351755|    POINT (-87.663517...|                     null|                      null|                      null|          1.93|        2.43|
  |fb41bc19a5a16aeb0...|82bc059c3b13e9734...| 2021-08-09 17:30:00|2019-08-09 18:15:00|        2531|     32.37|               null|         17031980000|                 null|                    76|  77.5|  0.0|  0.0|   7.5|      85.0|        Cash|           Flash Cab|                    null|                     null|                    null|              41.97907082|             -87.903039661|      POINT (-87.903039...|          1.84|        2.39|
  |179fb24614832d0d9...|11a881b7291dab7fe...| 2022-02-01 18:00:00|2019-02-01 18:45:00|        2915|     25.25|               null|                null|                   76|                    41|  60.5| 13.1|  0.0|   4.5|      78.6| Credit Card|    Medallion Leasin|            41.980264315|            -87.913624596|    POINT (-87.913624...|             41.794090253|             -87.592310855|      POINT (-87.592310...|          1.25|         2.4|
  |fadbb95de8548da76...|3563fdd4b400220b2...| 2021-08-22 20:30:00|2019-08-22 21:30:00|        3447|     24.94|               null|                null|                 null|                    41|  63.5| 0.01|  0.0|   5.0|     69.01| Credit Card|           Flash Cab|                    null|                     null|                    null|             41.794090253|             -87.592310855|      POINT (-87.592310...|          1.11|        2.55|
  |fdf1affca931bcfcc...|f886c7ded2e96ae77...| 2021-08-01 12:15:00|2021-08-01 13:00:00|        3120|      24.2|               null|                null|                   25|                    44| 59.75|  0.0|  0.0|   0.0|     59.75|     Unknown|Blue Ribbon Taxi ...|            41.890608853|            -87.756046711|    POINT (-87.756046...|             41.740205756|             -87.615969523|      POINT (-87.615969...|          1.15|        2.47|
  +--------------------+--------------------+--------------------+-------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+------+-----+-----+------+----------+------------+--------------------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+--------------+------------+

There is 32328 atypical rows in the relation with Trip Total column
```
Results 
* There is 32328 atypical rows in the relation with Trip Total column
### Insights
1.Top Companies with canceled trips
```SCALA
   // Company with Trips truncateds
val lowDf = dfCleaned_2.groupBy("Company")
  .agg(count("*").as("total_trips"))
  .orderBy(col("total_trips").desc_nulls_last)

lowDf.show()
+--------------------+-----------+
|             Company|total_trips|
+--------------------+-----------+
|Taxi Affiliation ...|       1934|
|Taxicab Insurance...|       1569|
|Star North Manage...|        758|
|Blue Ribbon Taxi ...|        503|
|Choice Taxi Assoc...|        285|
| Top Cab Affiliation|        210|
|           Flash Cab|        113|
|Chicago Independents|         75|
|KOAM Taxi Associa...|         62|
|Chicago Medallion...|         57|
|Suburban Dispatch...|         34|
|Taxi Affiliation ...|         19|
|        City Service|         15|
|3094 - 24059 G.L....|         11|
|    Medallion Leasin|         10|
|5062 - 34841 Sam ...|          8|
|312 Medallion Man...|          8|
|3721 - Santamaria...|          8|
|           U Taxicab|          6|
|6574 - Babylon Ex...|          5|
+--------------------+-----------+
only showing top 20 rows

```
The report shows the top 20 company with the highest numbers of canceled trips, it is filtered by Trip Miles in 0 and Trips second in 0 . It is also ordered by the Trip_total by descending. The reports has been construct with the dataframe Tripsdf5

2.Top companies benefited from the cancellation charge
```SCALA
     val charguesDF = tripsdf5
    .filter(col("Trip Miles") === 0 && col("Trip Seconds") === 0)
    .groupBy("Company").agg(sum("Trip Total").as("Trip_Total"))
    .orderBy(col("Trip_Total").desc_nulls_last)
      charguesDF.show()

  +--------------------+----------+
  |             Company|Trip_Total|
  +--------------------+----------+
  |Taxi Affiliation ...|1836511.14|
  |Suburban Dispatch...|1258401.01|
  |Star North Manage...| 357368.76|
  |Blue Ribbon Taxi ...| 332000.38|
  |Choice Taxi Assoc...| 171175.54|
  |    Medallion Leasin| 136059.35|
  |Taxicab Insurance...| 101152.84|
  | Top Cab Affiliation|   99542.6|
  |Chicago Independents|  31299.26|
  |          Globe Taxi|   26179.1|
  |            Sun Taxi|  17649.77|
  |KOAM Taxi Associa...|  17364.84|
  |Chicago Medallion...|  16995.54|
  |        City Service|  15120.58|
  |Nova Taxi Affilia...|    4456.8|
  |312 Medallion Man...|   4190.98|
  |     Chicago Taxicab|   3563.07|
  |     Petani Cab Corp|   3511.39|
  |       24 Seven Taxi|   3159.59|
  |Patriot Taxi Dba ...|   2585.14|
  +--------------------+----------+
```
The report shows the top 20 company with the highest numbers of Amount for trip cancellations , it is filtered by Trip Miles in 0 and Trips second in 0 . It is also ordered by the Trip_total by descending. The reports has been construct with the dataframe Tripsdf5



2.What are the distribution of Taking a taxi Per Hour?
```SCALA
     //Pick Hours
     val pickupsHoursDF = tripsdf5
       .withColumn("Hour of the day",hour(col("Trip End Timestamp")))
       .groupBy("Hour of the day")
       .agg(count("*").as("TotalTrips"))
       .orderBy(col("TotalTrips").desc_nulls_last)

       pickupsHoursDF.show()

  +---------------+----------+
  |Hour of the day|TotalTrips|
  +---------------+----------+
  |             18|   1034347|
  |             17|   1006802|
  |             19|    934014|
  |             16|    919208|
  |             15|    839101|
  |             13|    831067|
  |             14|    814614|
  |             12|    810910|
  |             20|    759548|
  |             11|    727557|
  |              9|    693130|
  |             10|    685471|
  |             21|    647498|
  |              8|    590989|
  |             22|    575571|
  |             23|    454425|
  |              7|    352664|
  |              0|    342382|
  |              1|    265499|
  |              2|    205537|
  +---------------+----------+
```
This report shows the distribution of the Activity of taking taxi by hours. For this report we help by our column Trip Star timestamp .The result was a group by the counter and an order by hours in ascending order.

3.Pickups Community Area with most trips
```SCALA
// Pickups Community Area with most trips
val Community_trips_picksDf = tripsdf5.groupBy("Pickup Community Area")
  .agg(count("*").as("total_trips"))
  .join(Community,col("Id_Com") === col("Pickup Community Area"))
  .orderBy(col("total_trips").desc_nulls_last)
Community_trips_picksDf.show()
+---------------------+-----------+------+---------------+
|Pickup Community Area|total_trips|Id_Com|       Com_Name|
+---------------------+-----------+------+---------------+
|                    8|    4060406|     8|Near North Side|
|                   32|    3693211|    32| (The) Loop[11]|
|                   28|    1474884|    28| Near West Side|
|                   76|    1403221|    76|         O'Hare|
|                    6|     431067|     6|      Lake View|
|                   33|     375845|    33|Near South Side|
|                   56|     284801|    56| Garfield Ridge|
|                    7|     277175|     7|   Lincoln Park|
|                    3|     129753|     3|         Uptown|
|                   24|     127407|    24|      West Town|
|                   77|      99138|    77|      Edgewater|
|                    1|      52272|     1|    Rogers Park|
|                    2|      46559|     2|     West Ridge|
|                   22|      43484|    22|   Logan Square|
|                    4|      43378|     4| Lincoln Square|
|                   41|      42455|    41|      Hyde Park|
|                   16|      39542|    16|    Irving Park|
|                   11|      30833|    11| Jefferson Park|
|                   14|      28123|    14|    Albany Park|
|                    5|      26776|     5|   North Center|
+---------------------+-----------+------+---------------+
only showing top 20 rows
```
The report shows the count number of the pickup trips made by Community area , the report contains a inner join Between Community and tripsdf5
4.Dropoff Community Area with most trips
```SCALA
val Community_trips_DropsDf = tripsdf5.groupBy("Dropoff Community Area")
  .agg(count("*").as("total_trips"))
  .join(Community, col("Id_Com") === col("Dropoff Community Area"))
  .orderBy(col("total_trips").desc_nulls_last)
Community_trips_DropsDf.show()

+----------------------+-----------+------+---------------+
|Dropoff Community Area|total_trips|Id_Com|       Com_Name|
+----------------------+-----------+------+---------------+
|                     8|    3930054|     8|Near North Side|
|                    32|    3015320|    32| (The) Loop[11]|
|                    28|    1508064|    28| Near West Side|
|                     7|     576188|     7|   Lincoln Park|
|                     6|     566520|     6|      Lake View|
|                    76|     503123|    76|         O'Hare|
|                    33|     447335|    33|Near South Side|
|                    24|     434427|    24|      West Town|
|                     3|     182554|     3|         Uptown|
|                    22|     145195|    22|   Logan Square|
|                    56|     135957|    56| Garfield Ridge|
|                    77|     134249|    77|      Edgewater|
|                     5|      89336|     5|   North Center|
|                     4|      80334|     4| Lincoln Square|
|                    41|      67432|    41|      Hyde Park|
|                     1|      64851|     1|    Rogers Park|
|                    16|      64149|    16|    Irving Park|
|                     2|      60276|     2|     West Ridge|
|                    21|      42326|    21|       Avondale|
|                    14|      40906|    14|    Albany Park|
+----------------------+-----------+------+---------------+
only showing top 20 rows

```
The report shows the count number of the Dropoff trips made by Community area , the report contains a inner join Between Community and tripsdf5 

5. Average price per Minute grouped by company
```SCALA
val Month_AvgMinuteDF = tripsdf5
  .withColumn("Month",month(col("Trip End Timestamp")))
  .groupBy("Month")
  .agg(round(avg("price x Minute"),2).as("Avg price x Minute"))
  .orderBy(col("Month").desc)

+-----+------------------+
|Month|Avg price x Minute|
+-----+------------------+
|   12|              1.18|
|   11|              1.18|
|   10|              1.13|
|    9|              1.12|
|    8|               1.1|
|    7|              1.08|
|    6|              1.11|
|    5|              1.12|
|    4|              1.16|
|    3|              1.29|
|    2|              1.18|
|    1|              1.26|
+-----+------------------+

```
The report shows the AVG price x minute of the trips by Company , the report contains a ratio the is calculated like Fare /Minutes .This gives us some information about the quality of service

6. Average price per Mile grouped by company
```SCALA
   val Month_AvgMilesDF = tripsdf5
  .withColumn("Month", month(col("Trip End Timestamp")))
  .groupBy("Month")
  .agg(round(avg("price x Mile"), 2).as("Avg price x Mile"))
  .orderBy(col("Month").desc)

Month_AvgMilesDF.show()
+-----+----------------+
|Month|Avg price x Mile|
+-----+----------------+
|   12|           13.23|
|   11|           12.54|
|   10|           10.33|
|    9|           10.91|
|    8|           10.27|
|    7|           10.16|
|    6|           10.41|
|    5|           10.25|
|    4|           10.62|
|    3|           11.52|
|    2|           10.43|
|    1|            11.0|
+-----+----------------+
```
The report shows the AVG price x Mile of the trips by Company , the report contains a ratio the is calculated like Fare /Miles .This gives us some information about the quality of service

7.Ratios of tip by Company
```SCALA
             Company|Ratio of tips|
+--------------------+-------------+
|Blue Ribbon Taxi ...|        5.018|
|1469 - 64126 Omar...|        0.662|
|6574 - Babylon Ex...|        0.623|
|Chicago Star Taxicab|        0.594|
|Taxi Affiliation ...|        0.594|
|3591 - 63480 Chuk...|        0.576|
|3011 - 66308 JBL ...|        0.573|
|3721 - Santamaria...|        0.554|
|5006 - 39261 Sali...|        0.547|
|5062 - 34841 Sam ...|        0.541|
|1085 - 72312 N an...|        0.519|
|3094 - 24059 G.L....|        0.514|
|Choice Taxi Assoc...|        0.509|
|3620 - 52292 Davi...|        0.506|
|     Gold Coast Taxi|        0.499|
|Chicago Medallion...|        0.499|
|4787 - 56058 Reny...|        0.498|
|Taxicab Insurance...|        0.497|
|2733 - 74600 Benn...|        0.495|
|4623 - 27290 Jay Kim|        0.489|
+--------------------+-------------+
```
The report shows the Ratio tips of the trips by Company , the report contains a ratio the is calculated like Trips /Miles .This gives us some information about the quality of service

8.Paymemt method per month
```SCALA
//paymemt method per month
val Year_Payment_type = tripsdf5
.withColumn("month", month(col("Trip End Timestamp")))
.groupBy("month","Payment Type")
.agg(round(sum("Trip Total"),3).as("Trip Total"))
.orderBy(col("month").desc,col("Trip Total").desc)

Year_Payment_type.show()

  |month|Payment Type|   Trip Total|
  +-----+------------+-------------+
  |   12| Credit Card|  10650434408|
  |   12|        Cash|   7246084.15|
  |   12|      Mobile|    404939.51|
  |   12|      Prcard|     326786.9|
  |   12|     Unknown|    159138.54|
  |   12|   No Charge|     18806.69|
  |   12|     Dispute|      6730.77|
  |   12|     Prepaid|       1157.0|
  |   11| Credit Card|  11965513667|
  |   11|        Cash|   7172237.05|
  |   11|      Mobile|    416781.56|
  |   11|      Prcard|    302079.86|
  |   11|     Unknown|    155480.07|
  |   11|   No Charge|      21768.1|
  |   11|     Dispute|      8178.15|
  |   11|     Prepaid|      1125.25|
  |   10| Credit Card|  15167604457|
  |   10|        Cash|   7999339.86|
  |   10|      Mobile|    489506.07|
  |   10|      Prcard|    298707.58|
  +-----+------------+-------------+
```
The report shows the Amount of the trip total of the trips by Month and Payment type  , the report contains the sum of trip total 

9 . Determine if the trip is short or long
```SCALA
//Long and Short Trip

val longDistanceThreshold = 30
val tripsWithLengthDF = tripsdf5.withColumn("isLong", col("Trip Miles") >= longDistanceThreshold)
val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()

tripsByLengthDF.show()
+------+--------+
|isLong|   count|
+------+--------+
|  true|   40123|
| false|13986911|
+------+--------+

```
To make this report, a long Distance Threshold variable has been declared, which will be our parameter to determine if the trip is short or long. This parameter has been determined this way because it is the number of miles it takes to cross the city.