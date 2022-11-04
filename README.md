# Trainee
in this repository will be used for the big data trainee program

# About the Dataset 
Taxi trips reported to the City of Chicago in its role as a regulatory agency. 
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

### DATASET'S EDA 
EDA (Exploratory Data Analysis) is the stepping stone of Data Science, and the process involves 
investigating data and discovering underlying patterns in data. The EDA for this project is resolve 
with this code : 

``` SCALA
  println("There is " + tripsDF.count() + " Rows in the dataset")
  println("There is " + tripsDF.distinct().count() + "Unique Row in the dataset")
  println("there is "+ tripsDF.columns.size + " Number of Columns")
  
   def countCols(columns: Array[String]): Array[Column] = {
  columns.map(c => {
    count(col(c)).alias(c)
  })
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

As a result we have a table that in first rows has the number of register and in the second
it has the count of the nulls 

+--------+--------+--------------------+------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+--------+--------+--------+--------+----------+------------+--------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+
| Trip ID| Taxi ID|Trip Start Timestamp|Trip End Timestamp|Trip Seconds|Trip Miles|Pickup Census Tract|Dropoff Census Tract|Pickup Community Area|Dropoff Community Area|    Fare|    Tips|   Tolls|  Extras|Trip Total|Payment Type| Company|Pickup Centroid Latitude|Pickup Centroid Longitude|Pickup Centroid Location|Dropoff Centroid Latitude|Dropoff Centroid Longitude|Dropoff Centroid  Location|
+--------+--------+--------------------+------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+--------+--------+--------+--------+----------+------------+--------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+
|16477365|16477365|            16477365|          16476820|    16474629|  16476815|           10881829|            10784224|             15239379|              14818716|16475794|16475794|16217353|16475794|  16475794|    16477365|16477365|                15241413|                 15241413|                15241413|                 14877765|                  14877765|                  14877765|
|       0|       0|                   0|               545|        2736|       550|            5595536|             5693141|              1237986|               1658649|    1571|    1571|  260012|    1571|      1571|           0|       0|                 1235952|                  1235952|                 1235952|                  1599600|                   1599600|                   1599600|
+--------+--------+--------------------+------------------+------------+----------+-------------------+--------------------+---------------------+----------------------+--------+--------+--------+--------+----------+------------+--------+------------------------+-------------------------+------------------------+-------------------------+--------------------------+--------------------------+


```

### Data Preparation
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
```SCALA
  val tripsdf4 = tripsdf3.filter(col("Trip Seconds").isNotNull && col("Trip Miles").isNotNull
    && col("Trip Total").isNotNull)

  val tripsdf5 = tripsdf4
    .filter(col("Trip Seconds") =!= 0 && col("Trip Miles") =!= 0
      && col("Trip Total") =!= 0)

```


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

### Outliers
```SCALA
     //Outliers
val quantiles = tripsdf5.stat.approxQuantile("Trip Total",
  Array(0.25, 0.75), 0.0)
val Q1 = quantiles(0)
val Q3 = quantiles(1)
val IQR = Q3 - Q1

val lowerRange = Q1 - 1.5 * IQR
val upperRange = Q3 + 1.5 * IQR

val outliers = tripsdf5.filter(s"Trip Total < $lowerRange or Trip Total > $upperRange")
outliers.show()
println("There is "+ outliers.count() + "atypical rows in relation to the Trip Total column")
```