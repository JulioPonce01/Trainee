package Data

import org.apache.logging.log4j.Level.values
import org.apache.logging.log4j.core.lookup.StrSubstitutor.replace
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{avg, col, column, count, expr, from_unixtime, hour, length, lit, max, mean, min, month, regexp_replace, round, split, stddev, stddev_pop, stddev_samp, sum, to_date, to_timestamp, unix_timestamp, when, year}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable

object Ingesta extends App {


  val spark = SparkSession.builder()
    .appName("Ingesta Data")
    .config("spark.master", "local")
    .getOrCreate()

  val Community = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "no aplica")
    .csv("src/main/resources/prueba.csv")

  val tripsDF =
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ",")
      .csv("src/main/resources/Taxi_Trips_-_2019.csv")
  //tripsDF.show()

  //Eda

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



  //Data Preparation

  val tripsdf2 = tripsDF
    .withColumn("Trip Miles",col("Trip Miles").cast(DoubleType))
    .withColumn("Trip Start Timestamp",unix_timestamp(col("Trip Start Timestamp"),"MM/dd/yyyy hh:mm:ss a").cast(TimestampType))
    .withColumn("Trip End Timestamp",unix_timestamp(col("Trip End Timestamp"),"MM/dd/yyyy hh:mm:ss a").cast(TimestampType))
    .withColumn("Trip Seconds",regexp_replace(col("Trip Seconds"), ",", ""))
    .withColumn("Trip Seconds",col("Trip Seconds").cast(IntegerType))

  //tripsdf2.show()
  //tripsdf2.printSchema()

  //Aggregations
  val tripsdf3 = tripsdf2
    .withColumn("Price X Minute",round(col("Fare")/col("Trip Seconds")*60,2))
    .withColumn("Price X Mile",round(col("Fare")/col("Trip Miles"),2))
  // tripsdf3.show()


  // Data cleanising

  val tripsdf4 = tripsdf3.filter(col("Trip Seconds").isNotNull && col("Trip Miles").isNotNull
    && col("Trip Total").isNotNull)

  val tripsdf5 = tripsdf4
    .filter(col("Trip Seconds") =!= 0 && col("Trip Miles") =!= 0
      && col("Trip Total") =!= 0)

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




  //Company with Trips truncateds
  val lowDf = dfCleaned_2.groupBy("Company")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)

  //lowDf.show()

  //Outliers
  val quantiles = tripsdf5.stat.approxQuantile("Fare",
    Array(0.25, 0.75), 0.0)
  val Q1 = quantiles(0)
  val Q3 = quantiles(1)
  val IQR = Q3 - Q1

  val lowerRange = Q1 - 1.5 * IQR
  val upperRange = Q3 + 1.5 * IQR

  val outliers = tripsdf5.filter(s"Fare < $lowerRange or Fare > $upperRange")
  outliers.show()
  println("There is "+ outliers.count() + " atypical rows in relation to the Trip Total column")

  //which company is more benefited with the cancellation charges
  val charguesDF = tripsdf3
    .filter(col("Trip Miles") === 0 && col("Trip Seconds") === 0.0)
    .groupBy("Company").agg(round(sum("Trip Total"),2).as("Trip_Total"))
    .orderBy(col("Trip_Total").desc_nulls_last)
  charguesDF.show()

  //Pick Hours
  val pickupsHoursDF = tripsdf5
    .withColumn("Hour of the day",hour(col("Trip End Timestamp")))
    .groupBy("Hour of the day")
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("TotalTrips").desc_nulls_last)

  pickupsHoursDF.show()


  // Pickups Community Area with most trips
  val Community_trips_picksDf = tripsdf5.groupBy("Pickup Community Area")
    .agg(count("*").as("total_trips"))
    .join(Community,col("Id_Com") === col("Pickup Community Area"))
    .orderBy(col("total_trips").desc_nulls_last)
  Community_trips_picksDf.show()

  // Dropoff Community Area whit most trips
  val Community_trips_DropsDf = tripsdf5.groupBy("Dropoff Community Area")
    .agg(count("*").as("total_trips"))
    .join(Community, col("Id_Com") === col("Dropoff Community Area"))
    .orderBy(col("total_trips").desc_nulls_last)
  Community_trips_DropsDf.show()

  // average price per miles and per minute grouped by year

  val Year_AvgMinuteDF = tripsdf5
    .withColumn("Month",month(col("Trip End Timestamp")))
    .groupBy("Month")
    .agg(round(avg("price x Minute"),2).as("Avg price x Minute"))
    .orderBy(col("Month").desc)

  Year_AvgMinuteDF.show()

  val Year_AvgMilesDF = tripsdf5
    .withColumn("Month", month(col("Trip End Timestamp")))
    .groupBy("Month")
    .agg(round(avg("price x Mile"), 2).as("Avg price x Mile"))

    .orderBy(col("Month").desc)

  Year_AvgMilesDF.show()

  //tips por company /quality service
  val Company_service_qlty = tripsdf5
    .groupBy("Company")
    .agg(round(sum(col("Tips"))/sum(col("Trip Miles")),3).as("Ratio of tips"))
    .orderBy(col("Ratio of tips").desc)

  Company_service_qlty.show()


  //paymemt method per month
  val Year_Payment_type = tripsdf5
    .withColumn("month", month(col("Trip End Timestamp")))
    .groupBy("month","Payment Type")
    .agg(round(sum("Trip Total"),3).as("Trip Total"))
    .orderBy(col("month").desc,col("Trip Total").desc)

  Year_Payment_type.show()

  //Long and Short Trip

  val longDistanceThreshold = 30
  val tripsWithLengthDF = tripsdf5.withColumn("isLong", col("Trip Miles") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()

  tripsByLengthDF.show()





}
