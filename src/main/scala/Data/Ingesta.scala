package Data
import Data.Ingesta.tripsdf5
import Helper.FunctionHelper
import org.apache.logging.log4j.{Level, Logger}
import org.apache.logging.log4j.Level.values
import org.apache.logging.log4j.core.lookup.StrSubstitutor.replace
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{avg, col, column, count, expr, format_number, from_unixtime, hour, length, lit, max, mean, min, month, regexp_replace, round, split, stddev, stddev_pop, stddev_samp, sum, to_date, to_timestamp, unix_timestamp, when, year}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Ingesta extends App {

  val sparkConf = new SparkConf()
    .set("spark.driver.allowMultipleContexts", "true")
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile","src/main/resources/keyA.json")

  val spark = SparkSession.builder()
    .appName("Ingesta Data")
    .master("local[*]")
    .config(sparkConf)
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

  val googleStorageOutput = "gs://taxitrip_report/"


  val currencyFormat: Column => Column = (number: Column) =>concat(lit("$ "),format_number(number,2))
  val percentageFormat: Column => Column = (number: Column) => concat(format_number(number * 100, 2), lit(" %"))
  //tripsDF.show()

  //Eda

  println("There is " + tripsDF.count() + " Rows in the dataset")
  println("There is " + tripsDF.distinct().count() + "Unique Row in the dataset")
  println("there is " + tripsDF.columns.size + " Number of Columns")


  val stats_df = tripsDF.describe("Trip Seconds", "Trip Miles", "Fare", "Tips", "Tolls", "Extras", "Trip Total")
  stats_df.show()


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
    .withColumn("Trip Miles", col("Trip Miles").cast(DoubleType))
    .withColumn("Trip Start Timestamp", unix_timestamp(col("Trip Start Timestamp"), "MM/dd/yyyy hh:mm:ss a").cast(TimestampType))
    .withColumn("Trip End Timestamp", unix_timestamp(col("Trip End Timestamp"), "MM/dd/yyyy hh:mm:ss a").cast(TimestampType))
    .withColumn("Trip Seconds", regexp_replace(col("Trip Seconds"), ",", ""))
    .withColumn("Trip Seconds", col("Trip Seconds").cast(IntegerType))

  //tripsdf2.show()
  //tripsdf2.printSchema()

  //Aggregations
  val tripsdf3 = tripsdf2
    .withColumn("Price X Minute", round(col("Fare") / col("Trip Seconds") * 60, 2))
    .withColumn("Price X Mile", round(col("Fare") / col("Trip Miles"), 2))
  // tripsdf3.show()


  // Data cleanising

  val tripsdf4 = tripsdf3.filter(col("Trip Seconds").isNotNull && col("Trip Miles").isNotNull
    && col("Trip Total").isNotNull)

  val tripsdf5 = tripsdf4
    .filter(col("Trip Seconds") =!= 0 && col("Trip Miles") =!= 0
      && col("Trip Total") =!= 0)

  println(tripsdf5.count())


  val DfCleaned = tripsdf3.filter(col("Trip Seconds").isNull && col("Trip Miles").isNull
    && col("Trip Total").isNull)

  val dfCleaned_2 = tripsdf3
    .filter(col("Trip Seconds") === 0 && col("Trip Miles") === 0
      && col("Trip Total") === 0)

  dfCleaned_2.show()





  // Insights for trashed data
  DfCleaned.show()
  println("There is " + DfCleaned.count() + " Rows")
  dfCleaned_2.show()
  println("There is " + dfCleaned_2.count() + " Rows")


  //Company with Trips truncateds
  val lowDf = dfCleaned_2.groupBy("Company")
    .agg(count("*").as("total_trips"))
    .orderBy(col("total_trips").desc_nulls_last)

  lowDf.show()

//Outliers

val dfIdOutliers = FunctionHelper.getOutlier(tripsdf5, spark)
println(dfIdOutliers.count())
val tripsdf6 = tripsdf5.join(dfIdOutliers,tripsdf5.col("Trip Id") === dfIdOutliers.col("Trip Id"),"left_anti")
println(tripsdf6.count())




  //outliers.show()
  //println("There is "+ outliers.count() + " atypical rows in relation to the Trip Total column")

  tripsdf5.persist(StorageLevel.MEMORY_AND_DISK)



  //which company is more benefited with the cancellation charges
  val charguesDF = tripsdf3
    .filter(col("Trip Miles") === 0 && col("Trip Seconds") === 0.0)
    .groupBy("Company").agg(round(sum("Trip Total"),2).as("Amount_Total"))
    .orderBy(col("Amount_Total").desc_nulls_last)

  val charguesDF_R = charguesDF.select(
    col("Company"),
    (currencyFormat(col("Amount_Total")).as("Amount_Total")))


  charguesDF_R.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte1/")



  //Pick Hours
  val pickupsHoursDF = tripsdf5
    .withColumn("Hour of the day", hour(col("Trip End Timestamp")))
    .groupBy("Hour of the day")
    .agg(count("*").as("TotalTrips"))
    .orderBy(col("TotalTrips").desc_nulls_last)

  pickupsHoursDF.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte2/")


  // Pickups Community Area with most trips
  val Community_trips_picksDf = tripsdf5.groupBy("Pickup Community Area")
    .agg(count("*").as("total_trips"))
    .join(Community,col("Id_Com") === col("Pickup Community Area"))
    .orderBy(col("total_trips").desc_nulls_last)

  Community_trips_picksDf.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte3/")

  // Dropoff Community Area whit most trips
  val Community_trips_DropsDf = tripsdf5.groupBy("Dropoff Community Area")
    .agg(count("*").as("total_trips"))
    .join(Community, col("Id_Com") === col("Dropoff Community Area"))
    .orderBy(col("total_trips").desc_nulls_last)

  Community_trips_DropsDf.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte4/")

  // average price per miles and per minute grouped by year
  val Year_AvgMinuteDF = tripsdf5
    .withColumn("Month", month(col("Trip End Timestamp")))
    .groupBy("Month")
    .agg(round(avg("price x Minute"), 2).as("Avg price x Minute"))
    .orderBy(col("Month").desc)

  val Year_AvgMinuteDF_R = Year_AvgMinuteDF.select(
    col("Month"),
    (currencyFormat(col("Avg price x Minute")).as("Avg price x Minute")),
  )

  Year_AvgMinuteDF_R.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte5/")



  val Year_AvgMilesDF = tripsdf5
    .withColumn("Month", month(col("Trip End Timestamp")))
    .groupBy("Month")
    .agg(round(avg("price x Mile"), 2).as("Avg price x Mile"))

    .orderBy(col("Month").desc)

  val Year_AvgMilesDF_R = Year_AvgMilesDF.select(
    col("Month"),
    (currencyFormat(col("Avg price x Mile")).as("Avg price x Mile")),
  )

  Year_AvgMilesDF_R.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte6/")

  //tips por company /quality service
  val Company_service_qlty = tripsdf5
    .groupBy("Company")
    .agg(round(sum(col("Tips"))/sum(col("Trip Miles")),3).alias("Ratio of tips x Mile"))
    .orderBy(col("Ratio of tips x Mile").desc)

  val Company_service_qlty_R = Company_service_qlty.select(
    col("Company"),
    (currencyFormat(col("Ratio of tips x Mile")).as("Ratio of tips x Mile")),
  )
  Company_service_qlty_R.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte7/")

  //paymemt method per month
  val Year_Payment_type = tripsdf5
    .withColumn("month", month(col("Trip End Timestamp")))
    .groupBy("month","Payment Type")
    .agg(round(sum("Trip Total"),3).as("Amount Total"))
    .orderBy(col("month").desc,col("Amount Total").desc)

  val Year_Payment_type_R = Year_Payment_type.select(
    col("month"),
    col("Payment Type"),
    (currencyFormat(col("Amount Total")).as("Amount Total")),
  )
  Year_Payment_type_R.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte8/")
  //Long and Short Trip

  val longDistanceThreshold = 30
  val tripsWithLengthDF = tripsdf5.withColumn("isLong", col("Trip Miles") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()

  tripsByLengthDF.repartition(1).write.option("header","true").option("sep",";").csv(googleStorageOutput + "Reporte9/")







}

