package Data

import org.apache.spark.sql.{Row, SparkSession}

object Ingesta extends App {
  val spark = SparkSession.builder()
    .appName("Ingesta Data")
    .config("spark.master", "local")
    .getOrCreate()

  val callsDF =
  spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/Taxi_Trips.csv")

  // showing a DF
  callsDF.show()
  callsDF.printSchema()


}
