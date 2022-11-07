package Helper

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object FunctionHelper {
  def getOutlier(dFtrips: DataFrame, spark: SparkSession): DataFrame = {
    val my_schema = StructType(Seq(
      StructField("Trip Id", StringType, nullable = false)
    ))
    var id_dataframe: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], my_schema)
    dFtrips.columns.foreach { columnName =>
      if (dFtrips.schema(columnName).dataType.typeName == "double") {
        val quantiles = dFtrips.stat.approxQuantile(columnName,
          Array(0.25, 0.75), 0.0)
        val Q1 = quantiles(0)
        val Q3 = quantiles(1)
        val IQR = Q3 - Q1

        val lowerRange = Q1 - 1.5 * IQR
        val upperRange = Q3 + 1.5 * IQR

        val outliers = dFtrips.filter(col(columnName) < lowerRange || col(columnName) > upperRange).select("Trip Id")

        id_dataframe = id_dataframe.union(outliers)
      }
    }
    id_dataframe.distinct()
  }


}
