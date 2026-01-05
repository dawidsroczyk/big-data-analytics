package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AirQualityPreprocessor {

  def transform(raw: DataFrame): DataFrame = {
    println("=== AirQualityPreprocessor: RAW schema ===")
    raw.printSchema()

    val locSplit = split(col("location"), ",")

    val df = raw
      .withColumn("lat", locSplit.getItem(0).cast("double"))
      .withColumn("lon", trim(locSplit.getItem(1)).cast("double"))
      .withColumn(
        "event_ts",
        to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
      )
      .withColumn("event_date", to_date(col("event_ts")))
      .withColumn("event_hour", hour(col("event_ts")))
      .withColumn("aqi",  col("aqi").cast("double"))
      .withColumn("pm25", col("pm2_5").cast("double"))
      .withColumn("pm10", col("pm10").cast("double"))
      .withColumn("no2",  col("no2").cast("double"))
      .withColumn("so2",  col("so2").cast("double"))
      .withColumn("o3",   col("o3").cast("double"))
      .withColumn("co",   col("co").cast("double"))
      .withColumn("nh3",  lit(null).cast("double"))
      .withColumn("data_provider", col("data_provider").cast("string"))

    val cleaned = df
      .filter(col("event_ts").isNotNull)
      .filter(col("lat").isNotNull && col("lon").isNotNull)

    println("=== AirQualityPreprocessor: SILVER sample ===")
//    cleaned.show(5, truncate = false)
//
    cleaned
  }
}
