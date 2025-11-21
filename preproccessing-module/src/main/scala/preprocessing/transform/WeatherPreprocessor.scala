package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object WeatherPreprocessor {

  def transform(raw: DataFrame): DataFrame = {
    println("=== WeatherPreprocessor: RAW schema ===")
    raw.printSchema()

    val locSplit = split(col("location"), ",")

    val df = raw
      .withColumn("lat", locSplit.getItem(0).cast("double"))
      .withColumn("lon", trim(locSplit.getItem(1)).cast("double"))
      .withColumn("event_ts", to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
      .withColumn("event_date", to_date(col("event_ts")))
      .withColumn("event_hour", hour(col("event_ts")))
      .withColumn("temperature", col("temperature").cast("double"))
      .withColumn("humidity", col("humidity").cast("double"))
      .withColumn("wind_speed", col("wind_speed").cast("double"))
      .withColumn("conditions", col("conditions").cast("string"))
      .withColumn("data_provider", col("data_provider").cast("string"))

    val cleaned = df
      .filter(col("event_ts").isNotNull)
      .filter(col("lat").isNotNull && col("lon").isNotNull)

    println("=== WeatherPreprocessor: CLEANED schema ===")
    cleaned.show(5, truncate = false)

    cleaned
  }
}
