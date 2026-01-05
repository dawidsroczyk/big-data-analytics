package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UvPreprocessor {

  def transform(raw: DataFrame): DataFrame = {
    println("=== UvPreprocessor: RAW schema ===")
    raw.printSchema()

    val locSplit = split(col("location"), ",")

    val df = raw
      .withColumn("lat", locSplit.getItem(0).cast("double"))
      .withColumn("lon", trim(locSplit.getItem(1)).cast("double"))
      .withColumn(
        "event_ts",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
      )
      .withColumn("event_date", to_date(col("event_ts")))
      .withColumn("event_hour", hour(col("event_ts")))
      .withColumn("uv_index", col("uv_index").cast("double"))
      .withColumn("data_provider", col("data_provider").cast("string"))

    val cleaned = df
      .filter(col("event_ts").isNotNull)
      .filter(col("lat").isNotNull && col("lon").isNotNull)

    println("=== UvPreprocessor: SILVER sample ===")
//    cleaned.show(5, truncate = false)
//
    cleaned
  }
}
