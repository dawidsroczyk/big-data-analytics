package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UvPreprocessor {

  def transform(raw: DataFrame): DataFrame = {
    val locSplit = split(col("location"), ",")

    val cleaned = raw
      .withColumn("lat", locSplit.getItem(0).cast("double"))
      .withColumn("lon", trim(locSplit.getItem(1)).cast("double"))
      .withColumn(
        "event_ts",
        date_trunc(
          "minute",
          to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        )
      )
      .withColumn("uv_index", col("uv_index").cast("double"))
      .withColumn("uv_provider", col("data_provider").cast("string"))
      .filter(col("event_ts").isNotNull)
      .filter(col("lat").isNotNull && col("lon").isNotNull)

    // 🔑 CRITICAL: aggregate to ONE row per timestamp
    cleaned
      .groupBy("lat", "lon", "event_ts")
      .agg(
        avg("uv_index").as("uv_index"),
        first("uv_provider").as("uv_provider")
      )
  }
}
