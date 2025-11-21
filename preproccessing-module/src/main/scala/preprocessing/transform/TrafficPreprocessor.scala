package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TrafficPreprocessor {

  def transform(raw: DataFrame): DataFrame = {
    println("=== TrafficPreprocessor: RAW schema ===")
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

      .withColumn("current_travel_time",   col("current_travel_time").cast("long"))
      .withColumn("free_flow_speed",       col("free_flow_speed").cast("double"))
      .withColumn("free_flow_travel_time", col("free_flow_travel_time").cast("long"))
      .withColumn("road_closure",          col("road_closure").cast("boolean"))
      .withColumn("data_provider",         col("data_provider").cast("string"))

    val cleaned = df
      .filter(col("event_ts").isNotNull)
      .filter(col("lat").isNotNull && col("lon").isNotNull)

    println("=== TrafficPreprocessor: SILVER sample ===")
    cleaned.show(5, truncate = false)

    cleaned
  }
}
