package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

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
      .filter(col("current_travel_time") >= 0 || col("current_travel_time").isNull)
      .filter(col("free_flow_travel_time") >= 0 || col("free_flow_travel_time").isNull)
      .filter(col("free_flow_speed") >= 0 || col("free_flow_speed").isNull)
      .dropDuplicates("lat", "lon", "event_ts")

    val locWindow = Window.partitionBy("lat", "lon")

    val enriched = cleaned
      // ---- IMPUTATIONS ----
      .withColumn(
        "current_travel_time_imputed",
        coalesce(
          col("current_travel_time"),
          percentile_approx(col("current_travel_time"), lit(0.5), lit(10000)).over(locWindow)
        )
      )
      .withColumn(
        "free_flow_travel_time_imputed",
        coalesce(
          col("free_flow_travel_time"),
          percentile_approx(col("free_flow_travel_time"), lit(0.5), lit(10000)).over(locWindow)
        )
      )
      // ---- METRICS ----
      .withColumn(
        "congestion_index",
        col("current_travel_time_imputed") / col("free_flow_travel_time_imputed")
      )
      .withColumn(
        "delay_seconds",
        col("current_travel_time_imputed") - col("free_flow_travel_time_imputed")
      )
      .withColumn(
        "is_congested",
        col("congestion_index") > 1.2
      )

    println("=== TrafficPreprocessor: SILVER sample ===")
    enriched.show(5, truncate = false)

    enriched
  }
}
