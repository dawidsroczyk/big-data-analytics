package preprocessing.transform

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object JoinPreprocessor {

  /** Join weather and traffic data within ±2 minutes window */
  def joinWeatherTraffic(weather: DataFrame, traffic: DataFrame): DataFrame = {
    println("=== JoinPreprocessor: WEATHER schema ===")
    weather.printSchema()
    println("=== JoinPreprocessor: TRAFFIC schema ===")
    traffic.printSchema()

    val w = weather.alias("w")
    val t = traffic.alias("t")

    val joined = w.join(
        t,
        expr(
          """
          w.lat = t.lat AND
          w.lon = t.lon AND
          t.event_ts BETWEEN w.event_ts - INTERVAL 2 MINUTES
                         AND w.event_ts + INTERVAL 2 MINUTES
        """
        ),
        "inner"
      ).select(
        col("w.lat"),
        col("w.lon"),
        col("w.event_ts"),
        col("w.event_date"),
        col("w.event_hour"),

        // WEATHER
        col("w.conditions"),
        col("w.temperature"),
        col("w.humidity"),
        col("w.wind_speed"),
        col("w.data_provider").as("weather_provider"),

        // TRAFFIC
        col("t.current_travel_time"),
        col("t.free_flow_speed"),
        col("t.free_flow_travel_time"),
        col("t.road_closure"),
        col("t.data_provider").as("traffic_provider")
      )
      .dropDuplicates("lat", "lon", "event_ts")

    println("=== JoinPreprocessor: JOINED sample ===")
    joined.show(10, truncate = false)

    joined
  }

  /** Attach air quality data to features, truncating timestamps to seconds */
  def attachAirQuality(features: DataFrame, air: DataFrame): DataFrame = {
    val f = features.withColumn("event_ts_sec", unix_timestamp(col("event_ts"))).alias("f")
    val airSelected = air.select(
      col("lat"),
      col("lon"),
      col("event_ts"),
      col("o3"),
      col("aqi"),
      col("pm25"),
      col("pm10"),
      col("no2"),
      col("co"),
      col("nh3"),
      col("so2"),
      col("data_provider")
    )
    val a = airSelected
      .withColumn("event_ts_sec", unix_timestamp(col("event_ts")))
      .groupBy("lat", "lon", "event_ts_sec")
      .agg(
        avg("aqi").as("label_aqi"),
        avg("pm25").as("pm25"),
        avg("pm10").as("pm10"),
        avg("no2").as("no2"),
        avg("so2").as("so2"),
        avg("o3").as("o3"),
        avg("co").as("co"),
        avg("nh3").as("nh3"),
        first("data_provider").as("air_provider")
      ).alias("a")

    val withAq = f.join(
      a,
      expr("f.lat = a.lat AND f.lon = a.lon AND abs(f.event_ts_sec - a.event_ts_sec) == 1"),
      "left"
    ).drop("event_ts_sec")
      .drop(a("lat"))
      .drop(a("lon"))// drop helper column

    println("=== JoinPreprocessor: FEATURES + LABEL sample ===")
    withAq.show(10, truncate = false)

    withAq
  }

  /** Attach UV data to features, timestamps should already be aligned */
  def attachUv(features: DataFrame, uvAgg: DataFrame): DataFrame = {
    import features.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    val f = features.withColumn("event_ts_sec", unix_timestamp(col("event_ts"))).alias("f")
    val u = uvAgg
      .withColumn("event_ts_sec", unix_timestamp(col("event_ts")))
      .groupBy("lat", "lon", "event_ts_sec")
      .agg(
        avg("uv_index").as("uv_index"),
        first("data_provider").as("uv_provider")
      )
      .select(
        col("lat"),
        col("lon"),
        col("event_ts_sec"),
        col("uv_index"),
        col("uv_provider")
      )
      .alias("u")

    val withUv = f.join(
      u,
      expr("f.lat = u.lat AND f.lon = u.lon AND abs(f.event_ts_sec - u.event_ts_sec) == 1"),
      "left"
    ).drop(u("event_ts_sec"))
      .drop(u("lat"))
      .drop(u("lon"))

    println("=== JoinPreprocessor: FEATURES + UV sample ===")
    withUv.show(10, truncate = false)

    withUv
  }
}
