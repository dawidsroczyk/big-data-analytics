package streaming.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object GoldStreamingBuilder {

  def joinWeatherTraffic(weather: DataFrame, traffic: DataFrame): DataFrame = {
    val w = weather.withWatermark("event_ts", "2 hours").alias("w")
    val t = traffic.withWatermark("event_ts", "10 minutes").alias("t")

    w.join(
      t,
      expr("""
        w.lat = t.lat AND
        w.lon = t.lon AND
        t.event_ts BETWEEN w.event_ts - INTERVAL 2 MINUTES AND w.event_ts + INTERVAL 2 MINUTES
      """),
      "inner"
    ).select(
      col("w.lat"),
      col("w.lon"),
      col("w.event_ts"),
      col("w.event_date"),
      col("w.event_hour"),

      col("w.conditions"),
      col("w.temperature"),
      col("w.humidity"),
      col("w.wind_speed"),
      col("w.data_provider").as("weather_provider"),

      col("t.current_travel_time_imputed").as("current_travel_time"),
      col("t.free_flow_speed"),
      col("t.free_flow_travel_time_imputed").as("free_flow_travel_time"),
      col("t.road_closure"),
      col("t.data_provider").as("traffic_provider")
    ).dropDuplicates("lat","lon","event_ts")
  }

  def attachAir(features: DataFrame, air: DataFrame): DataFrame = {
    val f = features.withWatermark("event_ts", "2 hours").alias("f")
    val a = air.withWatermark("event_ts", "6 hours").alias("a")

    f.join(
      a,
      expr("""
        f.lat = a.lat AND
        f.lon = a.lon AND
        a.event_ts BETWEEN f.event_ts - INTERVAL 2 MINUTES AND f.event_ts + INTERVAL 2 MINUTES
      """),
      "left"
    ).select(
      col("f.*"),
      col("a.aqi").as("label_aqi"),
      col("a.pm25"),
      col("a.pm10"),
      col("a.no2"),
      col("a.so2"),
      col("a.o3"),
      col("a.co"),
      col("a.nh3"),
      col("a.data_provider").as("air_provider")
    )
  }

  def attachUv(features: DataFrame, uv: DataFrame): DataFrame = {
    val f = features.withWatermark("event_ts", "2 hours").alias("f")
    val u = uv.withWatermark("event_ts", "6 hours").alias("u")

    f.join(
      u,
      expr("""
        f.lat = u.lat AND
        f.lon = u.lon AND
        u.event_ts BETWEEN f.event_ts - INTERVAL 2 MINUTES AND f.event_ts + INTERVAL 2 MINUTES
      """),
      "left"
    ).select(
      col("f.*"),
      col("u.uv_index"),
      col("u.data_provider").as("uv_provider")
    )
  }
}
