package preprocessing.transform

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object JoinPreprocessor {

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
                         AND     w.event_ts + INTERVAL 2 MINUTES
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

  /** GOLD – doklej label z air quality */
  def attachAirQuality(
                        features: DataFrame,
                        air: DataFrame
                      ): DataFrame = {
    val f = features.alias("f")
    val a = air.alias("a")

    val withLabel = f.join(
      a,
      expr(
        """
          f.lat = a.lat AND
          f.lon = a.lon AND
          a.event_ts BETWEEN f.event_ts - INTERVAL 2 MINUTES
                         AND     f.event_ts + INTERVAL 2 MINUTES
        """
      ),
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

    println("=== JoinPreprocessor: FEATURES + LABEL sample ===")
    withLabel.show(10, truncate = false)

    withLabel
  }

  /** GOLD – doklej UV */
  def attachUv(
                features: DataFrame,
                uv: DataFrame
              ): DataFrame = {
    val f = features.alias("f")
    val u = uv.alias("u")

    val withUv = f.join(
      u,
      expr(
        """
          f.lat = u.lat AND
          f.lon = u.lon AND
          u.event_ts BETWEEN f.event_ts - INTERVAL 2 MINUTES
                         AND     f.event_ts + INTERVAL 2 MINUTES
        """
      ),
      "left"
    ).select(
      col("f.*"),
      col("u.uv_index"),
      col("u.data_provider").as("uv_provider")
    )

    println("=== JoinPreprocessor: FEATURES + UV sample ===")
    withUv.show(10, truncate = false)

    withUv
  }
}
