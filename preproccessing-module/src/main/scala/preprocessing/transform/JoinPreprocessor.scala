package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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
      """),
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

    println("=== JoinPreprocessor: JOINED schema ===")
    joined.show(10, truncate = false)

    joined
  }

}
