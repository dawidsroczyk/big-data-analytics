package streaming.schema

import org.apache.spark.sql.types._

object Schemas {

  val weatherRaw: StructType = new StructType()
    .add("location", StringType)
    .add("updated_at", StringType)
    .add("temperature", StringType)
    .add("humidity", StringType)
    .add("wind_speed", StringType)
    .add("conditions", StringType)
    .add("data_provider", StringType)

  val trafficRaw: StructType = new StructType()
    .add("location", StringType)
    .add("updated_at", StringType)
    .add("current_travel_time", StringType)
    .add("free_flow_speed", StringType)
    .add("free_flow_travel_time", StringType)
    .add("road_closure", StringType)
    .add("data_provider", StringType)

  val airRaw: StructType = new StructType()
    .add("location", StringType)
    .add("updated_at", StringType)
    .add("aqi", StringType)
    .add("pm2_5", StringType)
    .add("pm10", StringType)
    .add("no2", StringType)
    .add("so2", StringType)
    .add("o3", StringType)
    .add("co", StringType)
    .add("data_provider", StringType)

  val uvRaw: StructType = new StructType()
    .add("location", StringType)
    .add("timestamp", StringType)
    .add("uv_index", StringType)
    .add("data_provider", StringType)
}
