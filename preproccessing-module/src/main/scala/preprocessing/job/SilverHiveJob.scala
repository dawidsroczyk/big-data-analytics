package preprocessing.job
import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder

object SilverHiveJob {
  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark  = SparkSessionBuilder.build("SilverHiveJob")

    spark.sparkContext.setLogLevel("WARN")

    val aqPath = s"${config.silverBasePath}/air_quality_clean"
    val trafficPath = s"${config.silverBasePath}/traffic_clean"
    val weatherPath = s"${config.silverBasePath}/weather_clean"
    val uvPath = s"${config.silverBasePath}/uv_clean"
    println("STARTING Hive data gathering job")

    println("Gathering silver.air_quality_clean data")
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS silver.air_quality_clean (
        lat DOUBLE,
        lon DOUBLE,
        event_ts TIMESTAMP,
        event_hour INT,
        pm25 DOUBLE,
        pm10 DOUBLE,
        no2 DOUBLE,
        so2 DOUBLE,
        o3 DOUBLE,
        co DOUBLE,
        nh3 DOUBLE,
        aqi INT,
        data_provider STRING
      )
      PARTITIONED BY (event_date STRING)
      STORED AS PARQUET
      LOCATION '${aqPath}'
    """)
    spark.sql("MSCK REPAIR TABLE silver.air_quality_clean")

    println("Gathering silver.traffic_clean data")
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS silver.traffic_clean (
        location STRING,
        free_flow_speed DOUBLE,
        current_travel_time INT,
        free_flow_travel_time INT,
        road_closure BOOLEAN,
        updated_at STRING,
        data_provider STRING,
        lat DOUBLE,
        lon DOUBLE,
        event_ts TIMESTAMP,
        event_hour INT,
        current_travel_time_imputed INT,
        free_flow_travel_time_imputed INT,
        congestion_index DOUBLE,
        delay_seconds INT,
        is_congested BOOLEAN
      )
      PARTITIONED BY (event_date STRING)
      STORED AS PARQUET
      LOCATION '${trafficPath}'
    """)

    spark.sql("MSCK REPAIR TABLE silver.traffic_clean")

    println("Gathering silver.weather_clean data")
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS silver.weather_clean (
        conditions STRING,
        data_provider STRING,
        humidity DOUBLE,
        location STRING,
        temperature DOUBLE,
        updated_at STRING,
        wind_speed DOUBLE,
        lat DOUBLE,
        lon DOUBLE,
        event_ts TIMESTAMP,
        event_hour INT
      )
      PARTITIONED BY (event_date STRING)
      STORED AS PARQUET
      LOCATION '${weatherPath}'
    """)

    spark.sql("MSCK REPAIR TABLE silver.weather_clean")

    println("Hive - Gathering silver.uv_clean data")
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS silver.uv_clean (
        uv_index DOUBLE,
        location STRING,
        `timestamp` STRING,
        data_provider STRING,
        lat DOUBLE,
        lon DOUBLE,
        event_ts TIMESTAMP,
        event_hour INT
      )
      PARTITIONED BY (event_date STRING)
      STORED AS PARQUET
      LOCATION '${uvPath}'
    """)

    spark.sql("MSCK REPAIR TABLE silver.uv_clean")

    println("FINISHED")

    spark.stop()

  }
}
