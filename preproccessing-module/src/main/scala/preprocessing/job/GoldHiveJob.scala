package preprocessing.job
import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder

object GoldHiveJob {
  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark  = SparkSessionBuilder.build("GoldHiveJob")

    spark.sparkContext.setLogLevel("WARN")
    println("STARTING Hive gold gathering data job")

    val aqfPath = s"${config.goldBasePath}/air_quality_features"

    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS gold.enriched_air_traffic (
        lat DOUBLE,
        lon DOUBLE,
        event_ts TIMESTAMP,
        event_ts_sec BIGINT,
        label_aqi DOUBLE,
        pm25 DOUBLE,
        pm10 DOUBLE,
        temperature DOUBLE,
        humidity DOUBLE,
        free_flow_speed DOUBLE,
        free_flow_travel_time INT,
        current_travel_time INT,
        aqi_lag_1 DOUBLE,
        aqi_lag_2 DOUBLE,
        pm25_roll_mean_4 DOUBLE,
        pm10_roll_mean_4 DOUBLE,
        aqi_delta_1 DOUBLE,
        temp_centered DOUBLE,
        humidity_centered DOUBLE,
        free_flow_speed_roll_mean_4 DOUBLE,
        free_flow_travel_time_roll_mean_4 DOUBLE,
        current_travel_time_roll_mean_4 DOUBLE
      )
      PARTITIONED BY (event_date STRING)
      STORED AS PARQUET
      LOCATION '${aqfPath}'
    """)

    spark.sql("MSCK REPAIR TABLE gold.enriched_air_traffic")

    println("FINISHED")

    spark.stop()

  }
}
