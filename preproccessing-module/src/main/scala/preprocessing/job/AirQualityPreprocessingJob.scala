package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.io.RawReaders
import preprocessing.transform.AirQualityPreprocessor

import org.apache.spark.sql.functions._

object AirQualityPreprocessingJob {

  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark  = SparkSessionBuilder.build("AirQualityPreprocessingJob")

    spark.sparkContext.setLogLevel("WARN")

    val raw    = RawReaders.readAirQualityRaw(spark, config.rawBasePath)
    val silver = AirQualityPreprocessor.transform(raw)

    val outputPath = s"${config.silverBasePath}/air_quality_clean"

    println(s"[AirQualityPreprocessingJob] Saving SILVER air quality to HDFS: $outputPath")

    silver
      .repartition(col("event_date"))
      .write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[AirQualityPreprocessingJob] Done.")

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

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
      LOCATION 'hdfs://namenode:8020${outputPath}'
    """)

    spark.sql("MSCK REPAIR TABLE silver.air_quality_clean")
    spark.stop()
  }
}
