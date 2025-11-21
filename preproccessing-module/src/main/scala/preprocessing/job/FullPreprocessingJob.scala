package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.transform.JoinPreprocessor

import org.apache.spark.sql.functions._

object FullPreprocessingJob {

  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark  = SparkSessionBuilder.build("FullPreprocessingJob")

    spark.sparkContext.setLogLevel("WARN")

    val weatherPath = s"${config.silverBasePath}/weather_clean"
    val trafficPath = s"${config.silverBasePath}/traffic_clean"

    println(s"[FullPreprocessingJob] Reading SILVER weather from: $weatherPath")
    println(s"[FullPreprocessingJob] Reading SILVER traffic from: $trafficPath")

    val weatherSilver = spark.read.parquet(weatherPath)
    val trafficSilver = spark.read.parquet(trafficPath)

    val joined = JoinPreprocessor.joinWeatherTraffic(weatherSilver, trafficSilver)

    val outputPath = s"${config.silverBasePath}/air_quality_features"

    println(s"[FullPreprocessingJob] Saving joined features to: $outputPath")

    joined
      .repartition(col("event_date"))
      .write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[FullPreprocessingJob] Done.")
    spark.stop()
  }
}

