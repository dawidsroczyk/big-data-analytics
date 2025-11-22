package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.transform.JoinPreprocessor
import preprocessing.transform.FeatureEngineeringPreprocessor

import org.apache.spark.sql.functions._

object FullPreprocessingJob {

  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark  = SparkSessionBuilder.build("FullPreprocessingJob")

    spark.sparkContext.setLogLevel("WARN")

    val weatherPath = s"${config.silverBasePath}/weather_clean"
    val trafficPath = s"${config.silverBasePath}/traffic_clean"
    val airPath     = s"${config.silverBasePath}/air_quality_clean"

    println(s"[FullPreprocessingJob] Reading SILVER weather from: $weatherPath")
    println(s"[FullPreprocessingJob] Reading SILVER traffic from: $trafficPath")
    println(s"[FullPreprocessingJob] Reading SILVER air quality from: $airPath")

    val weatherSilver = spark.read.parquet(weatherPath)
    val trafficSilver = spark.read.parquet(trafficPath)
    val airSilver     = spark.read.parquet(airPath)

    val features       = JoinPreprocessor.joinWeatherTraffic(weatherSilver, trafficSilver)
    val featuresWithAQ = JoinPreprocessor.attachAirQuality(features, airSilver)

    val finalFeatures  = FeatureEngineeringPreprocessor.enrich(featuresWithAQ)

    val outputPath = s"${config.goldBasePath}/air_quality_features"

    println(s"[FullPreprocessingJob] Saving GOLD features to: $outputPath")

    finalFeatures
      .repartition(col("event_date"))
      .write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[FullPreprocessingJob] Done.")
    spark.stop()
  }
}
