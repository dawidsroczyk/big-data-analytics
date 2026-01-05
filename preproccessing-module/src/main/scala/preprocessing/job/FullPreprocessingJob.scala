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
    val uvPath      = s"${config.silverBasePath}/uv_clean"   // ⬅⬅ NOWE

    println(s"[FullPreprocessingJob] Reading SILVER weather from: $weatherPath")
    println(s"[FullPreprocessingJob] Reading SILVER traffic from: $trafficPath")
    println(s"[FullPreprocessingJob] Reading SILVER air quality from: $airPath")
    println(s"[FullPreprocessingJob] Reading SILVER UV from: $uvPath")

    val weatherSilver = spark.read.parquet(weatherPath)
    val trafficSilver = spark.read.parquet(trafficPath)
    val airSilver     = spark.read.parquet(airPath)
    val uvSilver      = spark.read.parquet(uvPath)

    val features         = JoinPreprocessor.joinWeatherTraffic(weatherSilver, trafficSilver)
    val featuresWithAQ   = JoinPreprocessor.attachAirQuality(features, airSilver)
    val featuresWithAQUv = JoinPreprocessor.attachUv(featuresWithAQ, uvSilver)

    val finalFeatures  = FeatureEngineeringPreprocessor.enrich(featuresWithAQUv)

    val outputPath = s"${config.goldBasePath}/air_quality_features"

    println(s"[FullPreprocessingJob] Saving GOLD features to: $outputPath")

    finalFeatures
      .withColumn("event_date", to_date(col("event_ts")))
      .repartition(col("event_date"))
      .write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[FullPreprocessingJob] Done.")
    spark.stop()
  }
}
