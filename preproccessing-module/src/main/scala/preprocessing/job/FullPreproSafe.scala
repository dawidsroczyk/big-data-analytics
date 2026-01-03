package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.transform.JoinPreprocessor
import preprocessing.transform.FeatureEngineeringPreprocessor

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object FullPreproSafe {

  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark: SparkSession = SparkSessionBuilder.build("FullPreprocessingJobSAFE")

    spark.sparkContext.setLogLevel("WARN")

    val weatherPath = s"${config.silverBasePath}/weather_clean"
    val trafficPath = s"${config.silverBasePath}/traffic_clean"
    val airPath     = s"${config.silverBasePath}/air_quality_clean"
    val uvPath      = s"${config.silverBasePath}/uv_clean"

    println(s"[FullPreprocessingJob] Reading SILVER weather from: $weatherPath")
    println(s"[FullPreprocessingJob] Reading SILVER traffic from: $trafficPath")
    println(s"[FullPreprocessingJob] Reading SILVER air quality from: $airPath")
    println(s"[FullPreprocessingJob] Reading SILVER UV from: $uvPath")

    val weatherSilver = spark.read.parquet(weatherPath)
    val trafficSilver = spark.read.parquet(trafficPath)
    val airSilver     = spark.read.parquet(airPath)
    val uvSilver      = spark.read.parquet(uvPath)

    // Join weather + traffic
    val features = JoinPreprocessor.joinWeatherTraffic(weatherSilver, trafficSilver)

    // Broadcast small datasets to avoid shuffle (air + uv)
    val featuresWithAQ = JoinPreprocessor.attachAirQuality(features, broadcast(airSilver))
    val featuresWithAQUv = JoinPreprocessor.attachUv(featuresWithAQ, broadcast(uvSilver))

    // Feature engineering
    val finalFeatures = FeatureEngineeringPreprocessor.enrich(featuresWithAQUv.dropDuplicates("lat", "lon", "event_ts"))

    val outputPath = s"${config.goldBasePath}/air_quality_features"
    println(s"[FullPreprocessingJob] Saving GOLD features to: $outputPath")

    // Use coalesce instead of repartition to reduce shuffle
    val featuresWithDate = finalFeatures
      .withColumn("event_date", to_date(col("event_ts")))
    featuresWithDate
      .coalesce(8) // adjust partition count according to dataset size
      .write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[FullPreprocessingJob] Done.")
    spark.stop()
  }
}
