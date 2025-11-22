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
    spark.stop()
  }
}
