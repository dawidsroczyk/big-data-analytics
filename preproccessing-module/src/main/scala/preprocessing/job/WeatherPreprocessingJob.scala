package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.io.RawReaders
import preprocessing.transform.WeatherPreprocessor

import org.apache.spark.sql.functions._

object WeatherPreprocessingJob {

  def main(args: Array[String]) : Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark = SparkSessionBuilder.build("WeatherPreprocessingJob")

    spark.sparkContext.setLogLevel("WARN")

    val raw = RawReaders.readWeatherRaw(spark, config.rawBasePath)
    val silver = WeatherPreprocessor.transform(raw)

    val outputPath = s"${config.silverBasePath}/weather_clean"

    println(s"[WeatherPreprocessingJob] Saving SILVER weather to: $outputPath")

    silver
      .repartition(col("event_date"))
      .write
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[WeatherPreprocessingJob] Done.")
    spark.stop()

  }

}
