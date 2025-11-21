package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.io.RawReaders
import preprocessing.transform.TrafficPreprocessor

import org.apache.spark.sql.functions._

object TrafficPreprocessingJob {

  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark  = SparkSessionBuilder.build("TrafficPreprocessingJob")

    spark.sparkContext.setLogLevel("WARN")

    val raw    = RawReaders.readTrafficRaw(spark, config.rawBasePath)
    val silver = TrafficPreprocessor.transform(raw)

    val outputPath = s"${config.silverBasePath}/traffic_clean"

    println(s"[TrafficPreprocessingJob] Saving SILVER traffic to: $outputPath")

    silver
      .repartition(col("event_date"))
      .write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[TrafficPreprocessingJob] Done.")
    spark.stop()
  }
}
