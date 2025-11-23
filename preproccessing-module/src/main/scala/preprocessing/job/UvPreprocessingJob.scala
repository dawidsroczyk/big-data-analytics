package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.io.RawReaders
import preprocessing.transform.UvPreprocessor

import org.apache.spark.sql.functions._

object UvPreprocessingJob {

  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark  = SparkSessionBuilder.build("UvPreprocessingJob")

    spark.sparkContext.setLogLevel("WARN")

    val raw    = RawReaders.readUvRaw(spark, config.rawBasePath)
    val silver = UvPreprocessor.transform(raw)

    val outputPath = s"${config.silverBasePath}/uv_clean"

    println(s"[UvPreprocessingJob] Saving SILVER UV to HDFS: $outputPath")

    silver
      .repartition(col("event_date"))
      .write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(outputPath)

    println("[UvPreprocessingJob] Done.")
    spark.stop()
  }
}
