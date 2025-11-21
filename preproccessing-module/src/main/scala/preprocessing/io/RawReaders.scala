package preprocessing.io

import org.apache.spark.sql.{DataFrame, SparkSession}

object RawReaders {

  def readWeatherRaw(spark: SparkSession, rawBasePath: String) : DataFrame = {
    val path = s"$rawBasePath/weather/"
    println(s"[RawReaders] Reading weather RAW from: $path")
    spark.read
      .option("multiLine", "true")
      .json(path)
  }

  def readTrafficRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/traffic"
    println(s"[RawReaders] Reading traffic RAW from: $path")
    spark.read
      .option("multiLine", "true")
      .json(path)
  }
}
