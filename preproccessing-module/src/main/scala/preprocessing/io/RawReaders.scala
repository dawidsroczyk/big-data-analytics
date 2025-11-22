package preprocessing.io

import org.apache.spark.sql.{DataFrame, SparkSession}

object RawReaders {

  def readWeatherRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/weather"
    println(s"[RawReaders] Reading WEATHER RAW from HDFS: $path")
    spark.read
      .format("avro")
      .load(path)
  }

  def readTrafficRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/traffic"
    println(s"[RawReaders] Reading TRAFFIC RAW from HDFS: $path")
    spark.read
      .format("avro")
      .load(path)
  }

  def readAirQualityRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/air_quality"
    println(s"[RawReaders] Reading AIR QUALITY RAW from HDFS: $path")
    spark.read
      .format("avro")
      .load(path)
  }
}
