package preprocessing.io

import org.apache.spark.sql.{DataFrame, SparkSession}

object RawReaders {

  def readWeatherRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/weather"
    println(s"[RawReaders] Reading WEATHER RAW from HDFS (JSON): $path")
    spark.read
      .json(path)
  }

  def readTrafficRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/traffic"
    println(s"[RawReaders] Reading TRAFFIC RAW from HDFS (AVRO): $path")
    spark.read
      .format("avro")
      .load(path)
  }

  def readAirQualityRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/air_pollution"
    println(s"[RawReaders] Reading AIR POLLUTION RAW from HDFS (AVRO): $path")
    spark.read
      .format("avro")
      .load(path)
  }
  
  def readUvRaw(spark: SparkSession, rawBasePath: String): DataFrame = {
    val path = s"$rawBasePath/uv"
    println(s"[RawReaders] Reading UV RAW from HDFS (AVRO): $path")
    spark.read
      .format("avro")
      .load(path)
  }
}
