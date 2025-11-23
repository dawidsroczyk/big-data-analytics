import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import preprocessing.io.RawReaders
import preprocessing.transform.{WeatherPreprocessor, TrafficPreprocessor, AirQualityPreprocessor, UvPreprocessor}

object HdfsSmokeTest {

  def main(args: Array[String]): Unit = {
    println("=== HdfsSmokeTest: start ===")

    val config = PreprocessingConfig.fromEnv()
    println(s"[HdfsSmokeTest] RAW_BASE_PATH    = ${config.rawBasePath}")
    println(s"[HdfsSmokeTest] SILVER_BASE_PATH = ${config.silverBasePath}")
    println(s"[HdfsSmokeTest] GOLD_BASE_PATH   = ${config.goldBasePath}")

    val spark = SparkSessionBuilder.build("HdfsSmokeTest")
    spark.sparkContext.setLogLevel("WARN")

    try {
      // WEATHER RAW → SILVER preview
      println("\n=== WEATHER RAW ===")
      val weatherRaw = RawReaders.readWeatherRaw(spark, config.rawBasePath)
      weatherRaw.show(5, truncate = false)

      println("\n=== WEATHER SILVER (preview transform) ===")
      val weatherSilver = WeatherPreprocessor.transform(weatherRaw)
      weatherSilver.show(5, truncate = false)

      // TRAFFIC RAW → SILVER preview
      println("\n=== TRAFFIC RAW ===")
      val trafficRaw = RawReaders.readTrafficRaw(spark, config.rawBasePath)
      trafficRaw.show(5, truncate = false)

      println("\n=== TRAFFIC SILVER (preview transform) ===")
      val trafficSilver = TrafficPreprocessor.transform(trafficRaw)
      trafficSilver.show(5, truncate = false)

      // AIR POLLUTION RAW → SILVER preview
      println("\n=== AIR POLLUTION RAW ===")
      val airRaw = RawReaders.readAirQualityRaw(spark, config.rawBasePath)
      airRaw.show(5, truncate = false)

      println("\n=== AIR POLLUTION SILVER (preview transform) ===")
      val airSilver = AirQualityPreprocessor.transform(airRaw)
      airSilver.show(5, truncate = false)

      // UV RAW → SILVER preview
      println("\n=== UV RAW ===")
      val uvRaw = RawReaders.readUvRaw(spark, config.rawBasePath)
      uvRaw.show(5, truncate = false)

      println("\n=== UV SILVER (preview transform) ===")
      val uvSilver = UvPreprocessor.transform(uvRaw)
      uvSilver.show(5, truncate = false)

      println("\n=== HdfsSmokeTest: SUCCESS – wszystko się wczytało i przetworzyło ===")
    } catch {
      case e: Throwable =>
        println("\n=== HdfsSmokeTest: ERROR ===")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
