import org.apache.spark.sql.SparkSession
import preprocessing.io.RawReaders
import preprocessing.transform.WeatherPreprocessor

object RunWeatherTest extends App {

  val spark = SparkSession.builder()
    .appName("weather-test")
    .master("local[*]")
    .getOrCreate()

  val rawBase = "data/raw"

  val weatherRaw = RawReaders.readWeatherRaw(spark, rawBase)
  val weatherSilver = WeatherPreprocessor.transform(weatherRaw)

  println("=== WEATHER SILVER SCHEMA ===")
  weatherSilver.printSchema()

  println("=== WEATHER SILVER SAMPLE ===")
  weatherSilver.show(false)

  spark.stop()
}
