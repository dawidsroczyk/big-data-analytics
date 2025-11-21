import org.apache.spark.sql.SparkSession
import preprocessing.io.RawReaders
import preprocessing.transform.{WeatherPreprocessor, TrafficPreprocessor, JoinPreprocessor}

object RunJoinTest extends App {

  val spark = SparkSession.builder()
    .appName("join-test")
    .master("local[*]")
    .getOrCreate()

  val rawBase = "data/raw"

  val weatherRaw = RawReaders.readWeatherRaw(spark, rawBase)
  val trafficRaw = RawReaders.readTrafficRaw(spark, rawBase)

  val weatherSilver = WeatherPreprocessor.transform(weatherRaw)
  val trafficSilver = TrafficPreprocessor.transform(trafficRaw)

  val joined = JoinPreprocessor.joinWeatherTraffic(weatherSilver, trafficSilver)

  joined.printSchema()
  joined.show(false)

  spark.stop()
}
