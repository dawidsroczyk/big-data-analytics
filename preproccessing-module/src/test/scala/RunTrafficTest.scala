import org.apache.spark.sql.SparkSession
import preprocessing.io.RawReaders
import preprocessing.transform.TrafficPreprocessor

object RunTrafficTest extends App {

  val spark = SparkSession.builder()
    .appName("traffic-test")
    .master("local[*]")
    .getOrCreate()

  val rawBase = "data/raw"

  val trafficRaw = RawReaders.readTrafficRaw(spark, rawBase)
  val trafficSilver = TrafficPreprocessor.transform(trafficRaw)

  trafficSilver.printSchema()
  trafficSilver.show(false)

  spark.stop()
}
