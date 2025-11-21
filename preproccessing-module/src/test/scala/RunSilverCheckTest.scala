import org.apache.spark.sql.SparkSession

object RunSilverCheckTest extends App {

  val spark = SparkSession.builder()
    .appName("silver-check-test")
    .master("local[*]")
    .getOrCreate()

  val ws = spark.read.parquet("data/silver/weather_clean")
  val ts = spark.read.parquet("data/silver/traffic_clean")
  val joined = spark.read.parquet("data/silver/air_quality_features")

  println("=== WEATHER SILVER ===")
  ws.printSchema()
  ws.show(false)

  println("=== TRAFFIC SILVER ===")
  ts.printSchema()
  ts.show(false)

  println("=== JOINED FEATURES ===")
  joined.printSchema()
  joined.show(false)

  spark.stop()
}
