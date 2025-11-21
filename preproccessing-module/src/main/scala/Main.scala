import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Test")
      .master("local[*]") // uruchamia Spark lokalnie
      .getOrCreate()

    println("✅ SparkSession uruchomiony poprawnie!")

    // mały test – tworzymy prosty DataFrame
    val df = spark.createDataFrame(Seq(
      ("Warsaw", 5),
      ("Krakow", 4),
      ("Gdansk", 3)
    )).toDF("city", "aqi")

    df.show()

    spark.stop()
  }
}
