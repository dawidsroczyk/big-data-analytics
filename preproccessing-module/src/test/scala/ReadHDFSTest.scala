
import org.apache.spark.sql.SparkSession

object ReadHDFSTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark HDFS Test")
      .getOrCreate()

    val df = spark.read.csv("hdfs://localhost:8020/test/iris.csv")
    println(">>> Showing first 5 rows:")
    df.show(5, truncate = false)

    df.show()

    spark.stop()
  }
}
