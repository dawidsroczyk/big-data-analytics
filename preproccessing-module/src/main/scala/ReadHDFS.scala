import org.apache.spark.sql.SparkSession

object ReadHDFS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark HDFS Test")
      .master("spark://spark-master:7077")         // Docker Spark Master
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")   // Docker HDFS
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .csv("hdfs://namenode:8020/test/iris.csv")

    println(">>> FIRST 5 ROWS:")
    df.show(5, truncate = false)

    spark.stop()
  }
}
