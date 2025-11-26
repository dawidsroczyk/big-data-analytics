package preprocessing.spark

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build(appName: String) : SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("spark://spark-master:7077")   // IMPORTANT: use Docker master
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
      .getOrCreate()
  }
  def build_original(appName: String) : SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      // zakładam, że w klastrze Spark ma już skonfigurowany HDFS (core-site.xml, hdfs-site.xml)
      .getOrCreate()
  }

}
