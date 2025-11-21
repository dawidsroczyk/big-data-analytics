package preprocessing.spark

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

  def build(appName: String) : SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
  }

}
