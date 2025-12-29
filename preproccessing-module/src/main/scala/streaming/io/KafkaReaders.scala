package streaming.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object KafkaReaders {

  def readJsonTopic(
                     spark: SparkSession,
                     bootstrap: String,
                     topic: String,
                     schema: StructType
                   ): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).as("r"))
      .select("r.*")
  }
}
