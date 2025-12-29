package streaming.job

import streaming.io.KafkaReaders
import streaming.schema.Schemas
import streaming.config.StreamConfig
import streaming.io.HBaseWriter
import streaming.io.HBaseWriter.HBaseConfig
import preprocessing.transform.UvPreprocessor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import org.apache.hadoop.hbase.util.Bytes

object UvSilverToHBaseJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UvSilverToHBaseJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val raw = KafkaReaders.readJsonTopic(spark, StreamConfig.kafkaBootstrap, "raw_uv", Schemas.uvRaw)

    val silver = UvPreprocessor
      .transform(raw)
      .withWatermark("event_ts", "6 hours")
      .dropDuplicates("lat","lon","event_ts")

    val chkPath = s"${StreamConfig.chkBase}/silver_uv_to_hbase"
    val hcfg = HBaseConfig(StreamConfig.hbaseQuorum, StreamConfig.hbasePort, StreamConfig.hbaseZnodeParent)

    silver.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
      val latest = batch
          .withColumn("rn", row_number().over(Window.partitionBy("lat","lon").orderBy(col("event_ts").desc)))
          .filter(col("rn") === 1)
          .drop("rn")

        HBaseWriter.writeLatestRDD(
          df = latest,
          table = StreamConfig.silverTable,
          cf = StreamConfig.silverCf,
          rowKeyFn = (r: Row) => s"uv#${r.getAs[Double]("lat")}#${r.getAs[Double]("lon")}",
          cols = Seq(
            "event_ts" -> ((r: Row) => Bytes.toBytes(r.getAs[java.sql.Timestamp]("event_ts").getTime)),
            "uv_index" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("uv_index"))),
            "data_provider" -> ((r: Row) => HBaseWriter.bytesOrNullString(r.getAs[String]("data_provider")))
          ),
          hcfg = hcfg
        )
      }
      .option("checkpointLocation", chkPath)
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
