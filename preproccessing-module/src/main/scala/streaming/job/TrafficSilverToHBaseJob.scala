package streaming.job

import streaming.io.KafkaReaders
import streaming.schema.Schemas
import streaming.config.StreamConfig
import streaming.io.HBaseWriter
import streaming.io.HBaseWriter.HBaseConfig
import streaming.transform.TrafficStreamingPreprocessor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import org.apache.hadoop.hbase.util.Bytes

object TrafficSilverToHBaseJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TrafficSilverToHBaseJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val raw = KafkaReaders.readJsonTopic(spark, StreamConfig.kafkaBootstrap, "raw_traffic", Schemas.trafficRaw)

    val silver = TrafficStreamingPreprocessor.transform(raw)

    val chkPath = s"${StreamConfig.chkBase}/silver_traffic_to_hbase"
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
          rowKeyFn = (r: Row) => s"traffic#${r.getAs[Double]("lat")}#${r.getAs[Double]("lon")}",
          cols = Seq(
            "event_ts" -> ((r: Row) => Bytes.toBytes(r.getAs[java.sql.Timestamp]("event_ts").getTime)),
            "current_travel_time_imputed" -> ((r: Row) => Bytes.toBytes(r.getAs[Long]("current_travel_time_imputed"))),
            "free_flow_travel_time_imputed" -> ((r: Row) => Bytes.toBytes(r.getAs[Long]("free_flow_travel_time_imputed"))),
            "free_flow_speed" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("free_flow_speed"))),
            "congestion_index" -> ((r: Row) => Bytes.toBytes(r.getAs[Double]("congestion_index"))),
            "delay_seconds" -> ((r: Row) => Bytes.toBytes(r.getAs[Long]("delay_seconds"))),
            "is_congested" -> ((r: Row) => Bytes.toBytes(r.getAs[Boolean]("is_congested"))),
            "road_closure" -> ((r: Row) => Bytes.toBytes(Option(r.getAs[java.lang.Boolean]("road_closure")).exists(_.booleanValue()))),
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
