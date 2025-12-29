package streaming.job

import streaming.io.KafkaReaders
import streaming.schema.Schemas
import streaming.config.StreamConfig
import streaming.io.HBaseWriter
import streaming.io.HBaseWriter.HBaseConfig
import preprocessing.transform.AirQualityPreprocessor
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.hbase.util.Bytes

object AirQualitySilverToHBaseJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AirQualitySilverToHBaseJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val raw = KafkaReaders.readJsonTopic(spark, StreamConfig.kafkaBootstrap, "raw_air_quality", Schemas.airRaw)

    val silver = AirQualityPreprocessor
      .transform(raw)
      .withWatermark("event_ts", "6 hours")
      .dropDuplicates("lat","lon","event_ts")

    val chkPath = s"${StreamConfig.chkBase}/silver_air_to_hbase"
    val hcfg = HBaseConfig(StreamConfig.hbaseQuorum, StreamConfig.hbasePort, StreamConfig.hbaseZnodeParent)

    silver.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val latest = batch
          .withColumn("rn", row_number().over(Window.partitionBy("lat","lon").orderBy(col("event_ts").desc)))
          .filter(col("rn") === 1)
          .drop("rn")

        HBaseWriter.writeLatestRDD(
          df = latest,
          table = StreamConfig.silverTable,
          cf = StreamConfig.silverCf,
          rowKeyFn = (r: Row) => s"air#${r.getAs[Double]("lat")}#${r.getAs[Double]("lon")}",
          cols = Seq(
            "event_ts" -> ((r: Row) => Bytes.toBytes(r.getAs[java.sql.Timestamp]("event_ts").getTime)),
            "aqi" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("aqi"))),
            "pm25" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("pm25"))),
            "pm10" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("pm10"))),
            "no2" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("no2"))),
            "so2" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("so2"))),
            "o3"  -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("o3"))),
            "co"  -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("co"))),
            "nh3" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("nh3"))),
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
