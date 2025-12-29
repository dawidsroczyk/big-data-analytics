package streaming.job

import streaming.io.KafkaReaders
import streaming.schema.Schemas
import streaming.config.StreamConfig
import streaming.io.HBaseWriter
import streaming.io.HBaseWriter.HBaseConfig
import streaming.transform.{TrafficStreamingPreprocessor, GoldStreamingBuilder, StatefulFeatureEngineering}

import preprocessing.transform.{WeatherPreprocessor, AirQualityPreprocessor, UvPreprocessor}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import org.apache.hadoop.hbase.util.Bytes

object GoldFeaturesToHBaseJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GoldFeaturesToHBaseJob").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val rawWeather = KafkaReaders.readJsonTopic(spark, StreamConfig.kafkaBootstrap, "raw_weather", Schemas.weatherRaw)
    val rawTraffic = KafkaReaders.readJsonTopic(spark, StreamConfig.kafkaBootstrap, "raw_traffic", Schemas.trafficRaw)
    val rawAir     = KafkaReaders.readJsonTopic(spark, StreamConfig.kafkaBootstrap, "raw_air_quality", Schemas.airRaw)
    val rawUv      = KafkaReaders.readJsonTopic(spark, StreamConfig.kafkaBootstrap, "raw_uv", Schemas.uvRaw)

    val weather = WeatherPreprocessor.transform(rawWeather)
      .withWatermark("event_ts", "2 hours")
      .dropDuplicates("lat","lon","event_ts")

    val traffic = TrafficStreamingPreprocessor.transform(rawTraffic) // ma watermark i dedup w środku

    val air = AirQualityPreprocessor.transform(rawAir)
      .withWatermark("event_ts", "6 hours")
      .dropDuplicates("lat","lon","event_ts")

    val uv = UvPreprocessor.transform(rawUv)
      .withWatermark("event_ts", "6 hours")
      .dropDuplicates("lat","lon","event_ts")

    val wt   = GoldStreamingBuilder.joinWeatherTraffic(weather, traffic)
    val wta  = GoldStreamingBuilder.attachAir(wt, air)
    val wtau = GoldStreamingBuilder.attachUv(wta, uv)

    val gold = StatefulFeatureEngineering.enrichStreaming(wtau)

    val chkPath = s"${StreamConfig.chkBase}/gold_features_to_hbase"
    val hcfg = HBaseConfig(StreamConfig.hbaseQuorum, StreamConfig.hbasePort, StreamConfig.hbaseZnodeParent)

    gold.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
      val latest = batch
          .withColumn("rn", row_number().over(Window.partitionBy("lat","lon").orderBy(col("event_ts").desc)))
          .filter(col("rn") === 1)
          .drop("rn")

        HBaseWriter.writeLatestRDD(
          df = latest,
          table = StreamConfig.goldTable,
          cf = StreamConfig.goldCf,
          rowKeyFn = (r: Row) => s"${r.getAs[Double]("lat")}#${r.getAs[Double]("lon")}",
          cols = Seq(
            "event_ts" -> ((r: Row) => Bytes.toBytes(r.getAs[java.sql.Timestamp]("event_ts").getTime)),
            "event_ts_sec" -> ((r: Row) => Bytes.toBytes(r.getAs[Long]("event_ts_sec"))),

            "label_aqi" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("label_aqi"))),
            "pm25" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("pm25"))),
            "pm10" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("pm10"))),
            "temperature" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("temperature"))),
            "humidity" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("humidity"))),

            "aqi_lag_1" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("aqi_lag_1"))),
            "aqi_lag_2" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("aqi_lag_2"))),
            "aqi_delta_1" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("aqi_delta_1"))),

            "pm25_roll_mean_4" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("pm25_roll_mean_4"))),
            "pm10_roll_mean_4" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("pm10_roll_mean_4"))),

            "free_flow_speed_roll_mean_4" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("free_flow_speed_roll_mean_4"))),
            "free_flow_travel_time_roll_mean_4" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("free_flow_travel_time_roll_mean_4"))),
            "current_travel_time_roll_mean_4" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("current_travel_time_roll_mean_4"))),

            "temp_centered" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("temp_centered"))),
            "humidity_centered" -> ((r: Row) => HBaseWriter.bytesOrNullDouble(r.getAs[java.lang.Double]("humidity_centered")))
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
