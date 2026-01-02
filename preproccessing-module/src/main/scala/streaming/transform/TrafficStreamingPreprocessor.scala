package streaming.transform

import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import java.sql.Timestamp
import java.sql.Date

object TrafficStreamingPreprocessor {

  case class Parsed(
                     lat: Double,
                     lon: Double,
                     event_ts: Timestamp,
                     event_date: Date,
                     event_hour: Int,
                     current_travel_time: java.lang.Long,
                     free_flow_speed: java.lang.Double,
                     free_flow_travel_time: java.lang.Long,
                     road_closure: java.lang.Boolean,
                     data_provider: String
                   )

  case class State(lastCurrent: Option[Long], lastFftt: Option[Long])

  case class Out(
                  lat: Double,
                  lon: Double,
                  event_ts: Timestamp,
                  event_date: Date,
                  event_hour: Int,
                  current_travel_time: java.lang.Long,
                  free_flow_speed: java.lang.Double,
                  free_flow_travel_time: java.lang.Long,
                  road_closure: java.lang.Boolean,
                  data_provider: String,

                  current_travel_time_imputed: Long,
                  free_flow_travel_time_imputed: Long,
                  congestion_index: Double,
                  delay_seconds: Long,
                  is_congested: Boolean
                )

  implicit val parsedEnc: Encoder[Parsed] = Encoders.product[Parsed]
  implicit val stateEnc: Encoder[State]   = Encoders.product[State]
  implicit val outEnc: Encoder[Out]       = Encoders.product[Out]

  def transform(raw: DataFrame): DataFrame = {
    val spark = raw.sparkSession
    import spark.implicits._

    val locSplit = split(col("location"), ",")

    val base = raw
      .withColumn("lat", locSplit.getItem(0).cast("double"))
      .withColumn("lon", trim(locSplit.getItem(1)).cast("double"))
      .withColumn(
        "event_ts",
        to_timestamp(
          regexp_replace(col("updated_at"), "(\\.\\d{3})\\d+", "$1"),
          "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
      )
      .withColumn("event_date", to_date(col("event_ts")))
      .withColumn("event_hour", hour(col("event_ts")))
      .withColumn("current_travel_time", col("current_travel_time").cast("long"))
      .withColumn("free_flow_speed", col("free_flow_speed").cast("double"))
      .withColumn("free_flow_travel_time", col("free_flow_travel_time").cast("long"))
      .withColumn("road_closure", col("road_closure").cast("boolean"))
      .withColumn("data_provider", col("data_provider").cast("string"))
      .filter(col("event_ts").isNotNull && col("lat").isNotNull && col("lon").isNotNull)
      .filter(col("current_travel_time") >= 0 || col("current_travel_time").isNull)
      .filter(col("free_flow_travel_time") >= 0 || col("free_flow_travel_time").isNull)
      .filter(col("free_flow_speed") >= 0 || col("free_flow_speed").isNull)
      .select(
        col("lat"), col("lon"), col("event_ts"),
        col("event_date"), col("event_hour"),
        col("current_travel_time"), col("free_flow_speed"), col("free_flow_travel_time"),
        col("road_closure"), col("data_provider")
      )
      .as[Parsed]
      .withWatermark("event_ts", "10 minutes")

    val keyed = base.map(p => (s"${p.lat},${p.lon}", p))(Encoders.tuple(Encoders.STRING, parsedEnc))

    val out = keyed
      .groupByKey(_._1)(Encoders.STRING)
      .flatMapGroupsWithState[State, Out](OutputMode.Append(), GroupStateTimeout.EventTimeTimeout()) {
        case (_, it, state) =>
          val prev = if (state.exists) state.get else State(None, None)
          val rows = it.map(_._2).toList.sortBy(_.event_ts.getTime)

          val outRows = rows.map { r =>
            val cttImp  = Option(r.current_travel_time).map(_.longValue()).orElse(prev.lastCurrent).getOrElse(0L)
            val ffttImp = Option(r.free_flow_travel_time).map(_.longValue()).orElse(prev.lastFftt).getOrElse(0L)

            val cong  = if (ffttImp <= 0L) 0.0 else cttImp.toDouble / ffttImp.toDouble
            val delay = cttImp - ffttImp

            Out(
              r.lat, r.lon, r.event_ts, r.event_date, r.event_hour,
              r.current_travel_time, r.free_flow_speed, r.free_flow_travel_time,
              r.road_closure, r.data_provider,
              cttImp, ffttImp, cong, delay, cong > 1.2
            )
          }

          outRows.lastOption.foreach { last =>
            state.update(State(Some(last.current_travel_time_imputed), Some(last.free_flow_travel_time_imputed)))
            state.setTimeoutTimestamp(last.event_ts.getTime, "10 minutes")
          }

          outRows.iterator
      }
      .dropDuplicates("lat", "lon", "event_ts")

    out.toDF()
  }
}
