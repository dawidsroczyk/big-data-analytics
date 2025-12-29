package streaming.transform

import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import java.sql.{Date, Timestamp}

object StatefulFeatureEngineering {

  case class Base(
                   lat: Double,
                   lon: Double,
                   event_ts: Timestamp,
                   event_date: Date,
                   event_hour: Int,
                   label_aqi: java.lang.Double,
                   pm25: java.lang.Double,
                   pm10: java.lang.Double,
                   temperature: java.lang.Double,
                   humidity: java.lang.Double,
                   free_flow_speed: java.lang.Double,
                   free_flow_travel_time: java.lang.Long,
                   current_travel_time: java.lang.Long
                 )

  case class State(history: List[Base])

  case class Out(
                  lat: Double,
                  lon: Double,
                  event_ts: Timestamp,
                  event_date: Date,
                  event_hour: Int,
                  label_aqi: java.lang.Double,
                  pm25: java.lang.Double,
                  pm10: java.lang.Double,
                  temperature: java.lang.Double,
                  humidity: java.lang.Double,
                  free_flow_speed: java.lang.Double,
                  free_flow_travel_time: java.lang.Long,
                  current_travel_time: java.lang.Long,
                  event_ts_sec: Long,
                  aqi_lag_1: java.lang.Double,
                  aqi_lag_2: java.lang.Double,
                  aqi_delta_1: java.lang.Double,
                  pm25_roll_mean_4: java.lang.Double,
                  pm10_roll_mean_4: java.lang.Double,
                  free_flow_speed_roll_mean_4: java.lang.Double,
                  free_flow_travel_time_roll_mean_4: java.lang.Double,
                  current_travel_time_roll_mean_4: java.lang.Double,
                  temp_centered: java.lang.Double,
                  humidity_centered: java.lang.Double
                )

  implicit val baseEnc: Encoder[Base]   = Encoders.product[Base]
  implicit val stateEnc: Encoder[State] = Encoders.product[State]
  implicit val outEnc: Encoder[Out]     = Encoders.product[Out]

  private def meanD(xs: Seq[java.lang.Double]): java.lang.Double = {
    val v = xs.flatMap(x => Option(x).map(_.doubleValue()))
    if (v.isEmpty) null else java.lang.Double.valueOf(v.sum / v.size)
  }

  private def meanL(xs: Seq[java.lang.Long]): java.lang.Double = {
    val v = xs.flatMap(x => Option(x).map(_.longValue().toDouble))
    if (v.isEmpty) null else java.lang.Double.valueOf(v.sum / v.size)
  }

  def enrichStreaming(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val base = df
      .select(
        col("lat"), col("lon"), col("event_ts"), col("event_date"), col("event_hour"),
        col("label_aqi"), col("pm25"), col("pm10"),
        col("temperature"), col("humidity"),
        col("free_flow_speed"), col("free_flow_travel_time"), col("current_travel_time")
      )
      .as[Base]
      .withWatermark("event_ts", "2 hours")

    val keyed = base.map(b => (s"${b.lat},${b.lon}", b))(Encoders.tuple(Encoders.STRING, baseEnc))

    val out = keyed
      .groupByKey(_._1)(Encoders.STRING)
      .flatMapGroupsWithState[State, Out](
        outputMode = OutputMode.Append(),
        timeoutConf = GroupStateTimeout.EventTimeTimeout()
      ) { case (_, it, state) =>

        val prev = if (state.exists) state.get else State(Nil)

        // sortujemy po czasie zdarzenia
        val incoming: List[Base] =
          it.map(_._2).toList.sortBy(b => b.event_ts.getTime)

        var hist: List[Base] = prev.history

        val emitted: List[Out] = incoming.map { cur =>
          val newHist = (hist :+ cur).takeRight(4)

          val aqiLag1: java.lang.Double =
            hist.lastOption.flatMap(x => Option(x.label_aqi)).orNull

          val aqiLag2: java.lang.Double =
            hist.dropRight(1).lastOption.flatMap(x => Option(x.label_aqi)).orNull

          val pm25m: java.lang.Double = meanD(newHist.map(_.pm25))
          val pm10m: java.lang.Double = meanD(newHist.map(_.pm10))
          val ffsm: java.lang.Double  = meanD(newHist.map(_.free_flow_speed))
          val ffttm: java.lang.Double = meanL(newHist.map(_.free_flow_travel_time))
          val cttm: java.lang.Double  = meanL(newHist.map(_.current_travel_time))

          val aqiDelta: java.lang.Double =
            if (cur.label_aqi == null || aqiLag1 == null) null
            else java.lang.Double.valueOf(cur.label_aqi.doubleValue() - aqiLag1.doubleValue())

          val tempMean: java.lang.Double = meanD(newHist.map(_.temperature))
          val humMean: java.lang.Double  = meanD(newHist.map(_.humidity))

          val tempCentered: java.lang.Double =
            if (cur.temperature == null || tempMean == null) null
            else java.lang.Double.valueOf(cur.temperature.doubleValue() - tempMean.doubleValue())

          val humCentered: java.lang.Double =
            if (cur.humidity == null || humMean == null) null
            else java.lang.Double.valueOf(cur.humidity.doubleValue() - humMean.doubleValue())

          hist = newHist

          Out(
            cur.lat, cur.lon, cur.event_ts, cur.event_date, cur.event_hour,
            cur.label_aqi, cur.pm25, cur.pm10, cur.temperature, cur.humidity,
            cur.free_flow_speed, cur.free_flow_travel_time, cur.current_travel_time,
            cur.event_ts.getTime / 1000L,
            aqiLag1, aqiLag2, aqiDelta,
            pm25m, pm10m, ffsm, ffttm, cttm,
            tempCentered, humCentered
          )
        }

        if (hist.nonEmpty) {
          state.update(State(hist))
          state.setTimeoutTimestamp(hist.last.event_ts.getTime, "2 hours")
        }

        emitted.iterator
      }

    out.toDF()
  }
}
