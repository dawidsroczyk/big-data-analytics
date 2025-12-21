package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object FeatureEngineeringPreprocessor {

  def enrich(features: DataFrame): DataFrame = {

    val base = features
      .select(
        "event_ts",
        "event_ts_sec",
        "label_aqi",
        "pm25",
        "pm10",
        "temperature",
        "humidity"
      )
      .orderBy("event_ts")

    val orderWindow = Window.orderBy(col("event_ts_sec"))
    val rollingWindow = orderWindow.rowsBetween(-3, 0)

    base
      .withColumn("aqi_lag_1", lag("label_aqi", 1).over(orderWindow))
      .withColumn("aqi_lag_2", lag("label_aqi", 2).over(orderWindow))
      .withColumn("pm25_roll_mean_4", avg("pm25").over(rollingWindow))
      .withColumn("pm10_roll_mean_4", avg("pm10").over(rollingWindow))
      .withColumn("aqi_delta_1", col("label_aqi") - col("aqi_lag_1"))
      .withColumn("temp_centered", col("temperature") - avg("temperature").over(rollingWindow))
      .withColumn("humidity_centered", col("humidity") - avg("humidity").over(rollingWindow))
  }


}
