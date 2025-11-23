package preprocessing.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object FeatureEngineeringPreprocessor {

  def enrich(featuresWithAQ: DataFrame): DataFrame = {

    val locTimeWindow = Window
      .partitionBy("lat", "lon")
      .orderBy(col("event_ts"))
      .rowsBetween(-3, 0)

    val locTimeOrdered = Window
      .partitionBy("lat", "lon")
      .orderBy(col("event_ts"))

    val enriched = featuresWithAQ
      .withColumn("pm25_roll_mean_4", avg("pm25").over(locTimeWindow))
      .withColumn("pm10_roll_mean_4", avg("pm10").over(locTimeWindow))
      .withColumn("aqi_lag_1", lag("label_aqi", 1).over(locTimeOrdered))
      .withColumn("aqi_lag_2", lag("label_aqi", 2).over(locTimeOrdered))
      .withColumn("aqi_delta_1", col("label_aqi") - col("aqi_lag_1"))
      .withColumn("temp_centered", col("temperature") - avg("temperature").over(locTimeWindow))
      .withColumn("humidity_centered", col("humidity") - avg("humidity").over(locTimeWindow))

    println("=== FeatureEngineeringPreprocessor: ENRICHED sample ===")
    enriched.show(10, truncate = false)

    enriched
  }
}
