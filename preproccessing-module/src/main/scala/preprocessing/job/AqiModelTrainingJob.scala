package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

object AqiModelTrainingJob {
  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark: SparkSession = SparkSessionBuilder.build("AqiModelTrainingJob")
    spark.sparkContext.setLogLevel("WARN")

    // Parquet/Hive read robustness
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

    // Load GOLD parquet directly (bypass Hive SerDe)
    println("[AqiModelTrainingJob] Loading GOLD parquet directly, bypassing Hive SerDe")
    val goldPath = s"${config.goldBasePath}/air_quality_features"
    val baseDf = spark.read.option("mergeSchema", "true").parquet(goldPath)

    val safeDf = baseDf.select(
      col("temperature"),
      col("humidity"),
      col("current_travel_time").cast("long").alias("current_travel_time"),
      col("free_flow_speed"),
      col("free_flow_travel_time").cast("long").alias("free_flow_travel_time"),
      col("pm25"),
      col("pm10"),
      col("aqi_lag_1"),
      col("aqi_lag_2"),
      col("label_aqi")
    )

    val candidateFeatures = Seq(
      "temperature","humidity","current_travel_time","free_flow_speed",
      "free_flow_travel_time","pm25","pm10","aqi_lag_1","aqi_lag_2"
    )
    val featureCols = candidateFeatures.filter(safeDf.columns.contains)

    if (featureCols.nonEmpty && safeDf.columns.contains("label_aqi")) {
      println(s"[AqiModelTrainingJob] Training LinearRegression with features: ${featureCols.mkString(", ")}")

      val trainingDf = safeDf
        .na.drop("any", Seq("label_aqi"))
        .na.fill(0.0, featureCols)

      val assembler = new VectorAssembler()
        .setInputCols(featureCols.toArray)
        .setOutputCol("features")

      val assembled = assembler.transform(trainingDf)

      val lr = new LinearRegression()
        .setLabelCol("label_aqi")
        .setFeaturesCol("features")
        .setMaxIter(50)

      val model = lr.fit(assembled)
      val modelPath = s"${config.goldBasePath}/models/aqi_lr"

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val p  = new Path(modelPath)
      if (fs.exists(p)) {
        println(s"[AqiModelTrainingJob] Removing existing model at $modelPath")
        fs.delete(p, true)
      }
      println(s"[AqiModelTrainingJob] Saving model to $modelPath")
      model.save(modelPath)

      // Save model parameters (features order, coefficients, intercept) for Python job
      import spark.implicits._
      val coeffArr = model.coefficients.toArray
      val interceptVal = model.intercept
      val paramsPath = s"${config.goldBasePath}/models/aqi_lr_params"
      Seq((featureCols.toArray, coeffArr, interceptVal))
        .toDF("features","coeffs","intercept")
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(paramsPath)
    } else {
      println("[AqiModelTrainingJob] Skipping ML training (missing label_aqi or features).")
    }

    println("[AqiModelTrainingJob] Done.")
    spark.stop()
  }
}
