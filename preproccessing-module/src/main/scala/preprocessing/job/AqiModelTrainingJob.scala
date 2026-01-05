package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

object AqiModelTrainingJob {
  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark: SparkSession = SparkSessionBuilder.build("AqiModelTrainingJob")
    spark.sparkContext.setLogLevel("WARN")

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

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
      println(s"[AqiModelTrainingJob] Training RandomForestRegressor with features: ${featureCols.mkString(", ")}")

      val trainingDf = safeDf
        .na.drop("any", Seq("label_aqi"))
        .na.fill(0.0, featureCols)

      val assembler = new VectorAssembler()
        .setInputCols(featureCols.toArray)
        .setOutputCol("features")

      val assembled = assembler.transform(trainingDf)

      val rf = new RandomForestRegressor()
        .setLabelCol("label_aqi")
        .setFeaturesCol("features")
        .setNumTrees(100)
        .setMaxDepth(10)
        .setFeatureSubsetStrategy("auto")

      val model = rf.fit(assembled)
      val modelPath = s"${config.goldBasePath}/models/aqi_rf"

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val p  = new Path(modelPath)
      if (fs.exists(p)) {
        println(s"[AqiModelTrainingJob] Removing existing model at $modelPath")
        fs.delete(p, true)
      }
      println(s"[AqiModelTrainingJob] Saving RandomForest model to $modelPath")
      model.save(modelPath)

      // Save model metadata (features order, feature importances, numTrees, maxDepth)
      import spark.implicits._
      val importances = model.featureImportances.toArray
      val paramsPath = s"${config.goldBasePath}/models/aqi_rf_params"
      Seq((featureCols.toArray, importances, model.getNumTrees, model.getMaxDepth))
        .toDF("features","feature_importances","num_trees","max_depth")
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
