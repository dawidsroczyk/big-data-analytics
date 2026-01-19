package preprocessing.job

import preprocessing.config.PreprocessingConfig
import preprocessing.spark.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

object AqiModelTrainingJob {
  def main(args: Array[String]): Unit = {
    val config = PreprocessingConfig.fromEnv()
    val spark: SparkSession = SparkSessionBuilder.build("AqiModelTrainingJob")
    spark.sparkContext.setLogLevel("WARN")

    // Optimization configs
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", "false")

    println("[AqiModelTrainingJob] Loading GOLD parquet directly, bypassing Hive SerDe")
    val goldPath = s"${config.goldBasePath}/air_quality_features"
    val baseDf = spark.read.option("mergeSchema", "true").parquet(goldPath)

    // Select and cast columns
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
      println(s"[AqiModelTrainingJob] Preparing data with features: ${featureCols.mkString(", ")}")

      // 1. Clean Data
      val cleanedDf = safeDf
        .na.drop("any", Seq("label_aqi"))
        .na.fill(0.0, featureCols)

      // 2. Vector Assemble
      val assembler = new VectorAssembler()
        .setInputCols(featureCols.toArray)
        .setOutputCol("features")

      val assembled = assembler.transform(cleanedDf)

      // 3. Train / Test Split (80% Train, 20% Test)
      println("[AqiModelTrainingJob] Splitting data into Training (80%) and Test (20%) sets...")
      val Array(trainingData, testData) = assembled.randomSplit(Array(0.8, 0.2), seed = 42L)

      val rf = new RandomForestRegressor()
        .setLabelCol("label_aqi")
        .setFeaturesCol("features")
        .setNumTrees(100)
        .setMaxDepth(10)
        .setFeatureSubsetStrategy("auto")

      // 4. Fit model on TRAINING data only
      println(s"[AqiModelTrainingJob] Training RandomForestRegressor on ${trainingData.count()} rows...")
      val model = rf.fit(trainingData)

      // 5. Evaluate on TEST data
      println("[AqiModelTrainingJob] Evaluating model on Test set...")
      val predictions = model.transform(testData)

      val evaluator = new RegressionEvaluator()
        .setLabelCol("label_aqi")
        .setPredictionCol("prediction")

      // Calculate RMSE (Root Mean Squared Error)
      val rmse = evaluator.setMetricName("rmse").evaluate(predictions)
      // Calculate R2 (Coefficient of Determination)
      val r2 = evaluator.setMetricName("r2").evaluate(predictions)

      println(f"[AqiModelTrainingJob] Evaluation Results: RMSE = $rmse%.4f, R2 = $r2%.4f")

      // 6. Save Model
      val modelPath = s"${config.goldBasePath}/models/aqi_rf"
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val p  = new Path(modelPath)
      if (fs.exists(p)) {
        println(s"[AqiModelTrainingJob] Removing existing model at $modelPath")
        fs.delete(p, true)
      }
      println(s"[AqiModelTrainingJob] Saving RandomForest model to $modelPath")
      model.save(modelPath)

      // 7. Save Model Metadata (Now includes Evaluation Metrics)
      import spark.implicits._
      val importances = model.featureImportances.toArray
      val paramsPath = s"${config.goldBasePath}/models/aqi_rf_params"

      // Create a small DataFrame with metrics and params
      val metadataDf = Seq((
        featureCols.toArray, 
        importances, 
        model.getNumTrees, 
        model.getMaxDepth,
        rmse, // Added RMSE
        r2    // Added R2
      )).toDF("features", "feature_importances", "num_trees", "max_depth", "rmse", "r2")

      metadataDf
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(paramsPath)
      
      println(s"[AqiModelTrainingJob] Model parameters and metrics saved to $paramsPath")

    } else {
      println("[AqiModelTrainingJob] Skipping ML training (missing label_aqi or features).")
    }

    println("[AqiModelTrainingJob] Done.")
    spark.stop()
  }
}
