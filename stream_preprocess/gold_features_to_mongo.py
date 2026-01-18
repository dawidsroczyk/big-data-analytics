import os
import time
import datetime

# ML imports
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, split, trim,
    to_timestamp, regexp_replace, coalesce,
    to_date, hour, expr, concat_ws,
    round as sround, date_trunc, row_number
)
from pyspark.sql.window import Window

# --- INFLUXDB IMPORTS ---
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092")
CHK_BASE = os.getenv("CHK_BASE", "/spark/checkpoints")
WM = os.getenv("WM", "6 hours")

JOIN_RANGE_SECONDS = int(os.getenv("JOIN_RANGE", "60"))
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

# InfluxDB Config
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "my-super-secret-auth-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "air_quality_metrics")

BUCKET_UNIT = os.getenv("BUCKET_UNIT", "minute")
GEO_PRECISION = int(os.getenv("GEO_PRECISION", "3"))
ENABLE_DEBUG = os.getenv("ENABLE_DEBUG", "0") == "1"

# Paths to RF model
RF_BASE = "/gold/models"
RF_MODEL_PATH = f"{RF_BASE}/aqi_rf"
RF_PARAMS_PATH = f"{RF_BASE}/aqi_rf_params"


# --- KAFKA & SPARK HELPERS (Unchanged) ---
def read_kafka_json(spark, topic, schema_ddl, starting_offsets=STARTING_OFFSETS):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json")
        .selectExpr(f"from_json(json, '{schema_ddl}') as r")
        .select("r.*")
    )

def parse_ts(col_str):
    fixed = regexp_replace(col_str, r"Z$", "+00:00")
    fixed = regexp_replace(fixed, r"(\+\d\d):(\d\d)$", r"\1\2")
    return coalesce(to_timestamp(fixed), to_timestamp(col_str))

def with_geo(df, precision=GEO_PRECISION):
    return df.withColumn(
        "geo_key",
        concat_ws(
            "#",
            sround(col("lat"), precision).cast("string"),
            sround(col("lon"), precision).cast("string"),
        )
    )

# --- RF PREDICTION HELPERS (Unchanged) ---
def load_rf_model_and_features(spark: SparkSession):
    '''
    params_df = spark.read.json(RF_PARAMS_PATH)
    row = params_df.limit(1).collect()[0]
    feature_cols = list(row["features"])
    model = RandomForestRegressionModel.load(RF_MODEL_PATH)
    print(f"[RF] Loaded model from {RF_MODEL_PATH} with features: {feature_cols}")
    '''
    print(f"[RF] WARNING: Using DUMMY model. Skipping load from {RF_MODEL_PATH}")
    return None, [] # Return None for model, empty list for features

def ensure_feature_columns(df, feature_cols):
    out = df
    for f in feature_cols:
        if f not in out.columns:
            out = out.withColumn(f, lit(0.0))
        else:
            out = out.withColumn(f, col(f).cast("double"))
    return out

def add_prediction(df, model, feature_cols):
    '''
    tmp = ensure_feature_columns(df, feature_cols)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled = assembler.transform(tmp)
    predicted = model.transform(assembled).withColumnRenamed("prediction", "predicted_aqi").drop("features")
    return predicted
    '''
    print("[RF] Injecting dummy prediction value -1.0")
    return df.withColumn("predicted_aqi", lit(-1.0))

def dedup_by_bucket(df, ts_col="event_ts"):
    return (
        df.withColumn("event_bucket", date_trunc(BUCKET_UNIT, col(ts_col)))
          .withWatermark(ts_col, WM)
          .dropDuplicates(["geo_key", "event_bucket"])
          .drop("event_bucket")
    )

# --- PREPROCESSING & JOINS (Unchanged) ---
def weather_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("updated_at")))
           .withColumn("event_date", to_date(col("event_ts")))
           .withColumn("event_hour", hour(col("event_ts")))
           .withColumn("temperature", col("temperature").cast("double"))
           .withColumn("humidity", col("humidity").cast("double"))
           .withColumn("wind_speed", col("wind_speed").cast("double"))
           .withColumn("conditions", col("conditions").cast("string"))
           .withColumn("data_provider", col("data_provider").cast("string"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
           .select("lat","lon","event_ts","event_date","event_hour","conditions","temperature","humidity","wind_speed","data_provider")
    )
    return with_geo(df)

def traffic_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("updated_at")))
           .withColumn("event_date", to_date(col("event_ts")))
           .withColumn("event_hour", hour(col("event_ts")))
           .withColumn("current_travel_time", col("current_travel_time").cast("long"))
           .withColumn("free_flow_speed", col("free_flow_speed").cast("double"))
           .withColumn("free_flow_travel_time", col("free_flow_travel_time").cast("long"))
           .withColumn("road_closure", col("road_closure").cast("boolean"))
           .withColumn("data_provider", col("data_provider").cast("string"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
           .select("lat","lon","event_ts","event_date","event_hour","current_travel_time","free_flow_speed","free_flow_travel_time","road_closure","data_provider")
    )
    return with_geo(df)

def air_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("updated_at")))
           .withColumn("event_date", to_date(col("event_ts")))
           .withColumn("event_hour", hour(col("event_ts")))
           .withColumn("aqi", col("aqi").cast("double"))
           .withColumn("pm25", col("pm2_5").cast("double"))
           .withColumn("pm10", col("pm10").cast("double"))
           .withColumn("no2", col("no2").cast("double"))
           .withColumn("so2", col("so2").cast("double"))
           .withColumn("o3", col("o3").cast("double"))
           .withColumn("co", col("co").cast("double"))
           .withColumn("nh3", lit(None).cast("double"))
           .withColumn("data_provider", col("data_provider").cast("string"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
           .select("lat","lon","event_ts","event_date","event_hour","aqi","pm25","pm10","no2","so2","o3","co","nh3","data_provider")
    )
    return with_geo(df)

def uv_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("timestamp")))
           .withColumn("event_date", to_date(col("event_ts")))
           .withColumn("event_hour", hour(col("event_ts")))
           .withColumn("uv_index", col("uv_index").cast("double"))
           .withColumn("data_provider", col("data_provider").cast("string"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
           .select("lat","lon","event_ts","event_date","event_hour","uv_index","data_provider")
    )
    return with_geo(df)

def join_weather_traffic(weather, traffic):
    w = weather.withWatermark("event_ts", WM).alias("w")
    t = traffic.withWatermark("event_ts", WM).alias("t")
    return (
        w.join(t, expr(f"w.geo_key = t.geo_key AND t.event_ts BETWEEN w.event_ts - INTERVAL {JOIN_RANGE_SECONDS} SECONDS AND w.event_ts + INTERVAL {JOIN_RANGE_SECONDS} SECONDS"), "inner")
        .select(
            col("w.lat"), col("w.lon"), col("w.geo_key"), col("w.event_ts"), col("w.event_date"), col("w.event_hour"),
            col("w.conditions"), col("w.temperature"), col("w.humidity"), col("w.wind_speed"), col("w.data_provider").alias("weather_provider"),
            col("t.current_travel_time"), col("t.free_flow_speed"), col("t.free_flow_travel_time"), col("t.road_closure"), col("t.data_provider").alias("traffic_provider"),
        )
    )

def attach_air(features, air):
    f = features.withWatermark("event_ts", WM).alias("f")
    a = air.withWatermark("event_ts", WM).alias("a")
    return (
        f.join(a, expr(f"f.geo_key = a.geo_key AND a.event_ts BETWEEN f.event_ts - INTERVAL {JOIN_RANGE_SECONDS} SECONDS AND f.event_ts + INTERVAL {JOIN_RANGE_SECONDS} SECONDS"), "left")
        .select(
            col("f.*"), col("a.aqi").alias("label_aqi"), col("a.pm25"), col("a.pm10"), col("a.no2"), col("a.so2"), col("a.o3"), col("a.co"), col("a.nh3"), col("a.data_provider").alias("air_provider"),
        )
    )

def attach_uv(features, uv):
    f = features.withWatermark("event_ts", WM).alias("f")
    u = uv.withWatermark("event_ts", WM).alias("u")
    return (
        f.join(u, expr(f"f.geo_key = u.geo_key AND u.event_ts BETWEEN f.event_ts - INTERVAL {JOIN_RANGE_SECONDS} SECONDS AND f.event_ts + INTERVAL {JOIN_RANGE_SECONDS} SECONDS"), "left")
        .select(col("f.*"), col("u.uv_index"), col("u.data_provider").alias("uv_provider"))
    )

# --- NEW: INFLUXDB WRITER ---
def write_batch_to_influx(batch_df, batch_id):
    """
    Writes a Spark batch to InfluxDB.
    """
    # 1. Setup Influx Client
    # Note: Creating a client per batch is acceptable for streaming intervals >5-10s.
    # For very high frequency, consider connection pooling or mapPartitions.
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # 2. Collect data to driver
    # Warning: If batch is massive (>100k rows), use rdd.mapPartitions to write in parallel
    rows = batch_df.collect()
    
    if not rows:
        client.close()
        return

    print(f"[InfluxDB] Processing batch {batch_id} with {len(rows)} records...")
    
    points = []
    for row in rows:
        try:
            # Timestamp: Spark Timestamp -> Python Datetime
            ts = row["event_ts"]
            
            # Create Point
            # Measurement name: 'environment_metrics'
            p = Point("environment_metrics") \
                .time(ts) \
                .tag("geo_key", str(row["geo_key"])) \
                .tag("weather_provider", str(row["weather_provider"] or "unknown")) \
                .tag("traffic_provider", str(row["traffic_provider"] or "unknown")) \
                .tag("is_congested", str(row["is_congested"]))

            # Optional Tags (only if they exist)
            if row["air_provider"]: p.tag("air_provider", str(row["air_provider"]))

            # Fields: Numeric data (Must handle None/Null)
            # Weather
            if row["temperature"] is not None: p.field("temperature", float(row["temperature"]))
            if row["humidity"] is not None:    p.field("humidity", float(row["humidity"]))
            if row["wind_speed"] is not None:  p.field("wind_speed", float(row["wind_speed"]))
            
            # Traffic
            if row["current_travel_time"] is not None: p.field("current_travel_time", int(row["current_travel_time"]))
            if row["congestion_index"] is not None:    p.field("congestion_index", float(row["congestion_index"]))
            if row["delay_seconds"] is not None:       p.field("delay_seconds", float(row["delay_seconds"]))
            
            # Air Quality (Labels & Features)
            if row["pm25"] is not None: p.field("pm25", float(row["pm25"]))
            if row["pm10"] is not None: p.field("pm10", float(row["pm10"]))
            if row["no2"] is not None:  p.field("no2", float(row["no2"]))
            if row["label_aqi"] is not None: p.field("label_aqi", float(row["label_aqi"]))
            
            # UV
            if row["uv_index"] is not None: p.field("uv_index", float(row["uv_index"]))

            # ML PREDICTION
            if row["predicted_aqi"] is not None: p.field("predicted_aqi", float(row["predicted_aqi"]))

            points.append(p)
            
        except Exception as e:
            print(f"Error creating point for row: {e}")

    # 3. Write in Bulk
    if points:
        try:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            print(f"[InfluxDB] Successfully wrote {len(points)} points.")
        except Exception as e:
            print(f"[InfluxDB] Write Failed: {e}")
    
    client.close()


def main():
    spark = (
        SparkSession.builder
        .appName("GoldFeaturesToInfluxJob")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

    # [DDL strings omitted for brevity, same as before]
    weather_ddl = "location STRING, temperature DOUBLE, conditions STRING, humidity DOUBLE, wind_speed DOUBLE, updated_at STRING, data_provider STRING"
    traffic_ddl = "location STRING, free_flow_speed DOUBLE, current_travel_time LONG, free_flow_travel_time LONG, road_closure BOOLEAN, updated_at STRING, data_provider STRING"
    air_ddl     = "location STRING, aqi LONG, co DOUBLE, no2 DOUBLE, o3 DOUBLE, pm2_5 DOUBLE, pm10 DOUBLE, so2 DOUBLE, updated_at STRING, data_provider STRING"
    uv_ddl      = "uv_index DOUBLE, location STRING, timestamp STRING, data_provider STRING"

    weather = weather_preprocess(read_kafka_json(spark, "raw_weather", weather_ddl))
    traffic = traffic_preprocess(read_kafka_json(spark, "raw_traffic", traffic_ddl))
    air     = air_preprocess(read_kafka_json(spark, "raw_air_quality", air_ddl))
    uv      = uv_preprocess(read_kafka_json(spark, "raw_uv", uv_ddl))

    # [Join Logic omitted, same as before]
    weather_slim = dedup_by_bucket(weather)
    traffic_slim = dedup_by_bucket(traffic)
    air_slim     = dedup_by_bucket(air)
    uv_slim      = dedup_by_bucket(uv)

    wt = join_weather_traffic(weather_slim, traffic_slim)
    wta = attach_air(wt, air_slim)
    wtau = attach_uv(wta, uv_slim)

    # Load RF model
    rf_model, rf_features = load_rf_model_and_features(spark)

    chk_path = f"file:{CHK_BASE}/gold_features_to_influx_py"

    def foreach_batch_process(batch_df, batch_id):
        batch_df = batch_df.persist()
        try:
            n = batch_df.count()
            if n == 0: return

            # Calculate Derived Metrics (Congestion, etc.)
            enriched = (
                batch_df
                .withColumn("key", col("geo_key"))
                .withColumn("congestion_index",
                    when(col("free_flow_travel_time") > 0,
                         col("current_travel_time") / col("free_flow_travel_time"))
                    .otherwise(lit(None))
                )
                .withColumn("delay_seconds",
                    when(col("free_flow_travel_time").isNotNull() & col("current_travel_time").isNotNull(),
                         col("current_travel_time") - col("free_flow_travel_time"))
                    .otherwise(lit(None))
                )
                .withColumn("is_congested", when(col("congestion_index") > 1.2, lit(True)).otherwise(lit(False)))
            )

            # ADD PREDICTION
            enriched_with_pred = add_prediction(enriched, rf_model, rf_features)

            # PUSH TO INFLUX
            write_batch_to_influx(enriched_with_pred, batch_id)

        except Exception as e:
            print("[Batch] FAILED:", str(e))
        finally:
            batch_df.unpersist()

    (
        wtau.writeStream
            .foreachBatch(foreach_batch_process)
            .option("checkpointLocation", chk_path)
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
            .awaitTermination()
    )


if __name__ == "__main__":
    main()