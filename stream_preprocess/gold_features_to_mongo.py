import os
import shutil
import time

# ML imports
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, split, trim,
    to_timestamp, regexp_replace, coalesce,
    to_date, hour, expr, concat_ws,
    round as sround, date_trunc
)
from pyspark.sql.functions import floor, unix_timestamp, timestamp_seconds

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka1:9092,kafka2:9092")
CHK_BASE = "/tmp/checkpoints_instrumented_v1" # Internal path for speed

# TUNING
WM = os.getenv("WM", "60 minutes") 
JOIN_RANGE_SECONDS = int(os.getenv("JOIN_RANGE", "60"))
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

# InfluxDB Config
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "my-super-secret-auth-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "air_quality_metrics")

BUCKET_UNIT = os.getenv("BUCKET_UNIT", "minute")
GEO_PRECISION = int(os.getenv("GEO_PRECISION", "3"))

# Paths to RF model
RF_BASE = "/gold/models"
RF_MODEL_PATH = f"{RF_BASE}/aqi_rf"
RF_PARAMS_PATH = f"{RF_BASE}/aqi_rf_params"


# --- KAFKA & SPARK HELPERS ---
def read_kafka_json(spark, topic, schema_ddl):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000) 
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

'''
def quick_dedup(df, ts_col="event_ts"):
    return (
        df.withColumn("bucket", date_trunc("minute", col(ts_col)))
          .withWatermark(ts_col, WM)
          .dropDuplicates(["geo_key", "bucket"])
          .drop("bucket")
    )
'''
def quick_dedup(df, ts_col="event_ts"):
    return (
        df.withColumn("bucket", 
            timestamp_seconds(
                (floor(unix_timestamp(col(ts_col)) / 10) * 10).cast("long")
            )
        )
        .withWatermark(ts_col, WM)
        .dropDuplicates(["geo_key", "bucket"])
        .drop("bucket")
    )

# --- RF prediction helpers ---
'''
def load_rf_model_and_features(spark: SparkSession):
    print(f"[RF] WARNING: Using DUMMY model for speed optimization.")
    return None, []
'''
def load_rf_model_and_features(spark: SparkSession):
    # params JSON produced by Scala job: features, feature_importances, num_trees, max_depth
    params_df = spark.read.json(RF_PARAMS_PATH)
    row = params_df.limit(1).collect()[0]
    feature_cols = list(row["features"])
    model = RandomForestRegressionModel.load(RF_MODEL_PATH)
    print(f"[RF] Loaded model from {RF_MODEL_PATH} with features: {feature_cols}")
    return model, feature_cols

'''
def add_prediction(df, model, feature_cols):
    return df.withColumn("predicted_aqi", lit(-1.0))
'''

def ensure_feature_columns(df, feature_cols):
    out = df
    for f in feature_cols:
        if f not in out.columns:
            out = out.withColumn(f, lit(0.0))
        else:
            out = out.withColumn(f, col(f).cast("double"))
    return out

def add_prediction(df, model, feature_cols):
    tmp = ensure_feature_columns(df, feature_cols)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled = assembler.transform(tmp)
    predicted = model.transform(assembled).withColumnRenamed("prediction", "predicted_aqi").drop("features")
    return predicted
# --- end RF helpers -

# --- PREPROCESSING ---
def weather_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("updated_at")))
           .withColumn("temperature", col("temperature").cast("double"))
           .withColumn("humidity", col("humidity").cast("double"))
           .withColumn("wind_speed", col("wind_speed").cast("double"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
    )
    return with_geo(df).select("lat","lon","geo_key","event_ts","conditions","temperature","humidity","wind_speed","data_provider")

def traffic_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("updated_at")))
           .withColumn("current_travel_time", col("current_travel_time").cast("long"))
           .withColumn("free_flow_travel_time", col("free_flow_travel_time").cast("long"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
    )
    return with_geo(df).select("geo_key","event_ts","current_travel_time","free_flow_speed","free_flow_travel_time","road_closure","data_provider")

def air_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("updated_at")))
           .withColumn("aqi", col("aqi").cast("double"))
           .withColumn("pm25", col("pm2_5").cast("double"))
           .withColumn("pm10", col("pm10").cast("double"))
           .withColumn("no2", col("no2").cast("double"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
    )
    return with_geo(df).select("geo_key","event_ts","aqi","pm25","pm10","no2","so2","o3","co","data_provider")

def uv_preprocess(raw):
    loc = split(col("location"), ",")
    df = (
        raw.withColumn("lat", loc.getItem(0).cast("double"))
           .withColumn("lon", trim(loc.getItem(1)).cast("double"))
           .withColumn("event_ts", parse_ts(col("timestamp")))
           .withColumn("uv_index", col("uv_index").cast("double"))
           .filter(col("event_ts").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
    )
    return with_geo(df).select("geo_key","event_ts","uv_index","data_provider")

def join_weather_traffic(weather, traffic):
    w = weather.withWatermark("event_ts", WM).alias("w")
    t = traffic.withWatermark("event_ts", WM).alias("t")
    return w.join(t, expr(f"w.geo_key = t.geo_key AND t.event_ts BETWEEN w.event_ts - INTERVAL {JOIN_RANGE_SECONDS} SECONDS AND w.event_ts + INTERVAL {JOIN_RANGE_SECONDS} SECONDS"), "inner") \
            .select(col("w.*"), col("t.current_travel_time"), col("t.free_flow_travel_time"), col("t.data_provider").alias("traffic_provider"))

def attach_air(features, air):
    f = features.withWatermark("event_ts", WM).alias("f")
    a = air.withWatermark("event_ts", WM).alias("a")
    return f.join(a, expr(f"f.geo_key = a.geo_key AND a.event_ts BETWEEN f.event_ts - INTERVAL {JOIN_RANGE_SECONDS} SECONDS AND f.event_ts + INTERVAL {JOIN_RANGE_SECONDS} SECONDS"), "left") \
            .select(col("f.*"), col("a.aqi").alias("label_aqi"), col("a.pm25"), col("a.pm10"), col("a.no2"), col("a.data_provider").alias("air_provider"))

def attach_uv(features, uv):
    f = features.withWatermark("event_ts", WM).alias("f")
    u = uv.withWatermark("event_ts", WM).alias("u")
    return f.join(u, expr(f"f.geo_key = u.geo_key AND u.event_ts BETWEEN f.event_ts - INTERVAL {JOIN_RANGE_SECONDS} SECONDS AND f.event_ts + INTERVAL {JOIN_RANGE_SECONDS} SECONDS"), "left") \
            .select(col("f.*"), col("u.uv_index"), col("u.data_provider").alias("uv_provider"))


# --- WRITER WITH DETAILED TIMERS ---
def write_partition_to_influx(iterator):
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS

    t_connect_start = time.time()
    
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=5000)
    
    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        print(f"   [Worker] Connection established in {time.time() - t_connect_start:.3f}s")
    except Exception as e:
        print(f"   [Worker] !!! Connection FAILED ({time.time() - t_connect_start:.3f}s): {e}")
        return

    batch = []
    count = 0
    t_process_start = time.time()

    try:
        for row in iterator:
            try:
                p = Point("environment_metrics") \
                    .time(row["event_ts"]) \
                    .tag("geo_key", str(row["geo_key"])) \
                    .tag("weather_provider", str(row["data_provider"] or "unknown")) \
                    .tag("traffic_provider", str(row["traffic_provider"] or "unknown")) \
                    .tag("is_congested", str(row["is_congested"]))

                if row["temperature"] is not None: p.field("temperature", float(row["temperature"]))
                if row["humidity"] is not None:    p.field("humidity", float(row["humidity"]))
                if row["wind_speed"] is not None:  p.field("wind_speed", float(row["wind_speed"]))
                
                if row["current_travel_time"] is not None: p.field("current_travel_time", int(row["current_travel_time"]))
                if row["congestion_index"] is not None:    p.field("congestion_index", float(row["congestion_index"]))
                
                if row["pm25"] is not None: p.field("pm25", float(row["pm25"]))
                if row["label_aqi"] is not None: p.field("label_aqi", float(row["label_aqi"]))
                if row["uv_index"] is not None: p.field("uv_index", float(row["uv_index"]))
                
                if row["predicted_aqi"] is not None: p.field("predicted_aqi", float(row["predicted_aqi"]))

                batch.append(p)
                count += 1

                if len(batch) >= 2000:
                    t_chunk = time.time()
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=batch)
                    print(f"   [Worker] Flushed 2000 records in {time.time() - t_chunk:.3f}s")
                    batch = []

            except Exception as e:
                print(f"   [Worker] Row Error: {e}")

        if batch:
            t_chunk = time.time()
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=batch)
            print(f"   [Worker] Flushed remaining {len(batch)} records in {time.time() - t_chunk:.3f}s")

    except Exception as e:
        print(f"   [Worker] Partition Fatal Error: {e}")
    finally:
        client.close()
        total_time = time.time() - t_connect_start
        print(f"   [Worker] Partition Complete. {count} rows in {total_time:.3f}s (Avg: {(count/total_time) if total_time > 0 else 0:.1f} rec/s)")


def main():
    spark = SparkSession.builder.appName("InstrumentedFeatures").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    
    try:
        spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    except: pass

    weather_ddl = "location STRING, temperature DOUBLE, conditions STRING, humidity DOUBLE, wind_speed DOUBLE, updated_at STRING, data_provider STRING"
    traffic_ddl = "location STRING, free_flow_speed DOUBLE, current_travel_time LONG, free_flow_travel_time LONG, road_closure BOOLEAN, updated_at STRING, data_provider STRING"
    air_ddl     = "location STRING, aqi LONG, co DOUBLE, no2 DOUBLE, o3 DOUBLE, pm2_5 DOUBLE, pm10 DOUBLE, so2 DOUBLE, updated_at STRING, data_provider STRING"
    uv_ddl      = "uv_index DOUBLE, location STRING, timestamp STRING, data_provider STRING"

    weather = weather_preprocess(read_kafka_json(spark, "raw_weather", weather_ddl))
    traffic = traffic_preprocess(read_kafka_json(spark, "raw_traffic", traffic_ddl))
    air     = air_preprocess(read_kafka_json(spark, "raw_air_quality", air_ddl))
    uv      = uv_preprocess(read_kafka_json(spark, "raw_uv", uv_ddl))

    weather_slim = quick_dedup(weather)
    traffic_slim = quick_dedup(traffic)
    air_slim     = quick_dedup(air)
    uv_slim      = quick_dedup(uv)

    wt = join_weather_traffic(weather_slim, traffic_slim)
    wta = attach_air(wt, air_slim)
    wtau = attach_uv(wta, uv_slim)

    rf_model, rf_features = load_rf_model_and_features(spark)
    chk_path = f"file:{CHK_BASE}"

    if os.path.exists(CHK_BASE):
        try: shutil.rmtree(CHK_BASE)
        except: pass

    def foreach_batch_process(batch_df, batch_id):
        t_start_batch = time.time()
        print(f"\n=== STARTING BATCH {batch_id} ===")

        t_calc_start = time.time()
        batch_df.persist() 
        count = batch_df.count()
        t_calc_end = time.time()
        
        print(f"1. [Computation Phase] Computed {count} joined records in {t_calc_end - t_calc_start:.3f}s")
        
        if count == 0:
            batch_df.unpersist()
            print(f"Batch empty. Skipped.")
            return

        enriched = (
            batch_df
            .withColumn("congestion_index",
                when(col("free_flow_travel_time") > 0,
                     col("current_travel_time") / col("free_flow_travel_time"))
                .otherwise(lit(None))
            )
            .withColumn("is_congested", when(col("congestion_index") > 1.2, lit(True)).otherwise(lit(False)))
        )
        final_df = add_prediction(enriched, rf_model, rf_features)
        
        print(f"2. [Write Phase] Starting Upload to InfluxDB...")
        t_write_start = time.time()
        
        final_df.coalesce(1).rdd.foreachPartition(write_partition_to_influx)
        
        t_write_end = time.time()
        print(f"2. [Write Phase] Upload Finished in {t_write_end - t_write_start:.3f}s")

        print(f"=== BATCH {batch_id} COMPLETE. Total Duration: {time.time() - t_start_batch:.3f}s ===")
        
        batch_df.unpersist()

    (
        wtau.writeStream
            .foreachBatch(foreach_batch_process)
            .option("checkpointLocation", chk_path)
            .outputMode("append")
            .trigger(processingTime="0 seconds") 
            .start()
            .awaitTermination()
    )

if __name__ == "__main__":
    main()