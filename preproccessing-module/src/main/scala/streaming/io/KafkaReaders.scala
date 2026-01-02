package streaming.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.SchemaConverters

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileStream, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

object KafkaReaders {

  def readJsonTopic(
                     spark: SparkSession,
                     bootstrap: String,
                     topic: String,
                     schema: StructType
                   ): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).as("r"))
      .select("r.*")
  }

  def readAvroOcfTopic(
                        spark: SparkSession,
                        bootstrap: String,
                        topic: String,
                        avroSchemaJson: String
                      ): DataFrame = {

    val avroSchema = new Schema.Parser().parse(avroSchemaJson)

    val sparkSchema: StructType =
      SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

    val ocfToJson = udf { bytes: Array[Byte] =>
      if (bytes == null || bytes.isEmpty) null
      else {
        val reader = new GenericDatumReader[GenericRecord](avroSchema)
        var dfs: DataFileStream[GenericRecord] = null
        try {
          dfs = new DataFileStream[GenericRecord](new SeekableByteArrayInput(bytes), reader)
          if (dfs.hasNext) {
            val rec = dfs.next()
            rec.toString // Avro GenericRecord -> JSON
          } else null
        } catch {
          case _: Throwable => null
        } finally {
          if (dfs != null) try dfs.close() catch { case _: Throwable => () }
        }
      }
    }

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .select(ocfToJson(col("value")).as("json"))
      .filter(col("json").isNotNull)
      .select(from_json(col("json"), sparkSchema).as("r"))
      .select("r.*")
  }

  def readAvroTopic(
                     spark: SparkSession,
                     bootstrap: String,
                     topic: String,
                     avroSchemaJson: String
                   ): DataFrame = {
    import org.apache.spark.sql.avro.functions.from_avro

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .select(from_avro(col("value"), avroSchemaJson).as("r"))
      .select("r.*")
  }
}
