"""
04 - Structured Streaming: watermark, ventanas de tiempo, dedup y
foreachBatch + MERGE idempotente (exactly-once hacia Iceberg).

Demo local: usa la fuente 'rate' (genera filas) para poder ejecutarlo sin Kafka.
En AWS se cambia la fuente por Kafka (MSK) o Kinesis (conector de Glue/EMR).

    python src/04_streaming_kinesis_kafka.py
"""
import os
import tempfile

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("streaming_demo").master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")     # en streaming queda fijado en el checkpoint
    # Estado grande -> RocksDB en lugar de memoria JVM:
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
base = tempfile.mkdtemp()

# ---------------------------------------------------------------------------
# FUENTE
# Producción (MSK / Kafka):
#   raw = (spark.readStream.format("kafka")
#          .option("kafka.bootstrap.servers", BROKERS)
#          .option("subscribe", "transacciones")
#          .option("startingOffsets", "latest")
#          .option("maxOffsetsPerTrigger", 500000)        # backpressure
#          .load()
#          .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d")).select("d.*"))
# ---------------------------------------------------------------------------
raw = spark.readStream.format("rate").option("rowsPerSecond", 50).load()
eventos = raw.select(
    (F.col("value") % 500).alias("id_transaccion"),          # % 500 -> genera duplicados a propósito
    F.concat(F.lit("T"), (F.col("value") % 20)).alias("tarjeta_id"),
    (F.rand() * 300).alias("importe"),
    F.col("timestamp").alias("ts_evento"),
)

# 1) Dedup con estado acotado por watermark
dedup = (eventos.withWatermark("ts_evento", "10 minutes")
                .dropDuplicates(["id_transaccion", "ts_evento"]))

# 2) Agregación por ventana de EVENT TIME (tumbling 1 min) -> señal de fraude simple
por_ventana = (
    eventos.withWatermark("ts_evento", "2 minutes")
    .groupBy(F.window("ts_evento", "1 minute"), "tarjeta_id")
    .agg(F.count("*").alias("n_ops"), F.round(F.sum("importe"), 2).alias("total"))
    .withColumn("sospechosa", F.col("n_ops") > 3)
)

q1 = (por_ventana.writeStream.outputMode("update").format("console")
      .option("truncate", False)
      .option("checkpointLocation", os.path.join(base, "cp_ventanas"))
      .trigger(processingTime="10 seconds")
      .start())


# 3) foreachBatch: sink idempotente. En AWS el cuerpo sería un MERGE INTO sobre Iceberg.
def upsert_lote(batch_df, batch_id):
    lote = batch_df.dropDuplicates(["id_transaccion"])
    # Producción:
    # lote.createOrReplaceTempView("lote")
    # lote.sparkSession.sql("""
    #   MERGE INTO glue_catalog.lake.transacciones t
    #   USING lote s ON t.id_transaccion = s.id_transaccion
    #   WHEN MATCHED THEN UPDATE SET *
    #   WHEN NOT MATCHED THEN INSERT *""")
    (lote.coalesce(1)                       # menos small files por micro-batch
         .write.mode("append").parquet(os.path.join(base, "sink", f"batch={batch_id}")))
    print(f"batch {batch_id}: {lote.count()} filas")


q2 = (dedup.writeStream.foreachBatch(upsert_lote)
      .option("checkpointLocation", os.path.join(base, "cp_sink"))
      .trigger(processingTime="15 seconds")    # trigger más largo = ficheros más grandes
      # .trigger(availableNow=True)            # "batch incremental" orquestado por Control-M
      .start())

# Monitorización: comparar entrada vs procesado (si procesas menos, acumulas lag)
q2.awaitTermination(45)
print("Último progreso:", q2.lastProgress and {
    k: q2.lastProgress[k] for k in ("batchId", "inputRowsPerSecond", "processedRowsPerSecond")})
q1.stop()
q2.stop()
spark.stop()
