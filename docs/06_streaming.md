# 06 · Streaming: conceptos y optimización

## 1. Batch vs micro-batch vs streaming real

| Modelo | Latencia | Ejemplo AWS |
|---|---|---|
| Batch | horas | Glue/EMR nocturno orquestado por Control-M |
| Micro-batch | segundos–minutos | **Spark Structured Streaming** (Glue Streaming, EMR) |
| Event-at-a-time | milisegundos | **Apache Flink** (Amazon Managed Service for Apache Flink), Lambda |

En banca: **fraude en tarjetas** y alertas → ms/segundos (Flink/Kinesis); **ingesta CDC al lake** →
minutos (Spark Streaming / Firehose); **reporting regulatorio** → batch.

## 2. Piezas en AWS

```
Productores (TPV, apps, CDC con DMS/Debezium)
   → Kinesis Data Streams  |  Amazon MSK (Kafka)          ← log duradero, particionado (shards/partitions)
   → Procesamiento: Glue Streaming / EMR Spark / Managed Flink / Lambda
   → Destinos: S3 (Iceberg) · Redshift (streaming ingestion) · DynamoDB · OpenSearch
   → Firehose: entrega gestionada a S3/Redshift/OpenSearch con buffering (sin código)
```

- **Kinesis Data Streams**: shards (1 MB/s o 1000 registros/s de entrada por shard), retención 24 h–365 días,
  orden garantizado **por partition key dentro del shard**.
- **MSK**: Kafka gestionado; topics, particiones, consumer groups, offsets.
- **Firehose**: sin gestionar consumidores; agrupa por tamaño/tiempo, convierte a Parquet, escribe Iceberg.

## 3. Spark Structured Streaming: modelo mental

Un stream es **una tabla infinita** a la que se le añaden filas. Escribes la misma query que en batch;
Spark la ejecuta **incrementalmente** en cada micro-batch.

```python
stream = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", brokers)
          .option("subscribe", "transacciones")
          .option("maxOffsetsPerTrigger", 500000)          # backpressure
          .load())
(resultado.writeStream
   .format("iceberg").outputMode("append")
   .option("checkpointLocation", "s3://lake/checkpoints/transacciones/")
   .trigger(processingTime="1 minute")
   .toTable("glue_catalog.lake.transacciones"))
```

### Conceptos clave
| Concepto | Qué es |
|---|---|
| **Trigger** | Cada cuánto corre un micro-batch: `processingTime="1 minute"`, `availableNow=True` (procesa lo pendiente y para → "batch incremental", ideal para orquestar con Control-M), `continuous` (experimental) |
| **Checkpoint** | Carpeta en S3 con **offsets** procesados + **estado**. Permite reanudar exactamente donde quedó. Una por query; **no se comparte ni se borra** |
| **Output mode** | `append` (solo filas nuevas finales), `update` (filas que cambian), `complete` (toda la tabla resultado) |
| **Event time vs processing time** | Hora en que ocurrió el evento vs hora en que llegó. Siempre agregar por **event time** |
| **Watermark** | Cuánto retraso tolero: `withWatermark("ts", "10 minutes")` → los eventos más tardíos se descartan y el **estado antiguo se purga** |
| **Estado** | Lo que Spark recuerda entre micro-batches (agregados, dedup, joins). Vive en el **state store** |

## 4. Ventanas de tiempo

```python
from pyspark.sql import functions as F
agg = (eventos
       .withWatermark("ts_evento", "10 minutes")
       .groupBy(F.window("ts_evento", "5 minutes"),          # tumbling
                "tarjeta_id")
       .agg(F.count("*").alias("n_ops"), F.sum("importe").alias("total")))
# sliding: F.window("ts", "10 minutes", "1 minute")   session: F.session_window("ts", "5 minutes")
```

Sin watermark, una agregación por ventana **guarda estado para siempre** → el estado crece hasta OOM.

## 5. Garantías: exactly-once

Spark consigue **exactly-once end-to-end** cuando:
1. La **fuente es reproducible** (Kafka/Kinesis con offsets).
2. El progreso se guarda en el **checkpoint**.
3. El **sink es idempotente o transaccional** (Iceberg/Delta, o MERGE por clave).

Con sinks no idempotentes (una API, una BD con INSERT) → at-least-once → **duplicados** si hay reintento.
Solución: `foreachBatch` + **MERGE** por clave de negocio + `batchId`.

```python
def upsert(batch_df, batch_id):
    (batch_df.dropDuplicates(["id_transaccion"])
             .createOrReplaceTempView("lote"))
    batch_df.sparkSession.sql("""
        MERGE INTO glue_catalog.lake.transacciones t
        USING lote s ON t.id_transaccion = s.id_transaccion
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")

stream.writeStream.foreachBatch(upsert).option("checkpointLocation", cp).start()
```

`foreachBatch` también permite: escribir a varios destinos, usar funciones de ventana analíticas
(`row_number`) sobre el micro-batch, y llamar a APIs batch.

## 6. Deduplicación en streaming

```python
(eventos.withWatermark("ts_evento", "1 hour")
        .dropDuplicatesWithinWatermark(["id_transaccion"]))   # Spark 3.5
```
Con `dropDuplicates(["id", "ts_evento"])` + watermark en versiones anteriores. Sin watermark → estado infinito.

## 7. Joins en streaming
- **Stream–static** (enriquecer con una dimensión): fácil; la dimensión se relee en cada micro-batch
  si es una tabla Delta/Iceberg (o haz broadcast si es pequeña).
- **Stream–stream**: requiere **watermark en ambos lados** y condición de tiempo
  (`ts_pago BETWEEN ts_autorizacion AND ts_autorizacion + interval 15 minutes`) para poder purgar estado.

## 8. Optimización de streaming (lo que preguntan a un senior)

| Problema | Causa | Solución |
|---|---|---|
| Micro-batch tarda más que el trigger → retraso creciente | Volumen > capacidad | Más executors/particiones, `maxOffsetsPerTrigger` / `maxFilesPerTrigger`, más shards/particiones en origen |
| **Small files** en S3 | Cada micro-batch escribe ficheros | Trigger más largo (1–5 min), compactación periódica (OPTIMIZE / rewrite_data_files), Firehose con buffering |
| Estado crece sin control → OOM | Agregaciones/dedup sin watermark | Watermark, TTL, `RocksDB` state store |
| GC alto por estado grande | State store en memoria JVM | `spark.sql.streaming.stateStore.providerClass` = RocksDB |
| Paralelismo bajo | Pocas particiones Kafka/shards | Aumentar particiones del topic; `minPartitions` en el reader de Kafka |
| Hot shard / partición caliente | Partition key mal elegida (ej: código de comercio grande) | Clave con más cardinalidad o con sufijo |
| Reprocesar tras un bug | — | Nuevo checkpoint + leer desde offset/timestamp; sink idempotente (MERGE) |
| Coste 24×7 de un cluster para datos que llegan cada hora | Streaming innecesario | `trigger(availableNow=True)` lanzado por Control-M: semántica streaming, coste batch |

Configuraciones típicas:
```python
spark.conf.set("spark.sql.shuffle.partitions", "64")    # en streaming no se ajusta con AQE igual que en batch;
                                                        # el nº queda FIJADO en el checkpoint
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
```
⚠️ `spark.sql.shuffle.partitions` en una query con estado **no puede cambiarse** después sin un checkpoint nuevo.

## 9. Monitorización
- `query.lastProgress` → `inputRowsPerSecond` vs `processedRowsPerSecond` (si procesas menos de lo que
  entra, acumulas lag), `batchDuration`, `stateOperators.numRowsTotal`.
- Kinesis: métrica **`GetRecords.IteratorAgeMilliseconds`** (retraso del consumidor). Kafka: **consumer lag**.
- Alarmas CloudWatch sobre lag y sobre fallos del job (Glue streaming reinicia; vigila reintentos).

## 10. Glue Streaming vs EMR vs Flink vs Lambda

| | Cuándo |
|---|---|
| **Glue Streaming** | Spark serverless, ingesta/ETL continuo a S3/Iceberg, equipo que ya sabe PySpark |
| **EMR (Spark Streaming)** | Control fino, cargas grandes y constantes, coste con instancias reservadas/Spot |
| **Managed Flink** | Latencia de ms, estado complejo, CEP (reglas de fraude), event-time estricto |
| **Lambda** | Transformaciones ligeras por evento, bajo volumen, sin estado |
| **Firehose** | Entregar a S3/Redshift sin código, con conversión a Parquet |

## Resumen

> "En streaming pienso en cuatro cosas: event time con watermark para acotar el estado, checkpoint en S3
> para reanudar, un sink idempotente (MERGE en Iceberg vía foreachBatch) para exactly-once, y control
> de small files con triggers razonables y compactación. Si la latencia que pide negocio es de minutos,
> uso trigger availableNow orquestado: semántica de streaming a coste de batch."
