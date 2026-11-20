# conocimiento_AWS_Laboratorio

Guía **conceptual y práctica de nivel senior** sobre Apache Spark y procesamiento de datos en AWS
(Glue, EMR, Athena, S3, Iceberg/Delta), pensada para entrevistas de **Data Engineer / Arquitecto de Datos**
(contexto banca: BBVA – plataforma ADA sobre AWS).

> Filosofía: primero **entender cómo funciona Spark por dentro**; todas las optimizaciones salen de ahí.
> Si sabes *dónde está el shuffle* y *cuántos ficheros escribes*, ya resolviste el 80% de los problemas.

## 📚 Índice

| # | Tema | Qué vas a poder explicar |
|---|------|--------------------------|
| 01 | [Arquitectura de Spark](docs/01_arquitectura_spark.md) | Driver, executors, jobs/stages/tasks, DAG, lazy evaluation, Catalyst, Tungsten, AQE, memoria |
| 02 | [Optimización de procesos pesados](docs/02_optimizacion_procesamiento_pesado.md) | Shuffle, skew, salting, broadcast, particionado, small files, pushdown, UDFs |
| 03 | [cache / persist / unpersist / coalesce / repartition / temporales](docs/03_cache_persist_coalesce_temporales.md) | Cuándo y por qué usar cada uno, vistas temporales, checkpoint |
| 04 | [Funciones de ventana](docs/04_funciones_ventana.md) | row_number, rank, lag/lead, acumulados, dedup, SCD, rendimiento |
| 05 | [Formatos de tabla: Z-ORDER, VACUUM, OPTIMIZE](docs/05_zorder_vacuum_formatos_tabla.md) | Delta Lake vs Iceberg, compactación, data skipping, time travel, borrado GDPR |
| 06 | [Streaming](docs/06_streaming.md) | Structured Streaming, watermarks, estado, exactly-once, Kinesis/MSK, foreachBatch + MERGE |
| 07 | [Spark en AWS: Glue y EMR](docs/07_spark_en_aws_glue_emr.md) | Workers/DPUs, configuración, EMR vs Glue, S3 committers, costes |
| 08 | [Preguntas de entrevista (senior)](docs/08_preguntas_entrevista.md) | Respuestas cortas para decir en voz alta |
| 09 | [Chuleta de diagnóstico](docs/09_chuleta_diagnostico.md) | Síntoma → causa → solución (Spark UI) |

## 🧪 Código de ejemplo (PySpark)

| Script | Qué muestra |
|--------|-------------|
| [src/01_optimizacion_batch.py](src/01_optimizacion_batch.py) | Broadcast, AQE, salting, persist/unpersist, coalesce al escribir |
| [src/02_funciones_ventana.py](src/02_funciones_ventana.py) | Deduplicación, ranking, acumulados, lag/lead, saldo móvil |
| [src/03_iceberg_delta_mantenimiento.py](src/03_iceberg_delta_mantenimiento.py) | MERGE, OPTIMIZE/Z-ORDER, VACUUM, expire snapshots |
| [src/04_streaming_kinesis_kafka.py](src/04_streaming_kinesis_kafka.py) | Streaming con watermark, ventanas, foreachBatch + MERGE idempotente |
| [src/05_glue_job_plantilla.py](src/05_glue_job_plantilla.py) | Plantilla de job Glue productivo (parámetros, logs, idempotencia) |

Ejecutar en local (para practicar):

```bash
pip install pyspark==3.5.1
python src/02_funciones_ventana.py
```

## 🗺️ Plan de estudio rápido (2 horas)

1. **20 min** → doc 01 (sabes dibujar driver/executors y explicar un shuffle).
2. **30 min** → docs 02 y 03 (las optimizaciones y el "por qué" de coalesce/unpersist).
3. **20 min** → doc 04 (ventanas: dedup y acumulados salen SIEMPRE en entrevistas).
4. **15 min** → doc 05 (Z-ORDER, VACUUM, Iceberg).
5. **20 min** → doc 06 (streaming).
6. **15 min** → doc 08 en voz alta.

## 🧠 Las 10 ideas que un senior repite siempre

1. **Lazy evaluation**: nada se ejecuta hasta una *acción*; Spark optimiza el plan completo.
2. **El shuffle es el enemigo**: red + disco + serialización. Minimízalo o hazlo una sola vez.
3. **Filtra y proyecta pronto** (predicate pushdown + column pruning) → menos datos en todo el DAG.
4. **Broadcast** a la tabla pequeña; nunca un shuffle join contra una dimensión de 50 MB.
5. **Skew**: una task tarda 10x más que el resto → AQE skew join o *salting*.
6. **Tamaño de fichero objetivo 128 MB–1 GB**; evita miles de ficheros pequeños (coalesce / compactación).
7. **cache solo si reutilizas** el DataFrame ≥2 veces, y **unpersist** al terminar.
8. **Evita UDFs Python**; usa funciones nativas (`pyspark.sql.functions`) → Catalyst las optimiza.
9. **Idempotencia**: overwrite dinámico por partición o MERGE; re-ejecutar no debe duplicar.
10. **Mide antes de optimizar**: `explain()`, Spark UI, métricas de stages; no adivines.
