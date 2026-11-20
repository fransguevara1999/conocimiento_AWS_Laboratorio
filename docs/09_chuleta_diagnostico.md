# 09 · Chuleta de diagnóstico: síntoma → causa → solución

| Síntoma (Spark UI / logs) | Causa probable | Solución |
|---|---|---|
| 1 task tarda 20× más que la mediana | **Skew** | AQE skewJoin, broadcast, salting, tratar clave caliente aparte |
| "Spill (Memory/Disk)" alto | Particiones demasiado grandes | Subir `shuffle.partitions`, workers con más memoria (G.2X), filtrar antes |
| `OutOfMemoryError` en **driver** | `collect()`, `toPandas()`, broadcast enorme, demasiados ficheros a listar | Escribir a S3, no recoger; bajar umbral de broadcast; compactar |
| `Container killed by YARN ... memory limits` | Overhead fuera del heap (PySpark, Arrow) | Subir `spark.executor.memoryOverhead` |
| Miles de tasks de milisegundos | Demasiadas particiones / small files | AQE coalesce, `coalesce`, `groupFiles` en Glue, compactación |
| Pocas tasks y cores ociosos | Pocas particiones (p.ej. tras `coalesce(1)` o gzip no divisible) | `repartition(n)`, formatos divisibles (Parquet, bzip2) |
| Mucho tiempo antes de que arranque el 1er stage | Listado de S3 con muchas particiones/ficheros, inferencia de esquema | Partition pushdown, esquema explícito, Iceberg (manifests) |
| `SortMergeJoin` con dimensión pequeña | Dimensión > umbral o sin estadísticas | `broadcast()` hint o subir `autoBroadcastJoinThreshold` |
| `BroadcastNestedLoopJoin` | Join sin igualdad | Reescribir con clave de igualdad o rangos discretizados |
| Mismo cálculo en varios jobs de la UI | Reutilización sin cache | `persist` + `unpersist` |
| GC time > 10% del task time | Executors enormes / muchos objetos | 4–5 cores por executor, menos UDFs, cache serializada |
| Escritura final muy lenta en S3 | Rename de committer clásico / muchas particiones | S3-optimized committer, Iceberg, `repartition` por columna de partición |
| Lectura JDBC con 1 sola task | Sin opciones de particionado | `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` |
| Streaming: lag creciente | Capacidad < entrada | Escalar, `maxOffsetsPerTrigger`, más shards/particiones |
| Streaming: estado crece sin parar | Sin watermark | `withWatermark`, RocksDB state store |
| Athena caro/lento sobre el lake | CSV, sin particiones, `SELECT *`, small files | Parquet, partition projection, columnas concretas, OPTIMIZE |

## Comandos útiles
```python
df.explain("formatted")                 # plan físico: busca Exchange, BroadcastHashJoin, PushedFilters
df.rdd.getNumPartitions()               # nº particiones actuales
spark.conf.get("spark.sql.shuffle.partitions")
df.groupBy(F.spark_partition_id()).count().show()   # tamaño por partición → detectar skew
df.groupBy("clave").count().orderBy(F.desc("count")).show(10)   # claves calientes
```
