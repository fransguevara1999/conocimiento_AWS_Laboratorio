"""
01 - Optimización batch en PySpark (ejecutable en local).

Muestra: AQE, broadcast join, detección de skew, salting, persist/unpersist,
coalesce vs repartition al escribir y overwrite dinámico idempotente.

    python src/01_optimizacion_batch.py
"""
import os
import tempfile

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("optimizacion_batch")
    .master("local[4]")
    .config("spark.sql.adaptive.enabled", "true")                     # AQE
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")    # idempotencia
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Datos sintéticos: movimientos con SKEW (el cliente 1 concentra el 40% de filas)
# ---------------------------------------------------------------------------
movs = (
    spark.range(0, 2_000_000)
    .withColumn(
        "cliente_id",
        F.when(F.rand(seed=1) < 0.4, F.lit(1)).otherwise((F.rand(seed=2) * 50_000).cast("int")),
    )
    .withColumn("oficina_id", (F.rand(seed=3) * 300).cast("int"))
    .withColumn("importe", F.round(F.rand(seed=4) * 1000 - 300, 2))
    .withColumn("fec_proceso", F.expr("date_add(date'2026-11-01', cast(rand(5) * 5 as int))"))
)
oficinas = spark.range(0, 300).select(
    F.col("id").cast("int").alias("oficina_id"),
    F.concat(F.lit("OF-"), F.col("id")).alias("nombre_oficina"),
)

# 1) Detectar skew: claves calientes
print("== Top claves (skew) ==")
movs.groupBy("cliente_id").count().orderBy(F.desc("count")).show(3)

# 2) Broadcast join: la dimensión pequeña viaja a cada executor, el lado grande NO se mueve
enriquecido = movs.join(F.broadcast(oficinas), "oficina_id")
print("== Plan con BroadcastHashJoin (sin Exchange en el lado grande) ==")
enriquecido.explain("formatted")

# 3) Salting para una agregación con skew (agregación en dos fases)
N_SALT = 8
parcial = (
    movs.withColumn("salt", (F.rand(seed=6) * N_SALT).cast("int"))
    .groupBy("cliente_id", "salt").agg(F.sum("importe").alias("suma_parcial"))
)
saldos = parcial.groupBy("cliente_id").agg(F.sum("suma_parcial").alias("saldo"))

# 4) persist: 'enriquecido' se usa en DOS acciones -> evitar recalcular el join
enriquecido.persist(StorageLevel.MEMORY_AND_DISK)

salida = os.path.join(tempfile.mkdtemp(), "lake")

# Acción 1: agregado por oficina -> pocas filas -> coalesce(1) es aceptable (salida pequeña)
(enriquecido.groupBy("nombre_oficina").agg(F.sum("importe").alias("total"))
    .coalesce(1).write.mode("overwrite").parquet(f"{salida}/gold/total_oficina"))

# Acción 2: detalle particionado por fecha -> repartition por la columna de partición
# => 1 fichero por carpeta de fecha (en vez de 64 ficheros pequeños por fecha)
(enriquecido.repartition("fec_proceso")
    .write.mode("overwrite").partitionBy("fec_proceso")
    .parquet(f"{salida}/master/movimientos"))

# 5) unpersist: liberar memoria de storage para que la usen joins/shuffles posteriores
enriquecido.unpersist()

saldos.write.mode("overwrite").parquet(f"{salida}/gold/saldos")

# 6) Re-ejecución idempotente de UN día: overwrite dinámico solo pisa esa partición
reproceso = enriquecido.filter(F.col("fec_proceso") == F.lit("2026-11-02").cast("date"))
reproceso.write.mode("overwrite").partitionBy("fec_proceso").parquet(f"{salida}/master/movimientos")

print("Particiones en MASTER:", sorted(os.listdir(f"{salida}/master/movimientos")))
print("Salida en:", salida)
spark.stop()
