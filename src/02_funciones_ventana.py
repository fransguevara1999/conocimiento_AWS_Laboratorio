"""
02 - Funciones de ventana: patrones de entrevista (ejecutable en local).

    python src/02_funciones_ventana.py
"""
from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.appName("ventanas").master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

movs = spark.createDataFrame(
    [
        ("C1", "2026-11-01 09:00:00", 500.0, 1),
        ("C1", "2026-11-01 09:00:30", -200.0, 2),
        ("C1", "2026-11-03 10:00:00", 500.0, 3),
        ("C2", "2026-11-01 12:00:00", 1000.0, 4),
        ("C2", "2026-11-02 12:00:00", -50.0, 5),
        ("C2", "2026-11-02 12:00:00", -50.0, 5),     # duplicado exacto (re-envío)
        ("C3", "2026-11-05 08:00:00", 300.0, 6),
    ],
    "cuenta_id string, ts string, importe double, id_mov int",
).withColumn("ts", F.to_timestamp("ts"))

# 1) Deduplicar: último registro por id_mov (desempate determinista)
w_dedup = Window.partitionBy("id_mov").orderBy(F.col("ts").desc())
movs = movs.withColumn("rn", F.row_number().over(w_dedup)).filter("rn = 1").drop("rn")

# 2) Varias métricas con la MISMA especificación de ventana -> un solo shuffle + sort
w_cta = Window.partitionBy("cuenta_id").orderBy("ts", "id_mov")
w_acum = w_cta.rowsBetween(Window.unboundedPreceding, Window.currentRow)

res = movs.select(
    "*",
    F.row_number().over(w_cta).alias("n_mov"),
    F.sum("importe").over(w_acum).alias("saldo_acumulado"),
    F.lag("ts").over(w_cta).alias("ts_anterior"),
    F.lead("importe").over(w_cta).alias("importe_siguiente"),
).withColumn(
    "seg_desde_anterior", F.col("ts").cast("long") - F.col("ts_anterior").cast("long")
).withColumn(
    "alerta_rapida", F.col("seg_desde_anterior") < 60     # dos operaciones en < 60 s
)
res.orderBy("cuenta_id", "ts").show(truncate=False)

# 3) rank vs dense_rank vs row_number
w_rank = Window.orderBy(F.col("importe").desc())   # sin partitionBy: OK solo porque son 6 filas
movs.select(
    "cuenta_id", "importe",
    F.row_number().over(w_rank).alias("row_number"),
    F.rank().over(w_rank).alias("rank"),
    F.dense_rank().over(w_rank).alias("dense_rank"),
).show()

# 4) Ventana temporal real: suma de los últimos 2 días por cuenta (rangeBetween en segundos)
w_2d = (Window.partitionBy("cuenta_id")
        .orderBy(F.col("ts").cast("long"))
        .rangeBetween(-2 * 86400, 0))
movs.withColumn("suma_ultimos_2_dias", F.sum("importe").over(w_2d)).orderBy("cuenta_id", "ts").show()

# 5) Alternativa sin ventana para "último valor por clave" (Spark >= 3.3): max_by
movs.groupBy("cuenta_id").agg(F.max_by("importe", "ts").alias("ultimo_importe")).show()

spark.stop()
