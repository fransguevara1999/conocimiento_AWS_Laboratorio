"""
05 - Plantilla de job AWS Glue productivo (PySpark).

Principios: parametrizado por fecha de proceso, idempotente (re-run seguro desde
Control-M), lectura con pushdown de particiones, validación/conciliación con el
fichero de control, escritura sin small files y logs claros.

Parámetros del job:
  --JOB_NAME  --FEC_PROCESO 2026-11-20  --RUTA_RAW s3://...  --RUTA_MASTER s3://...
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "FEC_PROCESO", "RUTA_RAW", "RUTA_MASTER"])
sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
log = glue.get_logger()
job = Job(glue)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.adaptive.enabled", "true")

fec = args["FEC_PROCESO"]
ruta_dia = f"{args['RUTA_RAW']}/fec_proceso={fec}/"

# 1) Fichero de control (lo deja el emisor; el watcher de Control-M espera el .ok)
ctl = spark.read.json(f"{ruta_dia}_control.json").first()   # {"registros": N, "suma_importe": X}

# 2) Lectura con esquema explícito (sin inferencia)
schema = "id_mov long, cuenta_id string, cliente_id string, importe decimal(18,2), ts timestamp"
df = spark.read.schema(schema).option("header", "true").csv(f"{ruta_dia}*.csv")

# 3) Conciliación con el control: si no cuadra, el job FALLA (Control-M alerta y no promociona)
agg = df.agg(F.count("*").alias("n"), F.sum("importe").alias("s")).first()
if agg["n"] != ctl["registros"] or float(agg["s"] or 0) != float(ctl["suma_importe"]):
    raise ValueError(f"Conciliación KO {fec}: leidos={agg['n']}/{agg['s']} "
                     f"control={ctl['registros']}/{ctl['suma_importe']}")
log.info(f"Conciliación OK {fec}: {agg['n']} registros")

# 4) Calidad + dedup por clave de negocio (último por ts)
w = Window.partitionBy("id_mov").orderBy(F.col("ts").desc())
limpio = (df.filter(F.col("id_mov").isNotNull() & F.col("cuenta_id").isNotNull())
            .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
            .withColumn("fec_proceso", F.lit(fec).cast("date")))

# 5) Escritura idempotente: solo se reemplaza la partición del día, en pocos ficheros
(limpio.repartition("fec_proceso")
       .write.mode("overwrite").partitionBy("fec_proceso")
       .parquet(args["RUTA_MASTER"]))

job.commit()
