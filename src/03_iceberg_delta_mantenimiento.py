"""
03 - Mantenimiento de tablas Iceberg en AWS Glue / EMR (referencia conceptual).

Ejecutar como Glue job con:  --datalake-formats iceberg
y la configuración del catálogo Glue de abajo. Las sentencias Delta equivalentes
se incluyen como comentarios.
"""
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

WAREHOUSE = "s3://mi-lake-bbva-demo/warehouse/"   # <- ajustar
TABLA = "glue_catalog.lake_master.movimientos"
FEC_PROCESO = sys.argv[1] if len(sys.argv) > 1 else "2026-11-20"

spark = (
    SparkSession.builder.appName("iceberg_mantenimiento")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
    .getOrCreate()
)

# 1) Tabla Iceberg particionada por día (partición OCULTA: days(ts))
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLA} (
    id_mov      BIGINT,
    cuenta_id   STRING,
    cliente_id  STRING,
    importe     DECIMAL(18,2),
    ts          TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ts))
TBLPROPERTIES (
  'format-version'='2',
  'write.target-file-size-bytes'='268435456',          -- 256 MB
  'write.delete.mode'='merge-on-read',                  -- deletes rápidos (ficheros de borrado)
  'write.merge.mode'='merge-on-read',
  'history.expire.max-snapshot-age-ms'='604800000'      -- 7 días
)
""")

# 2) Carga idempotente del día con MERGE (re-ejecutar no duplica)
spark.read.parquet(f"s3://mi-lake-bbva-demo/raw/movimientos/fec_proceso={FEC_PROCESO}/") \
     .dropDuplicates(["id_mov"]).createOrReplaceTempView("staging_movs")

spark.sql(f"""
MERGE INTO {TABLA} t
USING staging_movs s
ON t.id_mov = s.id_mov
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
# Delta:  igual sintaxis MERGE INTO sobre tabla Delta

# 3) Compactación (bin-pack) solo de la partición recién cargada
spark.sql(f"""
CALL glue_catalog.system.rewrite_data_files(
  table   => 'lake_master.movimientos',
  where   => "ts >= TIMESTAMP '{FEC_PROCESO} 00:00:00'",
  options => map('min-input-files','5')
)
""")
# Delta:  OPTIMIZE lake_master.movimientos WHERE fec_proceso = '...'

# 4) Z-ORDER semanal por columnas de filtro de alta cardinalidad
if datetime.strptime(FEC_PROCESO, "%Y-%m-%d").weekday() == 6:   # domingo
    spark.sql("""
    CALL glue_catalog.system.rewrite_data_files(
      table      => 'lake_master.movimientos',
      strategy   => 'sort',
      sort_order => 'zorder(cliente_id, cuenta_id)'
    )
    """)
    # Delta:  OPTIMIZE lake_master.movimientos ZORDER BY (cliente_id, cuenta_id)

# 5) "VACUUM" en Iceberg = expire_snapshots + remove_orphan_files
limite = (datetime.strptime(FEC_PROCESO, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"""
CALL glue_catalog.system.expire_snapshots(
  table       => 'lake_master.movimientos',
  older_than  => TIMESTAMP '{limite}',
  retain_last => 5
)
""")
spark.sql(f"""
CALL glue_catalog.system.remove_orphan_files(
  table      => 'lake_master.movimientos',
  older_than => TIMESTAMP '{limite}'
)
""")
# Delta:  VACUUM lake_master.movimientos RETAIN 168 HOURS
# Athena: VACUUM lake_master.movimientos   /  OPTIMIZE ... REWRITE DATA USING BIN_PACK

# 6) Borrado GDPR de un cliente: DELETE lógico + expire_snapshots = borrado físico
# spark.sql(f"DELETE FROM {TABLA} WHERE cliente_id = 'CLI-123'")
# ...luego (tras la retención acordada con Legal) expire_snapshots + remove_orphan_files

# 7) Time travel para auditoría
spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {TABLA}.snapshots "
          "ORDER BY committed_at DESC").show(5, truncate=False)
# SELECT * FROM {TABLA} TIMESTAMP AS OF '2026-11-19 08:00:00'

spark.stop()
