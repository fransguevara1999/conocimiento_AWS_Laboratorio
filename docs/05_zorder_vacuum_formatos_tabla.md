# 05 · Formatos de tabla abiertos: Z-ORDER, OPTIMIZE, VACUUM

## 1. El problema del Parquet "a pelo"

Una carpeta de Parquet en S3 no tiene: transacciones ACID, UPDATE/DELETE/MERGE por fila,
control de esquema, historial ni lectura consistente mientras alguien escribe.

**Formatos de tabla** (capa de metadatos encima de Parquet):
- **Apache Iceberg** → el más soportado en AWS (Athena, Glue, EMR, Redshift, S3 Tables).
- **Delta Lake** → origen Databricks; funciona en Glue/EMR.
- **Apache Hudi** → fuerte en upserts/CDC (EMR, Glue).

Todos dan: **ACID, MERGE/UPDATE/DELETE, time travel, schema evolution, snapshots**.

### Cómo funciona por dentro (concepto)
- **Delta**: carpeta `_delta_log/` con ficheros JSON de commits (+ checkpoints Parquet). Cada commit dice
  qué ficheros Parquet se **añaden** y cuáles se **eliminan lógicamente**.
- **Iceberg**: `metadata.json` → *manifest list* (por snapshot) → *manifests* (lista de ficheros con
  estadísticas por columna) → ficheros de datos. El catálogo (Glue Data Catalog) apunta al metadata actual.

Clave: **un UPDATE/DELETE no modifica ficheros**; escribe ficheros nuevos y marca los viejos como
eliminados en el nuevo snapshot. Los viejos **siguen en S3** (para time travel) → de ahí nace VACUUM.

## 2. Data skipping: estadísticas min/max

Cada fichero guarda min/max por columna. Consulta `WHERE cliente_id = 123`:
- Si los datos están **desordenados**, cada fichero tiene min≈0 y max≈∞ → hay que leer **todos**.
- Si están **ordenados/agrupados** por `cliente_id`, solo 1–2 ficheros contienen el 123 → se salta el resto.

Particionar ayuda para 1 columna de baja cardinalidad (fecha). Para columnas de **alta cardinalidad**
(cliente, cuenta, tarjeta) se usa **ordenación / Z-ORDER / clustering**.

## 3. Z-ORDER

**Qué es**: técnica de ordenación multidimensional (curva de Morton / Z-curve) que **co-localiza** en los
mismos ficheros filas con valores cercanos en **varias columnas a la vez**.

**Por qué no basta un `ORDER BY a, b`**: ordenar por `a, b` agrupa muy bien `a`, pero `b` queda disperso
(solo ordenado dentro de cada `a`). Z-ORDER **intercala los bits** de `a` y `b` → ambos filtros
saltan una buena proporción de ficheros.

```
ORDER BY a,b → filtro por a: excelente | filtro por b: malo
ZORDER (a,b) → filtro por a: bueno     | filtro por b: bueno
```

**Delta Lake**:
```sql
OPTIMIZE movimientos
WHERE fec_proceso >= '2026-11-01'         -- solo particiones recientes (coste!)
ZORDER BY (cliente_id, cuenta_id);
```

**Iceberg** (procedimiento Spark):
```sql
CALL glue_catalog.system.rewrite_data_files(
  table => 'lake.movimientos',
  strategy => 'sort',
  sort_order => 'zorder(cliente_id, cuenta_id)'
);
```

Reglas senior:
- Z-ORDER por columnas **usadas en filtros/joins** y de **alta cardinalidad**; **nunca** por la columna
  de partición (ya está agrupada).
- **2–4 columnas** máximo; cada columna extra diluye la eficacia.
- Es **caro** (reescribe ficheros) → incremental, fuera de horario, solo particiones nuevas.
- No es incremental "de forma gratuita": datos nuevos llegan desordenados → re-ejecutar periódicamente.
- Alternativa moderna en Delta: **Liquid Clustering** (`CLUSTER BY`), sustituye particionado + Z-ORDER y es incremental.
- Medir: comparar *files scanned / bytes scanned* antes y después.

## 4. OPTIMIZE / compactación (bin-packing)

Une muchos ficheros pequeños en ficheros grandes (~128 MB–1 GB) **sin reordenar** (bin-pack).
- Delta: `OPTIMIZE tabla` (sin ZORDER).
- Iceberg Spark: `CALL system.rewrite_data_files(table => 'db.t')` (strategy `binpack` por defecto).
- Athena (Iceberg): `OPTIMIZE db.tabla REWRITE DATA USING BIN_PACK WHERE ...`.
- Iceberg en AWS: Glue Data Catalog ofrece **optimizadores automáticos** de compactación,
  retención de snapshots y borrado de huérfanos; **S3 Tables** lo hace gestionado.

Imprescindible tras **streaming** y cargas incrementales frecuentes.

## 5. VACUUM: qué es y por qué importa

**Qué es**: borra **físicamente** de S3 los ficheros de datos que ya **no pertenecen a ningún snapshot
vigente** (porque fueron reemplazados por OPTIMIZE, UPDATE, DELETE o MERGE) y que superan el periodo
de retención.

**Por qué es necesario**:
1. **Coste**: sin VACUUM, cada MERGE diario deja copias viejas → el almacenamiento crece sin fin.
2. **Cumplimiento / GDPR**: un `DELETE` del cliente X solo lo borra **lógicamente**; sus datos siguen en
   ficheros antiguos accesibles por time travel. **Hasta que no haces VACUUM no está borrado de verdad.**
3. Rendimiento de listados y metadatos.

**Delta**:
```sql
VACUUM movimientos RETAIN 168 HOURS;   -- 7 días (por defecto)
VACUUM movimientos DRY RUN;            -- lista lo que borraría
```
⚠️ Una retención menor que la duración de la query/stream más larga puede **romper lecturas en curso**;
Delta lo impide salvo que desactives `spark.databricks.delta.retentionDurationCheck.enabled`.
⚠️ Tras VACUUM **pierdes el time travel** a versiones anteriores al umbral.

**Iceberg** (dos operaciones):
```sql
CALL glue_catalog.system.expire_snapshots(table => 'lake.movimientos',
                                          older_than => TIMESTAMP '2026-11-13 00:00:00',
                                          retain_last => 5);
CALL glue_catalog.system.remove_orphan_files(table => 'lake.movimientos');
```
- `expire_snapshots` → elimina snapshots viejos **y** los ficheros que solo ellos referenciaban.
- `remove_orphan_files` → borra ficheros que ningún metadata referencia (escrituras fallidas).
- **Athena**: `VACUUM db.tabla` hace ambas cosas para Iceberg (según propiedades de la tabla
  `vacuum_max_snapshot_age_seconds`, etc.).

**Ojo, no confundir**:
| VACUUM en Delta/Iceberg | VACUUM en Redshift | VACUUM en PostgreSQL |
|---|---|---|
| Borra ficheros obsoletos de S3 | Recupera espacio de filas borradas y reordena por sort key | Recupera tuplas muertas (MVCC) |

## 6. Ciclo de mantenimiento recomendado (tabla Iceberg en el lake de un banco)

```
Cada carga (Glue/EMR)      →  MERGE INTO por clave de negocio (idempotente)
Diario (Control-M, noche)  →  rewrite_data_files (bin-pack) en particiones del día
Semanal                    →  rewrite_data_files sort/zorder en particiones recientes
Diario                     →  expire_snapshots (retención acordada con negocio/regulatorio: 7–30 días)
Semanal                    →  remove_orphan_files
Mensual                    →  rewrite_manifests (metadatos compactos → planificación rápida)
```

## 7. Time travel (bonus de entrevista)

```sql
-- Iceberg en Athena
SELECT * FROM movimientos FOR TIMESTAMP AS OF TIMESTAMP '2026-11-19 08:00:00 UTC';
-- Delta
SELECT * FROM movimientos VERSION AS OF 42;
```
Usos: auditoría ("¿qué dato vio el regulador ayer?"), reproducir un modelo de riesgo, rollback tras una
carga errónea (`CALL system.rollback_to_snapshot(...)` / `RESTORE TABLE ... TO VERSION AS OF`).

## Resumen

> "Sobre S3 uso Iceberg para tener ACID, MERGE y time travel. Para que las consultas lean poco, particiono
> por fecha y aplico Z-ORDER o sort por las columnas de alta cardinalidad que se filtran, como cliente o
> cuenta. Compacto los small files con OPTIMIZE/rewrite_data_files, y ejecuto VACUUM o expire_snapshots
> porque cada MERGE deja ficheros viejos: sin eso crece el coste y, en un DELETE por GDPR, el dato sigue
> físicamente ahí."
