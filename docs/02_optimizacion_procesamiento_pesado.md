# 02 · Optimización de procesamientos pesados

> Método senior: **Medir → Identificar el cuello → Corregir 1 cosa → Volver a medir.**
> Herramientas: Spark UI (Stages, SQL tab), `explain("formatted")`, métricas de CloudWatch / Glue job metrics.

## 1. Leer menos datos (lo más barato es lo que no lees)

| Técnica | Cómo | Por qué |
|---|---|---|
| **Formato columnar** | Parquet/ORC + Snappy/ZSTD | Lee solo columnas usadas, compresión, estadísticas min/max |
| **Column pruning** | `select` solo lo necesario, nunca `select *` | Menos I/O y menos memoria |
| **Predicate pushdown** | `filter` lo antes posible, sobre columnas "crudas" | El filtro baja al lector Parquet / a la BD (JDBC) |
| **Partition pruning** | Particionar por la columna de filtro habitual (ej. `fec_proceso`) | Ni se listan las carpetas descartadas |
| **Data skipping** | Z-ORDER / sort / clustering (ver doc 05) | Salta ficheros completos por min/max |
| **Esquema explícito** | `spark.read.schema(schema)` | Evita un pase extra de inferencia |

⚠️ Pushdown se rompe si aplicas funciones a la columna: `filter(year(col("fecha")) == 2026)` no salta
particiones; mejor `filter(col("fecha").between("2026-01-01","2026-12-31"))`.

⚠️ No sobre-particionar: particionar por `cliente_id` → millones de carpetas diminutas. Regla:
**cardinalidad baja** y particiones de **≥1 GB**.

## 2. Joins: la decisión más importante

| Estrategia | Cuándo la elige Spark | Coste |
|---|---|---|
| **Broadcast Hash Join** | Un lado < `spark.sql.autoBroadcastJoinThreshold` (10 MB por defecto) o hint | **Sin shuffle** del lado grande. El más rápido |
| **Sort Merge Join** | Dos tablas grandes, join por igualdad | Shuffle + sort de ambos lados |
| **Shuffle Hash Join** | Un lado mediano que cabe en memoria por partición | Shuffle sin sort |
| **Broadcast Nested Loop / Cartesian** | Joins sin igualdad (`<`, `between`) | ⚠️ Peligroso, O(n×m) |

```python
from pyspark.sql.functions import broadcast
hechos.join(broadcast(dim_oficinas), "oficina_id")     # forzar broadcast de la dimensión
```

Buenas prácticas:
- Filtra y agrega **antes** del join, no después.
- Mismos tipos en las claves (string vs int → cast implícito → no hay pushdown ni bucketing).
- Claves nulas: muchas filas con `NULL` en la clave → skew; fíltralas o trátalas aparte.
- Para joins repetidos de dos tablas enormes por la misma clave → **bucketing** (pre-particionar
  en escritura) o tablas con clustering.

## 3. Data skew (datos desbalanceados)

**Síntoma**: en la Spark UI, en un stage, 199 tasks tardan 10 s y **1 tarda 20 min**; esa task
muestra un "Shuffle Read Size" enorme. Típico: un cliente corporativo con millones de movimientos,
o `NULL` / `'DESCONOCIDO'` como clave.

Soluciones (de menos a más esfuerzo):
1. **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled=true`).
2. **Broadcast** del lado pequeño (si cabe) → no hay shuffle, no hay skew.
3. **Tratar aparte la clave caliente**: procesar `cliente_id = X` por separado y hacer `union`.
4. **Salting**: añadir un sufijo aleatorio a la clave del lado grande y replicar el lado pequeño N veces.

```python
N = 10
grande = grande.withColumn("salt", (F.rand() * N).cast("int"))
pequeno = pequeno.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(N)])))
res = grande.join(pequeno, ["clave", "salt"]).drop("salt")
```

Para agregaciones con skew: **agregación en dos fases** (agregar por `clave+salt`, luego por `clave`).

## 4. Particiones: ni pocas ni muchas

- Objetivo: tasks de **~100–200 MB** de datos y **2–3× nº de cores** en particiones.
- `spark.sql.shuffle.partitions`: 200 es malo para 2 TB (particiones de 10 GB → spill) y para
  200 MB (200 tasks de 1 MB → overhead). Con AQE, pon un valor alto y deja que **coalesce** ajuste.
- `repartition(n)` → shuffle completo, equilibra. `repartition("col")` → agrupa por clave.
- `coalesce(n)` → **sin shuffle**, solo junta particiones; para **reducir** (ver doc 03).

## 5. El problema de los small files

Miles de ficheros de KB en S3:
- Listar S3 es lento, cada fichero = 1 petición GET + abrir footer Parquet.
- Athena/Spark tardan más en planificar que en leer.
- Coste de peticiones S3.

Causas: `shuffle.partitions=200` × particiones de fecha, streaming con micro-batches frecuentes,
`append` incremental de pocos registros.

Soluciones:
- `coalesce(n)` / `repartition(n, "fec_proceso")` antes de escribir.
- `spark.sql.files.maxRecordsPerFile` para acotar el tamaño máximo.
- Compactación periódica: `OPTIMIZE` (Delta), `rewrite_data_files` (Iceberg), `OPTIMIZE ... BIN_PACK` en Athena.
- Objetivo: **128 MB – 1 GB por fichero**.

## 6. Evitar trabajo repetido

- **Reutilización** del mismo DataFrame en varias acciones → `persist()` + `unpersist()` (doc 03).
- **Linajes muy largos** (bucles iterativos) → `checkpoint()` o escribir a staging y releer.
- No llamar a `count()` "para ver" en medio del pipeline en producción: cada `count` es un job completo.
- Varias agregaciones sobre la misma clave → hacerlas en **un solo `groupBy().agg(...)`**.
- Varias ventanas con la **misma** `partitionBy/orderBy` → Spark las resuelve en un solo shuffle.

## 7. Código eficiente

| Evitar | Usar |
|---|---|
| UDF Python fila a fila | `pyspark.sql.functions` nativas; si no hay, **Pandas UDF** |
| `collect()` / `toPandas()` grandes | Escribir a S3 y consultar con Athena |
| `withColumn` en bucle de 200 columnas | Un solo `select([...])` (plan más pequeño) |
| `distinct()` sobre todo el DataFrame | `dropDuplicates(["clave"])` o ventana `row_number` |
| `orderBy` global innecesario | `sortWithinPartitions` si solo necesitas orden dentro del fichero |
| `count() > 0` para saber si hay datos | `df.isEmpty()` / `limit(1)` |
| `union` de esquemas distintos por posición | `unionByName(allowMissingColumns=True)` |

## 8. Lectura desde bases de datos (JDBC / origen relacional)

- Paralelizar: `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` (si no, **1 sola conexión**).
- `fetchsize` alto (10 000) para reducir round-trips.
- Pushdown de la consulta (`query`/`dbtable` con subselect filtrado).
- No saturar el OLTP: extraer de réplica de lectura o vía **DMS/CDC** a S3.

## 9. Escritura idempotente

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(df.write.mode("overwrite").partitionBy("fec_proceso").parquet(ruta))
```
- `dynamic` → solo sobreescribe las particiones presentes en `df` (re-ejecutar el día X no borra el resto).
- Con Iceberg/Delta → `MERGE INTO` por clave de negocio.
- Nunca `append` sin control → duplicados al re-lanzar desde Control-M.

## 10. Checklist "proceso pesado que tarda 3 horas"

1. ¿Qué stage consume el tiempo? (Spark UI → Stages ordenados por duración)
2. ¿Hay **spill**? → más particiones o más memoria.
3. ¿Hay **skew**? (max task ≫ mediana) → AQE / broadcast / salting.
4. ¿Hay **shuffles evitables**? (`Exchange` en el plan) → broadcast, agregar antes, un solo groupBy.
5. ¿Se lee de más? (bytes leídos vs necesarios) → pruning, pushdown, columnar.
6. ¿Se recalcula lo mismo? → persist/unpersist.
7. ¿UDFs Python? → nativas.
8. ¿Escritura lenta / miles de ficheros? → coalesce, committer S3 (doc 07).
9. ¿Recursos bien dimensionados? → executors 4–5 cores, overhead suficiente, autoscaling.
10. Medir otra vez y documentar la mejora (ej: "de 3 h a 25 min, -70% coste DPU").
