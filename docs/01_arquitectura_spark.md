# 01 · Arquitectura de Apache Spark

## 1. ¿Qué es Spark?

Motor de **procesamiento distribuido en memoria**. Divide los datos en **particiones** y las procesa
en paralelo en muchas máquinas. Sustituye a MapReduce (que escribía a disco entre cada paso).

**Analogía**: una cocina industrial. El **chef (driver)** recibe la receta, la divide en pasos y reparte
el trabajo entre **cocineros (executors)**; cada cocinero tiene varias **manos (cores)** y cada mano
procesa un **plato (partición)** a la vez.

## 2. Componentes

```
                ┌──────────────────────────┐
                │         DRIVER           │  SparkSession / SparkContext
                │  - construye el DAG      │  - Catalyst optimiza el plan
                │  - planifica stages      │  - DAGScheduler / TaskScheduler
                └──────────┬───────────────┘
                           │ pide recursos
                ┌──────────▼───────────────┐
                │    CLUSTER MANAGER       │  YARN (EMR) · Kubernetes (EMR on EKS)
                └──────────┬───────────────┘  · Standalone · (Glue lo gestiona por ti)
          ┌────────────────┼────────────────┐
   ┌──────▼─────┐   ┌──────▼─────┐   ┌──────▼─────┐
   │ EXECUTOR 1 │   │ EXECUTOR 2 │   │ EXECUTOR N │   JVMs en los nodos worker
   │ cores=4    │   │ cores=4    │   │ cores=4    │   cada core ejecuta 1 TASK
   │ cache/mem  │   │ cache/mem  │   │ cache/mem  │   = 1 partición a la vez
   └────────────┘   └────────────┘   └────────────┘
```

| Componente | Rol | Error típico |
|---|---|---|
| **Driver** | Coordina, guarda el plan, recibe resultados de `collect()` | `collect()` de millones de filas → OOM en driver |
| **Executor** | Ejecuta tasks y guarda datos cacheados | Executors muy grandes → pausas largas de GC |
| **Cluster manager** | Asigna CPU/memoria | Pedir más recursos de los que hay → jobs en cola |
| **Partición** | Unidad de paralelismo | Pocas → cores ociosos; demasiadas → overhead |

## 3. Jerarquía de ejecución: Application → Job → Stage → Task

- **Application**: tu script / job Glue completo.
- **Job**: se crea por **cada acción** (`count`, `write`, `collect`, `show`).
- **Stage**: tramo del job **sin shuffle**. Cada **shuffle corta un stage**.
- **Task**: una por partición en cada stage. **Nº tasks = nº particiones.**

```python
df = spark.read.parquet("s3://lake/raw/movimientos/")      # transformación (lazy)
df2 = df.filter("importe > 0")                             # narrow  → mismo stage
df3 = df2.groupBy("cliente_id").sum("importe")             # wide    → SHUFFLE → nuevo stage
df3.write.parquet("s3://lake/gold/saldos/")                # ACCIÓN  → lanza el job
```

## 4. Transformaciones vs acciones · Lazy evaluation

- **Transformaciones** (`select`, `filter`, `join`, `groupBy`…) → solo construyen el plan.
- **Acciones** (`count`, `show`, `write`, `collect`) → disparan la ejecución.
- **Ventaja**: Spark ve el plan completo y lo reordena (empuja filtros hacia la fuente, elimina columnas).

### Narrow vs Wide

| Narrow (sin shuffle) | Wide (con shuffle) |
|---|---|
| `select`, `filter`, `withColumn`, `map`, `union`, `coalesce` | `groupBy`, `join` (no broadcast), `distinct`, `orderBy`, `repartition`, ventanas con `partitionBy` |
| Cada partición de salida depende de **una** de entrada | Cada partición de salida depende de **muchas** → mover datos por la red |

## 5. El Shuffle (lo más caro de Spark)

Redistribuye datos entre executors para que todas las filas con la misma clave caigan juntas.
1. **Map side**: cada task escribe ficheros de shuffle a disco local, particionados por hash de la clave.
2. **Reduce side**: las tasks del siguiente stage leen por red sus bloques.

Coste = **disco + red + serialización**. Por eso: filtrar antes, broadcast, evitar shuffles repetidos.

`spark.sql.shuffle.partitions` (por defecto **200**) = nº de particiones tras un shuffle.
Con AQE activado Spark ajusta este número automáticamente.

## 6. Catalyst y Tungsten (por qué DataFrame > RDD)

**Catalyst** (optimizador del plan):
```
SQL / DataFrame
   → Plan lógico sin resolver → (Analyzer + Catálogo) → Plan lógico
   → (Optimizer: predicate pushdown, column pruning, constant folding, reordenar joins)
   → Plan lógico optimizado → varios planes físicos → (Cost model) → Plan físico elegido
   → Whole-stage code generation (Tungsten)
```

**Tungsten** (ejecución eficiente): memoria off-heap binaria, generación de código Java por stage,
menos objetos JVM → menos GC.

> Por eso **las UDF Python son lentas**: son una caja negra para Catalyst y obligan a serializar
> filas entre JVM y Python. Alternativa: funciones nativas o **Pandas UDF** (Arrow, vectorizadas).

Ver el plan: `df.explain("formatted")`. Busca: `Exchange` (= shuffle), `BroadcastHashJoin`,
`SortMergeJoin`, `PushedFilters`, `ReadSchema`.

## 7. AQE · Adaptive Query Execution (Spark 3.x)

Reoptimiza **durante** la ejecución con estadísticas reales de cada stage:
1. **Coalesce de particiones de shuffle**: une particiones pequeñas (adiós a 200 tasks de 1 MB).
2. **Cambio de join a broadcast** si un lado resultó pequeño tras filtrar.
3. **Skew join**: divide particiones gigantes automáticamente.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")                    # default true desde 3.2
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## 8. Modelo de memoria del executor

```
Executor JVM heap (spark.executor.memory)
├── Reserved (300 MB)
├── User memory (~40%)            → estructuras de tu código, UDFs
└── Unified memory (spark.memory.fraction = 0.6)
     ├── Execution  → shuffles, joins, sorts, aggregations
     └── Storage    → cache/persist, broadcast
     (se prestan espacio entre sí; execution puede expulsar a storage)
+ memoryOverhead (fuera del heap, ~10%) → Python workers, buffers nativos
```

Síntomas:
- **Spill to disk** en la Spark UI → particiones demasiado grandes para la memoria de execution.
- **Container killed by YARN for exceeding memory limits** → sube `spark.executor.memoryOverhead`
  (muy típico en PySpark).
- **GC time alto** → executors demasiado grandes o demasiados objetos; usa 4–5 cores por executor.

## 9. Dimensionamiento clásico (EMR / YARN)

Regla práctica: **~5 cores por executor**, dejar 1 core y ~1 GB por nodo al SO/YARN.

Ejemplo: 10 nodos × 16 cores × 64 GB
- Cores útiles/nodo: 15 → 3 executors/nodo × 5 cores.
- Memoria/executor: 63 GB / 3 ≈ 21 GB → ~19 GB heap + ~2 GB overhead.
- Total: 30 executors − 1 (para el driver/AM) = **29 executors × 5 cores = 145 tasks en paralelo**.
- Nº particiones recomendado: **2–3× nº cores** → ~300–450.

## 10. Lectura de ficheros y particiones de entrada

- `spark.sql.files.maxPartitionBytes` (128 MB) → tamaño de split al leer.
- Parquet/ORC son **columnares** → leen solo las columnas pedidas y saltan row groups con min/max.
- CSV/JSON → leen todo, sin pushdown real, inferir esquema cuesta un pase extra (define el `schema`).
- **Partition pruning**: si la tabla está particionada por `fec_proceso` y filtras por ella, Spark
  ni abre las carpetas que no coinciden.

## Resumen para decir en voz alta

> "Spark tiene un driver que construye un DAG lazy y lo optimiza con Catalyst; el DAG se corta en stages
> en cada shuffle, y cada stage en tasks, una por partición, que corren en los cores de los executors.
> Mi trabajo de optimización es reducir datos pronto, minimizar y equilibrar shuffles, y controlar el
> tamaño de particiones y ficheros. En Spark 3 apoyo eso en AQE."
