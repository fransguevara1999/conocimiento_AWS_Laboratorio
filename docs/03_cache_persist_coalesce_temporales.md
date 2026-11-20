# 03 · cache, persist, unpersist, coalesce, repartition, temporales y checkpoint

## 1. ¿Por qué hace falta cachear? (por la lazy evaluation)

Spark **no guarda resultados intermedios**. Cada acción recalcula el DAG **desde la fuente**:

```python
base = spark.read.parquet(ruta).filter(...).join(...)   # caro
base.filter("tipo='A'").write.parquet(r1)   # job 1: lee S3 + join
base.filter("tipo='B'").write.parquet(r2)   # job 2: VUELVE a leer S3 + join
```

Con `persist`, el job 1 materializa `base` en memoria/disco de los executors y el job 2 la reutiliza.

## 2. cache vs persist

| | `cache()` | `persist(StorageLevel)` |
|---|---|---|
| Nivel | DataFrame: `MEMORY_AND_DISK` | El que elijas |
| Uso | Atajo | Control fino |

Niveles útiles:
- `MEMORY_AND_DISK` → lo que no cabe en memoria va a disco local (opción segura por defecto).
- `MEMORY_ONLY` → si no cabe, se **recalcula** la partición.
- `DISK_ONLY` → datasets enormes que no quieres recalcular.
- `*_2` → replicado en 2 executors (tolerancia a fallos; doble coste).

**Es lazy**: `persist()` no hace nada hasta la siguiente acción. Para materializar a propósito:
`df.persist(); df.count()`.

### Cuándo SÍ cachear
- El DataFrame se usa en **2 o más acciones** (varias escrituras, varios joins con distintas tablas).
- Algoritmos **iterativos** (ML, bucles).
- Resultado de un cálculo caro (join grande, ventana) que se reutiliza.

### Cuándo NO
- Se usa una sola vez (añades coste de escribir en memoria y no lo reutilizas).
- Dataset gigante que no cabe → expulsa otras cosas, provoca spill y GC.
- Justo después de leer Parquet sin transformaciones (releer Parquet de S3 suele ser barato).

## 3. unpersist: liberar memoria

```python
base.persist(StorageLevel.MEMORY_AND_DISK)
... usos ...
base.unpersist()          # libera los bloques en los executors
```

**Por qué importa**:
- La memoria de **storage** compite con la de **execution** (joins, sorts, shuffles).
  Datos cacheados que ya no usas → menos memoria para el resto → **spill** y OOM.
- En jobs largos con varias fases (típico Glue nocturno), cachear sin liberar acumula basura.
- Spark expulsa por LRU, pero **no sabe** que ya no lo necesitas; tú sí.
- `unpersist(blocking=True)` espera a que se libere (útil antes de una fase muy pesada).

Ver qué hay cacheado: pestaña **Storage** de la Spark UI. Limpiar todo: `spark.catalog.clearCache()`.

## 4. coalesce vs repartition

| | `coalesce(n)` | `repartition(n)` / `repartition(n, col)` |
|---|---|---|
| Shuffle | **No** (junta particiones vecinas) | **Sí**, completo |
| Dirección | Solo **reducir** | Aumentar o reducir |
| Equilibrio | Puede quedar desbalanceado | Particiones equilibradas (hash/round-robin) |
| Coste | Muy barato | Caro |
| Uso típico | Reducir nº de **ficheros de salida** | Más paralelismo, o agrupar por clave antes de escribir/join |

### ¿Por qué usar coalesce?
Tras un filtro fuerte o un join, tienes 400 particiones casi vacías → escribir generaría **400 ficheros
pequeños**. `coalesce(8)` → 8 ficheros de buen tamaño **sin shuffle**.

```python
df.coalesce(8).write.mode("overwrite").parquet(salida)
```

### ⚠️ La trampa del coalesce
`coalesce` es narrow → Spark puede **propagar el paralelismo reducido hacia atrás** en el mismo stage.
`df.join(...).groupBy(...).coalesce(1)` → todo el último stage corre con **1 task**.
- Si el cálculo previo es pesado, usa `repartition(n)` (añade un shuffle pero mantiene el paralelismo
  del cálculo) o escribe con AQE coalesce.
- `coalesce(1)` para "un único CSV" → todo pasa por un solo core. Solo para salidas pequeñas.

### repartition por columna antes de escribir particionado
```python
df.repartition("fec_proceso").write.partitionBy("fec_proceso").parquet(salida)
```
→ cada fecha queda en una partición → **1 fichero por carpeta** (en lugar de 200 ficheros por fecha).
Si una fecha es enorme: `repartition(n, "fec_proceso", "rango")` o `maxRecordsPerFile`.

## 5. Vistas temporales (temporales en Spark SQL)

```python
df.createOrReplaceTempView("movs_tmp")          # visible solo en esta SparkSession
spark.sql("SELECT cliente_id, SUM(importe) FROM movs_tmp GROUP BY cliente_id")
df.createOrReplaceGlobalTempView("movs_glob")    # visible entre sesiones: global_temp.movs_glob
```

Conceptos clave:
- Una **vista temporal NO materializa datos**: es solo un nombre para un plan lógico. Cada consulta
  la recalcula (salvo que la cachees: `spark.catalog.cacheTable("movs_tmp")` o `CACHE TABLE`).
- Sirve para: legibilidad (pasar lógica SQL de los analistas / migrar T-SQL o procedimientos),
  dividir una query monstruosa en pasos, mezclar API DataFrame y SQL.
- Equivalente en SQL a las **CTE** (`WITH ... AS`) o a las **#tablas temporales** de SQL Server, pero
  **cuidado**: la `#temp` de SQL Server sí materializa; la temp view de Spark no.

### Tablas "temporales" materializadas (staging)
En procesos pesados con muchas fases conviene **materializar** pasos intermedios:
- **Escribir a una ruta staging en S3** (Parquet) y releer → corta el linaje, permite reintentar
  desde la fase intermedia (muy útil con Control-M: re-run de la fase 3 sin repetir 1 y 2).
- `CACHE TABLE x AS SELECT ...` → materializa en memoria (solo durante la sesión).

## 6. checkpoint y localCheckpoint

- `df.checkpoint()` → escribe el DataFrame a `spark.sparkContext.setCheckpointDir(...)` y **trunca el linaje**.
- Útil en **bucles iterativos** donde el plan crece y Catalyst tarda minutos solo en planificar,
  o para evitar recomputar un linaje enorme tras un fallo.
- `localCheckpoint()` → más rápido (disco local de executors), pero no tolera la pérdida del executor.
- En **streaming**, "checkpoint" es otra cosa: la ubicación donde se guarda el progreso (offsets) y
  el estado (doc 06).

## 7. Broadcast variables vs broadcast join
- **Broadcast join**: Spark envía una tabla pequeña a todos los executors (`broadcast(df)`).
- **Broadcast variable** (`sc.broadcast(dict)`): enviar un objeto Python (diccionario de lookup)
  una vez por executor en lugar de una vez por task. Liberar con `.unpersist()` / `.destroy()`.

## Resumen para la entrevista

> "Cacheo solo lo que reutilizo en varias acciones, con `MEMORY_AND_DISK`, y hago `unpersist` en cuanto
> termino porque la memoria de storage le quita espacio a joins y shuffles. Uso `coalesce` para reducir
> ficheros de salida sin shuffle, sabiendo que puede reducir el paralelismo del stage; si el último cálculo
> es pesado prefiero `repartition`. Las temp views son solo nombres de planes, no materializan; si quiero
> cortar el linaje o poder reintentar por fases, escribo a staging en S3 o uso checkpoint."
