# 04 · Funciones de ventana (Window Functions)

## 1. Concepto

Una función de ventana calcula un valor **para cada fila** usando un **grupo de filas relacionadas**
(la ventana), **sin colapsar filas** como hace `groupBy`.

```
groupBy(cliente)  → 1 fila por cliente (pierdes el detalle)
ventana(cliente)  → todas las filas, cada una con su ranking / acumulado / valor anterior
```

Estructura:
```python
from pyspark.sql import Window, functions as F
w = (Window
     .partitionBy("cliente_id")          # ¿qué filas forman el grupo?  → provoca SHUFFLE
     .orderBy(F.col("fecha").asc())      # ¿en qué orden?               → provoca SORT
     .rowsBetween(Window.unboundedPreceding, Window.currentRow))  # ¿qué marco (frame)?
```

SQL equivalente: `SUM(importe) OVER (PARTITION BY cliente_id ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`

## 2. Familias de funciones

| Familia | Funciones | Caso bancario |
|---|---|---|
| **Ranking** | `row_number`, `rank`, `dense_rank`, `percent_rank`, `ntile` | Último movimiento, top-N clientes por oficina, deciles de riesgo |
| **Analíticas / offset** | `lag`, `lead`, `first`, `last`, `nth_value` | Saldo anterior, días entre transacciones, detección de cambios |
| **Agregadas** | `sum`, `avg`, `min`, `max`, `count` sobre ventana | Saldo acumulado, media móvil 30 días |

### row_number vs rank vs dense_rank (pregunta clásica)
| importe | row_number | rank | dense_rank |
|---|---|---|---|
| 500 | 1 | 1 | 1 |
| 500 | 2 | 1 | 1 |
| 300 | 3 | **3** | **2** |

- `row_number` → único siempre (para **deduplicar**, ojo: si hay empate el elegido es no determinista;
  añade un desempate al `orderBy`).
- `rank` → empates comparten puesto y deja huecos.
- `dense_rank` → empates sin huecos.

## 3. El frame: ROWS vs RANGE

- **Sin `orderBy`** → el frame es toda la partición.
- **Con `orderBy` y sin frame explícito** → por defecto `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
  (acumulado; ojo: con valores repetidos de orden, RANGE incluye todos los "empatados").
- `rowsBetween(-6, 0)` → las 6 filas anteriores + la actual (ventana física por filas).
- `rangeBetween(-30*86400, 0)` sobre un timestamp en segundos → **últimos 30 días** reales (ventana lógica
  por valor), aunque falten días.

## 4. Patrones que salen en TODAS las entrevistas

### a) Deduplicar quedándote con el registro más reciente
```python
w = Window.partitionBy("cuenta_id").orderBy(F.col("fec_actualizacion").desc(), F.col("id_carga").desc())
ultimo = (df.withColumn("rn", F.row_number().over(w))
            .filter("rn = 1").drop("rn"))
```
Típico al procesar CDC (varias versiones de la misma fila en el mismo lote).

### b) Top-N por grupo
```python
w = Window.partitionBy("oficina_id").orderBy(F.col("saldo").desc())
top3 = df.withColumn("r", F.dense_rank().over(w)).filter("r <= 3")
```

### c) Saldo acumulado
```python
w = Window.partitionBy("cuenta_id").orderBy("fecha", "id_mov") \
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("saldo_acum", F.sum("importe").over(w))
```

### d) Diferencia con el registro anterior (lag)
```python
w = Window.partitionBy("tarjeta_id").orderBy("ts")
df = (df.withColumn("ts_prev", F.lag("ts").over(w))
        .withColumn("seg_desde_anterior", F.col("ts").cast("long") - F.col("ts_prev").cast("long")))
# Fraude: 2 compras en < 60 s en países distintos
```

### e) Detección de cambios / SCD tipo 2
```python
w = Window.partitionBy("cliente_id").orderBy("fec_inicio")
scd = (df.withColumn("fec_fin", F.lead("fec_inicio").over(w))
         .withColumn("es_actual", F.col("fec_fin").isNull()))
```

### f) Gaps & islands (rachas consecutivas)
Días consecutivos en descubierto: `fecha - row_number()` es constante dentro de una racha →
agrupar por esa diferencia.

## 5. Rendimiento de las ventanas (lo que diferencia al senior)

1. **Cada `partitionBy` distinta = un shuffle + sort.** Agrupa los cálculos que comparten la misma
   especificación de ventana en el mismo `select`: Spark los resuelve en un único `Window` operator.
2. **Ventana sin `partitionBy`** (`Window.orderBy("fecha")`) → mueve **todos los datos a 1 partición**
   → 1 task → OOM. Spark avisa: *"No Partition Defined for Window operation!"*. Evitar siempre.
3. **Skew**: si un `cliente_id` tiene 50 M filas, toda su ventana va a una task. Soluciones: filtrar
   antes, partir la clave (p.ej. `cliente_id + mes`) si la lógica lo permite.
4. **Filtra antes** de la ventana (menos filas que ordenar).
5. **Dedup con `row_number`** suele ser mejor que `groupBy + max + join` de vuelta (evitas un join).
   Alternativa aún más barata para "último registro": `groupBy(clave).agg(F.max_by("valor", "fecha"))`
   (Spark ≥ 3.3) → agregación sin sort completo.
6. Frames `unboundedPreceding..unboundedFollowing` con `orderBy` obligan a mantener toda la partición;
   usa el frame mínimo necesario.
7. En **streaming**, las funciones de ventana analíticas (`row_number`, `lag`) **no están soportadas**
   en streams; se usan ventanas **de tiempo** (`window()`) o `foreachBatch` (doc 06).

## 6. Ventanas de tiempo ≠ funciones de ventana

| Window functions (`OVER`) | Time windows (`F.window`) |
|---|---|
| Cálculo por fila sobre filas relacionadas | **Agrupación** por intervalos de tiempo |
| Batch | Batch y **streaming** |
| `row_number().over(w)` | `groupBy(F.window("ts", "5 minutes", "1 minute"))` (tumbling / sliding) |

## Resumen

> "Las window functions calculan por fila sin colapsar. Las uso para dedup con row_number, top-N,
> acumulados y lag/lead. Sé que cada partitionBy implica shuffle y sort, así que agrupo las que comparten
> especificación, nunca hago una ventana sin partición, y vigilo el skew por clave."
