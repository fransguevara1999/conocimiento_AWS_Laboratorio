# 08 · Preguntas de entrevista (respuestas senior, cortas)

## Arquitectura Spark
**1. ¿Qué pasa cuando ejecutas `df.write`?**
Es una acción: el driver toma el plan lógico, Catalyst lo optimiza, se genera el plan físico, el
DAGScheduler lo corta en stages por cada shuffle, y cada stage se lanza como tasks (una por partición)
en los executors.

**2. Diferencia entre transformación narrow y wide.**
Narrow: cada partición de salida depende de una de entrada, sin mover datos (filter, select). Wide:
requiere shuffle (groupBy, join, distinct) y corta stage.

**3. ¿Por qué DataFrame y no RDD?**
Catalyst optimiza el plan y Tungsten genera código y gestiona memoria binaria; con RDD el código es
opaco y no hay optimización.

**4. ¿Qué es AQE?**
Reoptimización en tiempo de ejecución con estadísticas reales: une particiones pequeñas de shuffle,
convierte joins a broadcast y divide particiones con skew.

## Optimización
**5. Un job pasó de 1 h a 4 h sin cambios de código. ¿Qué haces?**
Comparo volumen de entrada (¿creció?), reviso Spark UI: stage más lento, skew (max vs mediana de tasks),
spill, nº de ficheros de entrada (small files nuevos), y si cambió el plan (un join dejó de ser broadcast
porque la dimensión creció por encima del umbral).

**6. ¿Cómo resuelves data skew?**
AQE skew join; broadcast si un lado es pequeño; separar las claves calientes; salting; agregación en dos fases.

**7. ¿coalesce o repartition?**
coalesce reduce particiones sin shuffle, ideal para reducir ficheros de salida; puede reducir el
paralelismo del stage. repartition hace shuffle completo y equilibra, sirve para aumentar o agrupar por clave.

**8. ¿Cuándo cacheas?**
Cuando reutilizo el DataFrame en dos o más acciones. MEMORY_AND_DISK, y unpersist al terminar para devolver
memoria a execution.

**9. ¿Una temp view materializa datos?**
No, es un alias de un plan lógico. Se recalcula en cada uso salvo que se cachee.

**10. ¿Por qué evitar UDFs Python?**
Son opacas para Catalyst (no hay pushdown ni codegen) y serializan cada fila entre JVM y Python.
Uso funciones nativas o Pandas UDF con Arrow.

**11. ¿Cómo evitas small files?**
coalesce/repartition antes de escribir, maxRecordsPerFile, triggers más largos en streaming y compactación
periódica (OPTIMIZE / rewrite_data_files).

**12. ¿Particionar por cliente_id?**
No, por alta cardinalidad: millones de carpetas pequeñas. Particiono por fecha y uso Z-ORDER o sort
por cliente_id.

## Ventanas
**13. ¿Cómo deduplicas quedándote con el último registro?**
`row_number()` sobre una ventana partitionBy(clave) orderBy(fecha desc, desempate) y filtro rn = 1.

**14. Diferencia entre rank y dense_rank.**
Ambos dan el mismo puesto a los empates; rank deja huecos (1,1,3) y dense_rank no (1,1,2).

**15. ¿Riesgo de una ventana sin partitionBy?**
Todo va a una sola partición: una task, sin paralelismo y riesgo de OOM.

## Formatos de tabla
**16. ¿Qué es Z-ORDER?**
Ordenación multidimensional que co-localiza valores de varias columnas en los mismos ficheros para
mejorar el data skipping por min/max cuando filtras por cualquiera de ellas.

**17. ¿Qué hace VACUUM?**
Borra físicamente los ficheros que ya no pertenecen a ningún snapshot vigente y superan la retención.
Reduce coste y es necesario para que un DELETE por GDPR sea un borrado real. Pierdes el time travel anterior.

**18. ¿Por qué Iceberg en un banco?**
ACID sobre S3, MERGE para CDC, time travel para auditoría, schema evolution sin reescribir, y soporte nativo
en Athena, Glue, EMR y Redshift. Es abierto, sin lock-in.

## Streaming
**19. ¿Qué es un watermark?**
El retraso máximo tolerado en event time. Permite cerrar ventanas y purgar estado. Los eventos más
tardíos se descartan.

**20. ¿Cómo garantizas exactly-once?**
Fuente reproducible con offsets, checkpoint, y sink transaccional o idempotente (Iceberg, o MERGE por
clave en foreachBatch).

**21. El streaming acumula retraso. ¿Qué haces?**
Comparo inputRowsPerSecond con processedRowsPerSecond. Escalo executors, aumento particiones o shards
del origen, limito con maxOffsetsPerTrigger, reviso el estado (watermark, RocksDB) y los small files del sink.

**22. ¿Cuándo no usarías streaming?**
Si negocio necesita latencia de horas, uso batch o trigger availableNow orquestado: misma lógica a menor
coste y menos complejidad operativa.

## Caso de diseño (respuesta estructurada)
**23. "Diseña la ingesta de 500 M de movimientos diarios desde el core bancario para analítica y fraude."**
1. **Ingesta**: CDC del core (DMS o ficheros de host) a S3 RAW con fichero de control. Lo urgente
   (autorizaciones de tarjeta) va por Kinesis/MSK.
2. **Orquestación**: watcher sobre el fichero de control y malla Control-M con dependencias y SLA.
3. **Procesamiento**: Glue/EMR en PySpark: validación de DQ, conciliación de cifras con el control,
   dedup por ventana y MERGE en Iceberg MASTER particionado por fecha, con Z-ORDER por cliente y cuenta.
4. **Fraude**: Flink sobre Kinesis con ventanas de event time y watermark, que escribe alertas en
   DynamoDB y el histórico en Iceberg.
5. **Consumo**: Athena ad-hoc, Redshift para BI en GOLD y SageMaker con Feature Store.
6. **Gobierno**: Lake Formation, KMS, CloudTrail, Macie para PII, y retención con expire_snapshots/VACUUM.
7. **Operación**: compactación nocturna, alarmas de lag y de SLA, y jobs idempotentes para el re-run.
