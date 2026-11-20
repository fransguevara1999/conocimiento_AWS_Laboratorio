# 07 · Spark en AWS: Glue y EMR

## 1. AWS Glue (Spark serverless)

| Concepto | Detalle |
|---|---|
| **Worker types** | `G.1X` (4 vCPU, 16 GB), `G.2X` (8 vCPU, 32 GB), `G.4X`/`G.8X` (cargas muy pesadas), `G.025X` (streaming ligero) |
| **DPU** | Unidad de facturación (4 vCPU + 16 GB). Se paga DPU-hora con facturación por segundo |
| **Auto Scaling** | Glue ≥ 3.0: escala workers según demanda hasta el máximo configurado |
| **Flex execution** | Más barato (capacidad sobrante) para jobs no urgentes; puede tardar en arrancar |
| **Job bookmarks** | Procesamiento incremental de ficheros/particiones nuevos |
| **DynamicFrame** | Abstracción de Glue tolerante a esquemas inconsistentes (`ResolveChoice`); para rendimiento, convertir a DataFrame (`toDF()`) |
| **Glue Data Catalog** | Metastore compartido (Athena, EMR, Redshift Spectrum) |
| **Glue Data Quality** | Reglas DQDL (`Completeness "id" > 0.99`, `IsUnique "id"`) |
| **Parámetros** | `--conf`, `--enable-metrics`, `--enable-spark-ui`, `--enable-auto-scaling`, `--datalake-formats iceberg` |

Optimizaciones específicas de Glue:
- Leer con **pushdown predicates** del catálogo: `push_down_predicate="fec_proceso='2026-11-20'"` →
  solo lista esas particiones.
- `groupFiles='inPartition'` y `groupSize` para leer muchos ficheros pequeños como menos tasks.
- Activar **Spark UI** y **job metrics** para ver stages y skew.
- Elegir `G.2X` si hay spill/OOM antes que añadir más `G.1X`.
- Job de Python Shell para tareas pequeñas (no levantar Spark para 10 MB).

## 2. Amazon EMR

| Concepto | Detalle |
|---|---|
| **Nodos** | Primary (driver/YARN RM), Core (HDFS + cómputo), Task (solo cómputo → **Spot**) |
| **Modos** | EMR on EC2 (control total), EMR on EKS (multi-tenant en Kubernetes), **EMR Serverless** |
| **Clúster transitorio** | Crear → ejecutar steps → terminar. Datos en S3, no en HDFS |
| **Instance fleets** | Varios tipos de instancia y mezcla On-Demand/Spot → resiliencia ante falta de Spot |
| **Managed scaling** | Escala core/task según carga YARN |
| **Optimizaciones EMR** | Runtime de Spark optimizado por AWS, `EMRFS S3-optimized committer` |

## 3. Escritura a S3: el committer

El commit clásico de Hadoop escribe en `_temporary` y luego **renombra**. En S3 *renombrar = copiar +
borrar* → lentísimo y no atómico. Soluciones:
- **EMRFS S3-optimized committer** (EMR, por defecto para Parquet).
- Los **formatos de tabla** (Iceberg/Delta) evitan el problema: commit = escribir un metadata nuevo.
- Glue: `--enable-s3-parquet-optimized-committer` (por defecto en versiones recientes).

## 4. Glue vs EMR vs EMR Serverless

| Criterio | Glue | EMR Serverless | EMR on EC2 |
|---|---|---|---|
| Gestión | Ninguna | Ninguna | Tú gestionas clúster |
| Control de versión/config Spark | Limitado | Medio | Total |
| Coste en cargas grandes y constantes | Alto | Medio | **Bajo** (Spot/reservas) |
| Arranque | ~segundos–1 min | ~segundos (pre-initialized capacity) | Minutos (si es transitorio) |
| Ideal para | ETL estándar, equipos pequeños | Spark puntual sin clúster | Migraciones Hadoop, cargas masivas, librerías nativas |

## 5. Orquestación

- **Control-M**: jobs nativos para Glue/EMR/EMR Serverless; dependencias, calendarios bancarios, SLA, re-run.
- **Airflow (MWAA)**: `GlueJobOperator`, `EmrServerlessStartJobOperator`, `S3KeySensor`.
- **Step Functions**: orquestación serverless con integración nativa (`.sync`) a Glue/EMR.
- Regla: **la lógica en el job, la orquestación fuera**; jobs parametrizados por `fec_proceso` e idempotentes.

## 6. Costes (FinOps)

- Dimensionar por métricas (no por intuición); auto scaling activado.
- Spot en task nodes; Flex en Glue para jobs no críticos.
- Leer menos (columnar, particiones, pruning) = menos tiempo de cluster.
- Clústeres transitorios; no dejar notebooks/endpoints encendidos.
- Etiquetas (`proyecto`, `dominio`, `entorno`) para repartir costes.
- Athena: workgroups con límite de bytes escaneados por query.

## 7. Seguridad del procesamiento

- Rol IAM por job con mínimo privilegio (solo sus prefijos S3 y sus tablas).
- **Lake Formation** para permisos de tabla/columna/fila y LF-Tags.
- Cifrado SSE-KMS en S3; cifrado de logs CloudWatch y de **Glue security configuration**.
- Jobs en VPC (subnets privadas) con **VPC endpoints** a S3/Glue/KMS → el tráfico no sale a Internet.
- Secretos en **Secrets Manager** (nunca credenciales en el código o en parámetros).
