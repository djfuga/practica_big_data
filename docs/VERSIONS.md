# 📌 Matriz de versiones congelada — Práctica Big Data 2026 (Parte II)

> **CRÍTICO**: Cualquier desviación de estas versiones debe ser aprobada explícitamente.
> Esta tabla es la fuente única de verdad para todo el proyecto.

## Stack base (impuesto por el README del repo de la práctica)

| Componente | Versión | Origen / Notas |
|---|---|---|
| **JDK** | OpenJDK 17 (`17.0.14-amzn`) | README: "chage or instal de jdk 17" |
| **Python** | 3.11 (para nuestros contenedores) | README sugiere 3.7 pero está obsoleto; 3.11 es compatible con PySpark 4.x |
| **Scala** | 2.13.16 | README: "Mandatory version 2.13.0". 2.13.16 es binary-compatible con 2.13.0 |
| **Apache Spark** | 4.1.1 | README: "Mandatory version 4.1.1" |
| **Apache Kafka** | kafka_2.13-4.2.0 (KRaft mode) | README: "Mandatory version kafka_2.13_4.2.0 with KRaft" |
| **sbt** | 1.10.7 | El 1.3.2 del repo es de 2019; subimos a 1.10.x para soportar Scala 2.13 |

## Stack añadido por nosotros (Parte II)

| Componente | Versión | Justificación |
|---|---|---|
| **Apache Cassandra** | 5.0 | Última estable. Reemplazo de Mongo según PDF Parte II |
| **MinIO** | RELEASE.2025-04-* | S3-compatible para el Data Lakehouse |
| **Apache Iceberg** | 1.10.1 (runtime 4.0_2.13) | ⚠️ No hay runtime oficial para Spark 4.1; usamos el de Spark 4.0 |
| **Apache Airflow** | 2.10.5 | Más moderno que el 2.1.4 del repo (incompatible con Python 3.11+) |
| **MLflow** | 2.18.0 | Última estable |

## Conectores Spark (los críticos)

| Paquete Maven | Versión | Compatibilidad verificada |
|---|---|---|
| `org.apache.spark:spark-sql-kafka-0-10_2.13` | 4.1.1 | ✅ Match exacto con Spark |
| `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13` | 1.10.1 | ⚠️ Para Spark 4.0, lo usamos en 4.1 (riesgo asumido) |
| `org.apache.iceberg:iceberg-aws-bundle` | 1.10.1 | Para integración S3/MinIO |
| `org.mongodb.spark:mongo-spark-connector_2.13` | 10.5.0 | ✅ Soporta Spark 3.2+. Solo mientras hagamos pasos intermedios |
| `com.datastax.spark:spark-cassandra-connector_2.13` | 3.5.1 | ⚠️ Para Spark 3.5; con Spark 4 puede dar problemas. Plan B: driver Java directo |

## Drivers y librerías Python (Flask y scripts auxiliares)

| Paquete pip | Versión | Uso |
|---|---|---|
| `flask` | 3.0.x | Servidor web |
| `flask-socketio` | 5.x | WebSockets (nuevo requisito Parte II) |
| `kafka-python` | 2.0.6 | Producer y consumer Python |
| `cassandra-driver` | 3.29.x | Reemplazo del cliente pymongo |
| `pymongo` | 4.x | (Lo mantenemos hasta migrar completamente) |
| `pyspark` | 4.1.1 | Match exacto con Spark |
| `mlflow` | 2.18.0 | Tracking de modelos |
| `boto3` | 1.35.x | Subir/bajar a MinIO |
| `iso8601` | 2.x | Manejo de fechas |
| `joblib` | 1.4.x | Serialización |

## Imágenes Docker base

| Servicio | Imagen | Tag |
|---|---|---|
| Spark Master/Worker | `apache/spark` o build propio | `4.1.1-scala2.13-java17-python3-ubuntu` |
| Kafka | `apache/kafka` (KRaft) | `4.2.0` |
| Cassandra | `cassandra` | `5.0` |
| MinIO | `minio/minio` | `RELEASE.2025-04-08T15-41-24Z` |
| MongoDB (transitorio) | `mongo` | `7.0.17` |
| Airflow | `apache/airflow` | `2.10.5-python3.11` |
| MLflow | `ghcr.io/mlflow/mlflow` | `v2.18.0` |
| Python/Flask base | `python` | `3.11-slim` |

## Versiones de la stack de GCP

| Recurso | Configuración |
|---|---|
| VM Compute Engine | `e2-standard-4` (4 vCPU, 16 GB RAM) |
| Disco | `pd-standard` 50 GB |
| GKE (Fase 3) | Autopilot |
| Artifact Registry | Docker repo `europe-southwest1` |
| Región GCP | `europe-southwest1` (Madrid) — más cercano para baja latencia |

## ⚠️ ACTUALIZACIÓN (decisión de arquitectura Iceberg)

Tras varias iteraciones, el catálogo Iceberg **Hadoop directo sobre MinIO** dio problemas
irresolubles de dependencias AWS SDK (ClassNotFoundException ObjectTransfer, conflictos v1/v2).

**Solución adoptada: Iceberg REST Catalog**
- Servicio: `apache/iceberg-rest-fixture:1.10.1` (mismo número que el runtime Iceberg)
- El catálogo REST gestiona metadata y acceso S3 vía S3FileIO (AWS SDK v2)
- Spark usa `type=rest`, `uri=http://iceberg-rest:8181`
- Imagen Spark: SIN hadoop-aws, SOLO iceberg-aws-bundle (SDK v2 puro)
- Config crítica: `spark.sql.catalog.lakehouse.client.region=us-east-1` (el SDK v2 exige región)

| Componente | Valor final |
|---|---|
| Catálogo Iceberg | REST (`apache/iceberg-rest-fixture:1.10.1`) |
| io-impl | `org.apache.iceberg.aws.s3.S3FileIO` |
| Región S3 (obligatoria SDK v2) | us-east-1 |
| hadoop-aws | ❌ ELIMINADO (causaba conflictos) |
| aws-java-sdk-bundle v1 | ❌ ELIMINADO |

✅ VALIDADO: Iceberg 1.10.1 sobre Spark 4.1.1 escribe/lee tablas en MinIO en modo distribuido.
