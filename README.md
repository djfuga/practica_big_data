# Práctica Big Data 2026 — Parte II

**Predicción de retrasos de vuelos** con arquitectura Lakehouse y procesamiento en streaming.

> **Asignatura**: Big Data — Máster, ETSIT UPM
> **Curso**: 2025-2026
> **Basado en**: https://github.com/Big-Data-ETSIT/practica_creativa

---

## 📑 Tabla de contenidos

1. [Descripción](#descripción)
2. [Arquitectura](#arquitectura)
3. [Stack tecnológico y versiones](#stack-tecnológico-y-versiones)
4. [Requisitos previos](#requisitos-previos)
5. [Estructura del repositorio](#estructura-del-repositorio)
6. [Despliegue con Docker Compose](#despliegue-con-docker-compose)
7. [Verificación de servicios](#verificación-de-servicios)
8. [Estado de implementación](#estado-de-implementación)
9. [Notas de seguridad](#notas-de-seguridad)
10. [Troubleshooting](#troubleshooting)

---

## Descripción

Sistema que predice si un vuelo sufrirá retraso, usando un modelo RandomForest entrenado
con datos históricos. La arquitectura permite:

- **Entrenamiento batch** con PySpark, leyendo y escribiendo en un Data Lakehouse (Iceberg sobre MinIO).
- **Predicción en streaming** con Spark Structured Streaming, consumiendo peticiones de Kafka.
- **Interfaz web** (Flask + WebSockets) donde el usuario introduce los datos del vuelo y recibe la predicción en tiempo real.
- **Persistencia** de distancias y predicciones en Cassandra.

---

## Arquitectura

```
                          +---------------------+
                          |   Navegador (web)   |
                          +----------+----------+
                                     | HTTP + WebSocket
                          +----------v----------+
                          |   Flask + SocketIO  |
                          +-----+----------+----+
                  produce a     |          |  lee distancias
              flight-delay-     |          |
                  request       |          v
                          +-----v----+  +--------------+
                          |  Kafka   |  |  Cassandra   |
                          | (KRaft)  |  | (distancias  |
                          +-----+----+  | +predicciones|
                                |       +------^-------+
                consume         |              | escribe
                                v              | prediccion
                       +----------------------------+
                       |   Spark Structured         |
                       |   Streaming (4.1.1)        |
                       |   master + worker          |
                       +------+--------------+------+
                  lee modelo  |              | produce a
                              |              | flight-delay-
                     +--------v-----+        | classification-response
                     |   Iceberg    |        |  (-> Kafka -> WebSocket)
                     | REST Catalog |
                     +------+-------+
                            | S3FileIO
                     +------v-------+
                     |    MinIO     |  <- Data Lakehouse (datos + modelos)
                     +--------------+
```

---

## Stack tecnológico y versiones

Las versiones se respetan según el repositorio original de la práctica. Detalle completo en
[`docs/VERSIONS.md`](docs/VERSIONS.md).

| Componente | Versión | Notas |
|---|---|---|
| Apache Spark | 4.1.1 (Scala 2.13, Java 17) | Cluster standalone master+worker |
| Apache Kafka | 4.2.0 (KRaft, sin Zookeeper) | Topics de request y response |
| Apache Cassandra | 5.0 | Distancias + predicciones |
| Apache Iceberg | 1.10.1 (runtime 4.0_2.13) | Vía REST Catalog |
| Iceberg REST Catalog | apache/iceberg-rest-fixture:1.10.1 | Gestión de metadata |
| MinIO | RELEASE.2025-04-08 | Object storage S3-compatible |
| MongoDB | 7.0.17 | (transitorio, en migración a Cassandra) |
| Apache Airflow | 2.10.x | Orquestación (Fase 2) |
| MLflow | 2.18.x | Tracking de modelos (Fase 2) |

---

## Requisitos previos

- **Docker** >= 24 y **Docker Compose** v2
- **8 GB RAM** mínimo disponibles para Docker (recomendado 10 GB)
- ~20 GB de espacio en disco (imágenes + datos)
- Conexión a internet para la primera construcción (descarga de imágenes y JARs)

> En **Windows con WSL2**: asignar al menos 10 GB de RAM a WSL2 vía `.wslconfig`.

---

## Estructura del repositorio

```
practica-bigdata-2026/
├── deployment/
│   ├── docker/
│   │   ├── compose.yaml          # orquestación de todos los servicios
│   │   ├── .env.example          # plantilla de variables (copiar a .env)
│   │   ├── bootstrap/            # scripts de init (MinIO, Kafka, Cassandra)
│   │   └── spark/                # Dockerfile + config de Spark
│   ├── gcp/                      # scripts de despliegue en Google Cloud
│   ├── k8s/                      # manifiestos Kubernetes (Fase 3)
│   └── airflow/                  # DAGs (Fase 2)
├── src/
│   ├── flight_prediction/        # job Scala de Spark Streaming
│   ├── training/                 # script PySpark de entrenamiento
│   ├── web/                      # servidor Flask
│   └── scripts/                  # utilidades
├── docs/
│   └── VERSIONS.md               # matriz de versiones detallada
└── README.md
```

---

## Despliegue con Docker Compose

### 1. Clonar el repositorio

```bash
git clone <URL_DE_TU_REPO>
cd practica-bigdata-2026
```

### 2. Configurar variables de entorno

```bash
cd deployment/docker
cp .env.example .env
# (opcional) editar .env para cambiar credenciales
```

### 3. Construir la imagen de Spark

La primera vez descarga la imagen base y los conectores (~10 min):

```bash
docker compose build spark-master
```

### 4. Levantar todo el stack

```bash
docker compose up -d
```

El arranque completo tarda ~60-90s (Cassandra es el más lento).

### 5. Comprobar el estado

```bash
docker compose ps
```

Todos los servicios deben aparecer como `Up (healthy)`; los `*-init` como `Exited (0)`.

---

## Verificación de servicios

| Servicio | URL / Comando | Esperado |
|---|---|---|
| MinIO consola | http://localhost:9001 | Login: minioadmin / minioadmin123 |
| Spark Master UI | http://localhost:8080 | 1 worker ALIVE |
| Spark Worker UI | http://localhost:8081 | Worker activo |
| Iceberg REST | `curl http://localhost:8181/v1/config` | JSON de config |
| Kafka topics | ver Troubleshooting | 2 topics de la práctica |
| Cassandra | `docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES"` | flight_db |

### Smoke test del Lakehouse (Iceberg + Spark distribuido)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/tests/test_iceberg.py
```

Debe terminar con `SMOKE TEST ICEBERG: OK`.

---

## Estado de implementación

Seguimiento de los criterios de evaluación de la Parte II:

| Criterio | Puntos | Estado |
|---|---|---|
| Data Lakehouse con Iceberg (MinIO) | 1 obl. | ✅ COMPLETO - tabla flights.training_data (457K registros) |
| Distancias en Cassandra | 1 obl. | PENDIENTE |
| Predicciones en Kafka + WebSockets + Cassandra | 1 obl. | PENDIENTE |
| Training lee/escribe en Lakehouse | 1 obl. | PENDIENTE |
| Docker + docker-compose completo | 1 obl. | EN PROGRESO - 6/9 servicios listos |
| Despliegue en Kubernetes | 3 | PENDIENTE |
| Airflow + MLflow | 1 | PENDIENTE |
| Despliegue en GCloud | 1 | PENDIENTE |
| Observabilidad / optimización | 1 | PENDIENTE |

### Servicios desplegados actualmente

- [x] MinIO (object storage)
- [x] Kafka 4.2.0 KRaft (topics `flight-delay-request` y `flight-delay-classification-response`)
- [x] Cassandra 5.0 (keyspace `flight_db`, tablas `origin_dest_distances` y `flight_delay_predictions`)
- [x] Iceberg REST Catalog
- [x] Spark 4.1.1 cluster (master + worker, modo distribuido validado)
- [] MongoDB (transitorio)
- [] Flask web
- [] Job Scala de streaming

---

## Notas de seguridad

**Entorno de desarrollo**: las credenciales en `.env.example` y en
`deployment/docker/spark/conf/spark-defaults.conf` son de **desarrollo local** (MinIO/Cassandra
en contenedores aislados). No usar en producción.

- El fichero `.env` con las credenciales reales está excluido por `.gitignore`.
- En el despliegue en Kubernetes (Fase 3) las credenciales se gestionarán mediante **Secrets**.

---

## Troubleshooting

### Pausar / reanudar entre sesiones

```bash
docker compose stop      # pausa (mantiene datos)
docker compose start     # reanuda
```

### Listar topics de Kafka

```bash
docker run --rm --network bigdata-net apache/kafka:4.2.0 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

### Cassandra tarda en arrancar

Es normal (~60s). Si el healthcheck falla, esperar y reintentar. Ver logs:
```bash
docker compose logs cassandra --tail=50
```

### Reset total (borra todos los datos)

```bash
docker compose down -v
```

### Puerto ocupado

Editar los puertos en `.env` (p.ej. `MINIO_API_PORT`, `KAFKA_EXTERNAL_PORT`).
