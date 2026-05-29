# Práctica Big Data 2026 — Parte II

**Predicción de retrasos de vuelos** con arquitectura Lakehouse y procesamiento en streaming.

> **Asignatura**: Big Data — Máster, ETSIT UPM
> **Curso**: 2025-2026
> **Basado en**: https://github.com/Big-Data-ETSIT/practica_creativa

---

## Descripción

Sistema que predice si un vuelo sufrirá retraso, usando un modelo RandomForest entrenado con datos históricos. La arquitectura:

- **Entrenamiento batch** (PySpark): lee dataset desde Iceberg en MinIO y guarda los modelos en el mismo Lakehouse.
- **Predicción en streaming** (Spark Structured Streaming): consume peticiones de Kafka, predice, escribe a Kafka response + Cassandra.
- **Interfaz web** (Flask + SocketIO): el usuario rellena el formulario, Flask produce a Kafka, recibe la predicción por WebSocket y la muestra en tiempo real.
- **Persistencia**: distancias y predicciones en Cassandra; datos y modelos en MinIO/Iceberg.

---

## Arquitectura

```
            Navegador
                |
                | HTTP + WebSocket
                v
         Flask + SocketIO  ----lee distancias---->  Cassandra
                |
                | produce (flight-delay-request)
                v
              Kafka
                |
                | consume
                v
       Spark Structured Streaming (master + worker)
                |                            |
       lee modelo desde                 produce a Kafka response
       Iceberg/MinIO                    + escribe a Cassandra
                |                            |
                v                            v
        Iceberg REST                       Flask consumer Kafka
                |                            |
                v                            v
              MinIO                       WebSocket -> navegador
```

---

## Stack tecnológico

| Componente | Versión |
|---|---|
| Apache Spark | 4.1.1 (Scala 2.13, Java 17) |
| Apache Kafka | 4.2.0 (KRaft) |
| Apache Cassandra | 5.0 |
| Apache Iceberg | 1.10.1 (REST catalog) |
| MinIO | RELEASE.2025-04-08 |
| Flask + SocketIO | 3.0.3 + 5.4.1 |
| Python | 3.11 |

Detalle completo en [`docs/VERSIONS.md`](docs/VERSIONS.md).

---

## Requisitos previos

- Docker >= 24 y Docker Compose v2
- 10 GB RAM disponibles
- ~20 GB de disco
- Conexión a internet (primer build)

En Windows + WSL2: asignar al menos 10 GB de RAM en `.wslconfig`.

---

## Despliegue paso a paso

### 1. Clonar y preparar

```bash
git clone https://github.com/djfuga/practica-bigdata-2026.git
cd practica-bigdata-2026
```

### 2. Descargar los datasets (no incluidos en Git)

```bash
cd data/
curl -Lo simple_flight_delay_features.jsonl.bz2 \
  "http://s3.amazonaws.com/agile_data_science/simple_flight_delay_features.jsonl.bz2"
curl -Lo origin_dest_distances.jsonl \
  "http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl"
cd ..
```

### 3. Configurar variables

```bash
cd deployment/docker
cp .env.example .env
```

### 4. Construir las imágenes propias

```bash
docker compose build spark-master web
```

(La primera vez tarda ~10 min: descarga Spark base + conectores JAR + Python deps.)

### 5. Levantar todo el stack

```bash
docker compose up -d
sleep 90    # Cassandra y Spark tardan
docker compose ps
```

Todos los servicios deben estar `Up (healthy)`.

### 6. Inicializar datos (una sola vez)

#### Ingestar dataset al Lakehouse (Iceberg)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest_training_data.py
```

#### Cargar distancias en Cassandra

```bash
cd ../..   # raíz del repo
docker run --rm --network bigdata-net \
  -v "$(pwd)/src/scripts:/scripts:ro" \
  -v "$(pwd)/data:/data:ro" \
  python:3.11-slim \
  sh -c "pip install --quiet cassandra-driver==3.29.2 && \
         python3 /scripts/load_distances_cassandra.py /data/origin_dest_distances.jsonl"
```

### 7. Entrenar el modelo (genera los 7 modelos en el Lakehouse)

```bash
chmod 777 models
cd deployment/docker
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/train_spark_mllib_model.py
```

Tras entrenar, subir los modelos a MinIO:

```bash
docker cp ../../models bigdata-minio:/tmp/models
docker compose exec minio sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin123 >/dev/null 2>&1
  mc cp --recursive /tmp/models/ local/lakehouse/models/
"
```

### 8. Acceder a la aplicación

| Servicio | URL | Credenciales |
|---|---|---|
| Web de predicción | http://localhost:5001 | — |
| MinIO consola | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master UI | http://localhost:8080 | — |
| Iceberg REST | http://localhost:8181/v1/config | — |

---

## Estructura del repositorio

```
practica-bigdata-2026/
├── data/                              # datasets (no versionados, ver paso 2)
├── deployment/
│   ├── docker/
│   │   ├── compose.yaml
│   │   ├── .env.example
│   │   ├── bootstrap/                 # init scripts de MinIO, Kafka, Cassandra
│   │   └── spark/
│   │       ├── Dockerfile             # imagen Spark con conectores
│   │       ├── conf/                  # spark-defaults.conf (Iceberg + S3)
│   │       ├── tests/                 # smoke tests
│   │       └── jobs/                  # scripts PySpark (ingesta, training)
│   ├── gcp/                           # scripts de GCloud
│   ├── k8s/                           # manifiestos K8s (Fase 3)
│   └── airflow/                       # DAGs (Fase 2)
├── src/
│   ├── flight_prediction/             # job Scala Spark Streaming (Fase 1.9)
│   ├── web/                           # Flask + SocketIO
│   │   ├── app.py
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── templates/predict.html
│   └── scripts/
│       └── load_distances_cassandra.py
├── models/                            # modelos entrenados (no versionados)
├── docs/
│   └── VERSIONS.md
└── README.md
```

---

## Estado de implementación

| Criterio | Pts | Estado |
|---|---|---|
| Data Lakehouse con Iceberg (MinIO) | 1 obl. | COMPLETO |
| Distancias en Cassandra | 1 obl. | COMPLETO (datos cargados + Flask lee de Cassandra) |
| Predicciones Kafka + WebSockets + Cassandra | 1 obl. | ✅ COMPLETO - validado end-to-end |
| Training lee/escribe en Lakehouse | 1 obl. | COMPLETO |
| Docker + docker-compose completo | 1 obl. | ✅ COMPLETO - 8 servicios + job Scala |
| Despliegue en Kubernetes | 3 | PENDIENTE |
| Airflow + MLflow | 1 | PENDIENTE |
| Despliegue en GCloud | 1 | PENDIENTE |
| Observabilidad | 1 | PENDIENTE |

---

## Operación

### Pausar / reanudar entre sesiones

```bash
cd deployment/docker
docker compose stop      # pausa (mantiene datos)
docker compose start     # reanuda
```

### Reset completo (borra todos los datos)

```bash
docker compose down -v
```

### Ver logs de un servicio

```bash
docker compose logs -f <servicio>     # web, kafka, spark-master, cassandra...
```

---

## Notas de seguridad

Las credenciales en `.env.example` y en `spark-defaults.conf` son de **desarrollo local**. El `.env` real está excluido por `.gitignore`. En el despliegue en Kubernetes (Fase 3) se gestionarán con Secrets.

---

## Troubleshooting

- **Cassandra no llega a healthy**: tarda ~60s; espera y reintenta. Ver `docker compose logs cassandra`.
- **Puerto ocupado**: editar puertos en `.env`.
- **WSL2 sin RAM**: ampliar a 10 GB en `%UserProfile%\.wslconfig` y `wsl --shutdown`.
- **`mc` no ve un volumen**: usar un contenedor mc efímero con `-v <volumen>:/path:ro`.
