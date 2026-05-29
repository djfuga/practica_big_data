# Práctica Big Data 2026 — Parte II

**Predicción de retrasos de vuelos** con arquitectura Lakehouse y procesamiento en streaming.

> **Asignatura**: Ingenieria Big Data en la Nube
> **ETSIT — Universidad Politécnica de Madrid** · Curso 2025–2026
> **Basado en**: https://github.com/Big-Data-ETSIT/practica_creativa

---

## 📑 Tabla de contenidos

1. [Descripción](#descripción)
2. [Arquitectura](#arquitectura)
3. [Stack tecnológico](#stack-tecnológico-y-versiones)
4. [Estado de los puntos de evaluación](#estado-de-los-puntos-de-evaluación)
5. [Requisitos previos](#requisitos-previos)
6. [Despliegue paso a paso](#despliegue-paso-a-paso)
7. [Verificación end-to-end](#verificación-end-to-end)
8. [Operación día a día](#operación-día-a-día)
9. [Estructura del repositorio](#estructura-del-repositorio)
10. [Notas de diseño](#notas-de-diseño)
11. [Troubleshooting](#troubleshooting)

---

## Descripción

Sistema que predice si un vuelo sufrirá retraso. Componentes:

- **Lakehouse** (Iceberg sobre MinIO) → almacena el dataset de entrenamiento (~457K vuelos) y los modelos ML.
- **Entrenamiento batch** (PySpark) → lee la tabla Iceberg, entrena un RandomForest y guarda 7 artefactos (Bucketizer + 4 StringIndexers + VectorAssembler + RF) en el Lakehouse.
- **Streaming** (Spark Structured Streaming en Scala 2.13) → consume peticiones de Kafka, aplica el pipeline ML, escribe la predicción simultáneamente a Kafka (response) y a Cassandra.
- **Cassandra** → distancias origen-destino (~4.700 entradas) + persistencia de predicciones.
- **Interfaz web** (Flask + SocketIO) → formulario, produce a Kafka, consumer Kafka en background emite la predicción al navegador por WebSocket.

---

## Arquitectura

```
            Navegador (HTML + Socket.IO)
                       |
                       | HTTP + WebSocket
                       v
              Flask + SocketIO  -----lee distancias-----> Cassandra
                       |                                  (flight_db)
                       | produce
                       v
                     Kafka  (topic: flight-delay-request)
                       |
                       | consume
                       v
            Spark Structured Streaming (Scala 2.13)
                   master + worker (modo distribuido)
                       |
       lee modelos     |     produce a Kafka response  -> consumer Flask
       del Lakehouse   |     escribe a Cassandra           -> WebSocket
                       v                                     -> navegador
              Iceberg REST Catalog
                       |
                       | S3FileIO
                       v
                     MinIO  (bucket: lakehouse/)
                            ├── warehouse/   (tablas Iceberg)
                            ├── raw/         (dataset crudo)
                            └── models/      (7 artefactos ML)
```

---

## Stack tecnológico y versiones

Versiones congeladas (ver [`docs/VERSIONS.md`](docs/VERSIONS.md) para detalles):

| Componente              | Versión                                  | Rol |
|-------------------------|------------------------------------------|-----|
| Apache Spark            | 4.1.1 (Scala 2.13, Java 17)              | Cluster standalone |
| Apache Iceberg          | 1.10.1 (runtime 4.0_2.13)                | Formato Lakehouse |
| Iceberg REST Catalog    | apache/iceberg-rest-fixture:1.10.1       | Gestor de metadata |
| Apache Kafka            | 4.2.0 (KRaft, sin Zookeeper)             | Bus de eventos |
| Apache Cassandra        | 5.0                                      | NoSQL distribuida |
| MinIO                   | RELEASE.2025-04-08                       | Object storage S3 |
| Flask + Flask-SocketIO  | 3.0.3 + 5.4.1                            | Servidor web + WS |
| Cassandra Java driver   | 4.17.0 (+ shaded-guava 25.1)             | Sink desde Spark |
| sbt + Scala compiler    | 1.10.7 + 2.13.16                         | Build del job |

---

## Estado de los puntos de evaluación

| Criterio | Pts | Estado |
|---|---|---|
| **1. Data Lakehouse con Iceberg (MinIO)** | 1 obl. | ✅ COMPLETO |
| **2. Distancias en Cassandra** | 1 obl. | ✅ COMPLETO |
| **3. Predicciones Kafka + WebSockets + Cassandra** | 1 obl. | ✅ COMPLETO |
| **4. Training lee/escribe en Lakehouse** | 1 obl. | ✅ COMPLETO |
| **5. Docker + docker-compose completo** | 1 obl. | ✅ COMPLETO |
| 6. Despliegue en Kubernetes | 3 | ⏳ Pendiente |
| 7. Airflow + MLflow | 1 | ⏳ Pendiente |
| 8. Despliegue en GCloud | 1 | ⏳ Pendiente |
| 9. Observabilidad / optimización | 1 | ⏳ Pendiente |

**Servicios validados en modo distribuido**: el entrenamiento y las predicciones se ejecutan en el cluster Spark (master + worker en contenedores separados), no en `local[*]`.

---

## Requisitos previos

| Recurso | Mínimo | Recomendado |
|---|---|---|
| **Docker** | 24.x | 28.x |
| **Docker Compose** | v2 | v2 |
| **RAM disponible para Docker** | 8 GB | 10 GB |
| **Disco libre** | 15 GB | 20 GB |
| **Conexión a internet** | Sí (primer build) | — |

### Windows + WSL2

Asignar al menos 10 GB de RAM a WSL2 en `%UserProfile%\.wslconfig`:

```ini
[wsl2]
memory=10GB
processors=8
swap=4GB
```

Después: `wsl --shutdown` desde PowerShell y reabrir terminal Ubuntu.

---

## Despliegue paso a paso

### 1. Clonar el repositorio

```bash
git clone https://github.com/djfuga/practica-bigdata-2026.git
cd practica-bigdata-2026
```

### 2. Descargar los datasets

⚠️ Los datasets **no se versionan en Git** (son pesados). Hay que descargarlos:

```bash
cd data/

curl -Lo simple_flight_delay_features.jsonl.bz2 \
  "http://s3.amazonaws.com/agile_data_science/simple_flight_delay_features.jsonl.bz2"

curl -Lo origin_dest_distances.jsonl \
  "http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl"

ls -lah *.jsonl*    # debe mostrar ~4.5 MB + ~218 KB
cd ..
```

### 3. Configurar variables de entorno

```bash
cd deployment/docker
cp .env.example .env
cd ../..
```

> 💡 El `.env` por defecto trae credenciales de desarrollo local. Para uso normal de la práctica no hay que tocar nada.

### 4. Preparar permisos del directorio de modelos

El cluster Spark escribe los modelos a una carpeta del host vía bind mount. Como Spark corre con UID 185 dentro del contenedor, damos permisos abiertos a la carpeta:

```bash
chmod 777 models
```

### 5. Compilar el job Scala de streaming

El JAR del job no se versiona (es un binario). Hay que compilarlo:

```bash
cd src/flight_prediction

docker run --rm \
  -v "$(pwd)":/app \
  -v sbt-cache:/root/.sbt \
  -v sbt-ivy:/root/.ivy2 \
  -w /app \
  sbtscala/scala-sbt:eclipse-temurin-17.0.13_11_1.10.7_2.13.15 \
  sbt clean assembly

# Copiar el JAR a la ruta que Spark monta como volumen
cp target/scala-2.13/flight_prediction.jar ../../deployment/docker/spark/jobs/

cd ../..
```

⚠️ El primer build sbt descarga muchas dependencias (~5–10 min). Los siguientes son rápidos gracias a los volúmenes `sbt-cache` y `sbt-ivy`.

### 6. Construir las imágenes propias

```bash
cd deployment/docker
docker compose build spark-master web
```

Toma ~10 min la primera vez (Spark base + conectores Iceberg/Kafka/Cassandra + Python deps + Flask).

### 7. Levantar todo el stack

```bash
docker compose up -d
echo "Esperando 90s a que arranquen Cassandra y Spark..."
sleep 90

docker compose ps
```

Estado esperado: 7 servicios `Up (healthy)` + 3 `*-init` en `Exited (0)`:

```
bigdata-cassandra      Up (healthy)
bigdata-iceberg-rest   Up (healthy)
bigdata-kafka          Up (healthy)
bigdata-minio          Up (healthy)
bigdata-spark-master   Up (healthy)
bigdata-spark-worker   Up (healthy)
bigdata-web            Up (healthy)
```

### 8. Inicializar los datos (una sola vez)

#### 8.1 Ingestar el dataset al Lakehouse Iceberg

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest_training_data.py
```

Debe terminar con `INGESTA ICEBERG: OK` (457.013 registros en `lakehouse.flights.training_data`).

#### 8.2 Cargar las distancias en Cassandra

```bash
cd ../..   # raíz del repo

docker run --rm --network bigdata-net \
  -v "$(pwd)/src/scripts:/scripts:ro" \
  -v "$(pwd)/data:/data:ro" \
  python:3.11-slim \
  sh -c "pip install --quiet cassandra-driver==3.29.2 && \
         python3 /scripts/load_distances_cassandra.py /data/origin_dest_distances.jsonl"

cd deployment/docker
```

Debe terminar con `CARGA DISTANCIAS CASSANDRA: OK` (4.696 distancias).

### 9. Entrenar el modelo

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/train_spark_mllib_model.py
```

Toma ~3–5 min. Termina con `ENTRENAMIENTO: OK` y un accuracy en torno a 0.58.

Los 7 modelos se generan en `./models/` (visibles desde el host). Subirlos al Lakehouse:

```bash
docker cp ../../models bigdata-minio:/tmp/models

docker compose exec minio sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin123 >/dev/null 2>&1
  mc cp --recursive /tmp/models/ local/lakehouse/models/
  echo 'Modelos subidos al Lakehouse:'
  mc ls local/lakehouse/models/
"
```

### 10. Lanzar el job Scala de streaming

Es un proceso de streaming: corre indefinidamente. Se lanza en una **terminal dedicada** para ver logs en vivo:

```bash
# Abrir una SEGUNDA terminal, ir a deployment/docker, y ejecutar:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --class es.upm.dit.ging.predictor.MakePrediction \
  /opt/spark/jobs/flight_prediction.jar
```

Esperar a ver los 3 marcadores `>>>`:

```
>>> Modelos cargados correctamente
>>> Sink Kafka activo -> topic=flight-delay-classification-response
>>> Sink Cassandra activo -> flight_db.flight_delay_predictions
```

A partir de ahí, el job está listo para procesar peticiones.

### 11. Acceder a la aplicación

| Servicio | URL | Credenciales |
|---|---|---|
| **Web de predicción** | http://localhost:5001 | — |
| MinIO consola | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark Master UI | http://localhost:8080 | — |
| Spark Worker UI | http://localhost:8081 | — |
| Iceberg REST | http://localhost:8181/v1/config | — |

---

## Verificación end-to-end

### Smoke test del Lakehouse (Iceberg sobre MinIO en modo distribuido)

```bash
cd deployment/docker
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/tests/test_iceberg.py
```

Debe terminar con `SMOKE TEST ICEBERG: OK`.

### Flujo completo predicción

1. Abrir http://localhost:5001 en el navegador.
2. Comprobar que aparece `WebSocket: conectado` abajo.
3. Rellenar el formulario (los valores por defecto sirven) y pulsar **Predecir**.
4. En ~5–15 segundos debe aparecer un resultado como **"Retraso moderado (clase 2)"**.

### Verificar persistencia en Cassandra

```bash
docker compose exec cassandra cqlsh -k flight_db \
  -e "SELECT COUNT(*) FROM flight_delay_predictions;"

docker compose exec cassandra cqlsh -k flight_db \
  -e "SELECT uuid, origin, dest, prediction FROM flight_delay_predictions LIMIT 5;"
```

Cada predicción que hagas en la web añade una fila.

### Verificar mensaje en Kafka response

```bash
docker run --rm --network bigdata-net apache/kafka:4.2.0 \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic flight-delay-classification-response \
  --from-beginning --max-messages 3 --timeout-ms 5000
```

---

## Operación día a día

### Pausar entre sesiones (libera RAM, mantiene datos)

```bash
cd deployment/docker
docker compose stop
```

### Reanudar

```bash
cd deployment/docker
docker compose start
sleep 60
docker compose ps
```

### Relanzar el job Scala (no se reinicia solo)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class es.upm.dit.ging.predictor.MakePrediction \
  /opt/spark/jobs/flight_prediction.jar
```

### Parar el job Scala

`Ctrl+C` en la terminal donde lo lanzaste. O si está en background:

```bash
docker compose exec spark-master sh -c "pkill -9 -f MakePrediction"
```

### Reset total (borra TODOS los datos)

```bash
docker compose down -v
```

Tras esto hay que repetir los pasos 7–10 (init de datos + training + lanzamiento del job).

### Ver logs

```bash
docker compose logs -f web              # Flask
docker compose logs -f spark-master     # Spark
docker compose logs -f cassandra        # Cassandra
```

---

## Estructura del repositorio

```
practica-bigdata-2026/
├── data/                                 # datasets (no versionados)
│   └── README.md                         # instrucciones de descarga
├── deployment/
│   ├── docker/
│   │   ├── compose.yaml                  # 8 servicios + 3 init
│   │   ├── .env.example
│   │   ├── bootstrap/                    # init scripts MinIO/Kafka/Cassandra
│   │   └── spark/
│   │       ├── Dockerfile                # imagen Spark con conectores
│   │       ├── conf/spark-defaults.conf  # Iceberg + S3FileIO
│   │       ├── tests/test_iceberg.py
│   │       └── jobs/
│   │           ├── ingest_training_data.py
│   │           ├── train_spark_mllib_model.py
│   │           └── flight_prediction.jar  # generado (no versionado)
│   ├── gcp/                              # scripts GCloud (futuro)
│   ├── k8s/                              # manifiestos K8s (futuro)
│   └── airflow/                          # DAGs (futuro)
├── src/
│   ├── flight_prediction/                # proyecto sbt Scala 2.13
│   │   ├── build.sbt
│   │   ├── project/
│   │   └── src/main/scala/es/upm/dit/ging/predictor/
│   │       └── MakePrediction.scala
│   ├── web/                              # servidor Flask
│   │   ├── app.py
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── templates/predict.html
│   └── scripts/
│       └── load_distances_cassandra.py
├── models/                               # modelos (no versionados)
├── docs/
│   └── VERSIONS.md
└── README.md
```

---

## Notas de diseño

### Por qué REST catalog para Iceberg

La combinación Iceberg 1.10 + Spark 4.1 + MinIO + Hadoop catalog directo provoca un *dependency hell* del AWS SDK (v1 vs v2). El **Iceberg REST Catalog** (`apache/iceberg-rest-fixture:1.10.1`) aísla la gestión de metadata en un servicio dedicado y elimina el conflicto. Spark solo necesita `iceberg-aws-bundle` (SDK v2 puro). Además es el patrón estándar de la industria (Databricks Unity, Snowflake Polaris).

### Por qué driver Java nativo para Cassandra (no spark-cassandra-connector)

El `spark-cassandra-connector` oficial solo soporta hasta Spark 3.5. Con Spark 4.1 daría errores de API. La solución profesional es usar el **driver Java nativo de DataStax** (`CqlSession`) dentro de `foreachBatch`, que es agnóstico a la versión de Spark.

Tres JARs necesarios del driver:
- `java-driver-core-shaded-4.17.0` (driver, con Netty/Jackson shaded)
- `java-driver-query-builder-4.17.0` (constructor de queries)
- `java-driver-shaded-guava-25.1-jre-graal-sub-1` (Guava re-empaquetada, **imprescindible** o falla con `NoClassDefFoundError: ImmutableList`)

### Por qué bind mount para los modelos (no named volume)

Spark ML, al guardar modelos, crea directorios anidados desde varios executors. Un *named volume* dio problemas de permisos (UID 185 vs root) y de concurrencia. Un **bind mount** a `./models` del host garantiza permisos consistentes (con `chmod 777` único) y los modelos quedan accesibles para inspección sin entrar al contenedor.

### Sobre seguridad de credenciales

Las credenciales en `.env.example` y `spark-defaults.conf` son **de desarrollo local** (MinIO/Cassandra en una red Docker aislada). El `.env` real con secretos está excluido por `.gitignore`. Para Kubernetes (Fase futura) las credenciales se gestionarán mediante Secrets.

---

## Troubleshooting

### Cassandra tarda en arrancar (~60s) y los healthchecks fallan

Es normal. Esperar y revisar `docker compose logs cassandra --tail=50`. Si tras 2 min sigue sin estar `healthy`, verificar que WSL2 tiene al menos 8 GB de RAM.

### Puerto ocupado en el host

Editar puertos en `deployment/docker/.env` (`WEB_PORT`, `KAFKA_EXTERNAL_PORT`, etc.) y `docker compose up -d` de nuevo.

### El job Scala falla con `NoClassDefFoundError: ImmutableList`

Significa que falta el JAR `java-driver-shaded-guava-25.1`. Reconstruir la imagen Spark:

```bash
cd deployment/docker
docker compose build spark-master
docker compose up -d --force-recreate spark-master spark-worker
```

### "Procesando..." en la web no termina

Verificar que el job Scala está corriendo: http://localhost:8080 debe mostrar la app `FlightDelayStreamingPredictor` activa. Si no está, relanzarla con el comando del paso 10.

### Reset total si nada funciona

```bash
cd deployment/docker
docker compose down -v
chmod 777 ../../models
# repetir pasos 7-10
```

### El primer `docker compose build` falla por red

Maven Central (`repo1.maven.org`) y Docker Hub no suelen estar bloqueados, pero hay redes universitarias restrictivas. Probar desde otra conexión o usar VPN.

---

## Licencia

Práctica académica con fines educativos. Basada en
[Big-Data-ETSIT/practica_creativa](https://github.com/Big-Data-ETSIT/practica_creativa)
(que a su vez se basa en el libro *Agile Data Science 2.0* de Russell Jurney).
