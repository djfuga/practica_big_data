# Práctica Big Data 2026 – Parte II

**Predicción de retrasos de vuelos en tiempo real** con Spark, Kafka, Cassandra,
Iceberg Lakehouse, MLflow, Airflow, MinIO y Flask.

---

## Estado de cumplimiento

| Punto | Descripción | Estado |
|-------|-------------|--------|
| 1 (obl) | Datos de entrenamiento en Iceberg sobre MinIO | Completado |
| 2 (obl) | Distancias en Cassandra | Completado |
| 3 (obl) | Predicciones por Kafka + WebSocket + sink Cassandra | Completado |
| 4 (obl) | Training lee y escribe en el Lakehouse | Completado |
| 5 (obl) | Despliegue Docker Compose | Completado |
| 6 | Despliegue completo en Kubernetes | Completado (local con kind) |
| 7 | Airflow + MLflow integrados | Completado |
| 8 | Despliegue en GCloud (GKE) | Pendiente (opcional) |
| 9 | Observabilidad / optimización | Pendiente |

Estado: 9/10 puntos asegurados.

---

## Arquitectura

```
+-------------+    +----------+    +---------------+   +-----------------+
|  Navegador  | <- |  Flask   | <- |     Kafka     | <-|  Spark Scala    |
|  + jQuery   | WS | SocketIO |    |  (2 topics)   |   |  Streaming Job  |
+-------------+    +----------+    +---------------+   +--------+--------+
                       |  ^                                      |
                       |  | distancias                           v predicción
                       v  |                               +-------------+
                  +----------+                            |  Cassandra  |
                  |Cassandra | <--------------------------+ (predict. + |
                  | flight_db|                            |  distances) |
                  +----------+                            +-------------+

       +----------------------+    +---------------+    +--------------+
       |  PySpark Training    | -> | Iceberg REST  | -> |    MinIO     |
       |  (RandomForest)      |    |   catalog     |    |  (Lakehouse) |
       +----------------------+    +---------------+    +--------------+
                  ^
       +----------------------+    +----------+
       |     Airflow DAGs     | -> |  MLflow  |
       | (retrain + cleanup)  |    | Tracking |
       +----------------------+    +----------+
```

---

## Componentes y versiones

| Componente | Versión |
|---|---|
| Apache Spark | 4.1.1 (Scala 2.13, Java 17) |
| Apache Kafka | 4.2.0 (KRaft, sin Zookeeper) |
| Apache Iceberg | 1.10.1 (REST catalog) |
| Apache Cassandra | 5.0 |
| Apache Airflow | 2.10.5 (LocalExecutor) |
| MLflow | 2.18.0 |
| MinIO | RELEASE.2025-04-08 |
| PostgreSQL | 16 (backend Airflow + MLflow) |
| Flask | 3.0 + Flask-SocketIO |
| Kubernetes (kind) | v1.31.0 |
| Helm | 3.21 |

---

## Estructura del repositorio

```
practica_big_data/
├── src/                                  # Códigos fuente
│   ├── flight_prediction/                # Job Scala (Spark Streaming)
│   └── web/                              # Servidor Flask
├── deployment/
│   ├── docker/                           # Stack Docker Compose
│   │   ├── compose.yaml
│   │   ├── .env.example
│   │   ├── bootstrap/                    # Scripts init (postgres, kafka, cassandra)
│   │   ├── spark/                        # Dockerfile + conf + jobs PySpark
│   │   ├── web/                          # Dockerfile Flask
│   │   ├── airflow/                      # Dockerfile (Python+Docker CLI)
│   │   └── mlflow/                       # Dockerfile
│   ├── airflow/dags/                     # DAGs (retrain, cleanup)
│   └── k8s/                              # Manifiestos Kubernetes
│       ├── kind-config.yaml
│       ├── 00-base/                      # Namespace + Secrets + ConfigMap
│       ├── 10-minio/
│       ├── 20-kafka/
│       ├── 30-cassandra/
│       ├── 40-iceberg-rest/
│       ├── 50-spark/                     # Master, worker, ingest, streaming
│       └── 60-web/
├── data/                                 # Datasets locales (gitignored)
└── models/                               # Modelos entrenados (gitignored)
```

---

## Requisitos

- Linux (probado en WSL2 Ubuntu 24.04)
- Docker 28+
- Docker Compose 2.30+
- 10 GB RAM mínimo, 4 cores
- Para Kubernetes local: `kind` v0.24+, `kubectl` v1.31+

---

## Despliegue A: Docker Compose (puntos obligatorios + Airflow/MLflow)

### 1. Preparar variables de entorno

```bash
cd deployment/docker
cp .env.example .env
# Editar .env solo si quieres cambiar credenciales por defecto
```

### 2. Construir imágenes propias

```bash
docker compose build spark-master web mlflow airflow-webserver
```

### 3. Levantar todos los servicios

```bash
docker compose up -d
```

Servicios que arranca:
- MinIO + bucket lakehouse
- Kafka KRaft + 2 topics
- Cassandra + keyspace + carga de 4696 distancias
- Iceberg REST catalog
- Spark master + worker
- Web Flask
- PostgreSQL (backend Airflow + MLflow)
- MLflow tracking server
- Airflow init + webserver + scheduler

Espera 3 min hasta que todos los healthchecks estén verdes:

```bash
docker compose ps
```

### 4. Ingesta inicial del lakehouse + entrenamiento

```bash
# Ingestar dataset crudo a tabla Iceberg
docker compose exec spark-master spark-submit \
  /opt/spark/jobs/ingest_training_data.py

# Entrenar el modelo (lee de Iceberg, escribe modelos a Iceberg/MinIO)
docker compose exec spark-master spark-submit \
  /opt/spark/jobs/train_spark_mllib_model.py
```

### 5. Lanzar el job de predicción streaming

```bash
docker compose exec -d spark-master spark-submit \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/flight_prediction.jar
```

### 6. Acceder a las interfaces

| Servicio | URL | Credenciales |
|---|---|---|
| Web (formulario predicción) | http://localhost:5000 | -- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master UI | http://localhost:8080 | -- |
| Iceberg REST | http://localhost:8181 | -- |
| Airflow | http://localhost:8082 | admin / admin |
| MLflow | http://localhost:5050 | -- |

### 7. Operar Airflow

```bash
# Despausar los DAGs
docker compose exec airflow-scheduler airflow dags unpause retrain_flight_model
docker compose exec airflow-scheduler airflow dags unpause cleanup_old_predictions

# Disparar manualmente (opcional)
docker compose exec airflow-scheduler airflow dags trigger retrain_flight_model
docker compose exec airflow-scheduler airflow dags trigger cleanup_old_predictions
```

### 8. Apagar

```bash
docker compose stop                     # Mantiene volúmenes
docker compose down                     # Borra contenedores, mantiene volúmenes
docker compose down -v                  # Borra TODO incluyendo datos
```

---

## Despliegue B: Kubernetes local con kind

Estado: implementado y funcionando end-to-end. La infraestructura completa
(MinIO, Kafka, Cassandra, Iceberg REST, Spark master/worker/streaming, Web)
se despliega en un cluster kind con manifiestos nativos K8s. Se han adaptado
las imágenes para no depender de bind mounts (datasets, scripts y modelos
empotrados en la imagen Spark). Verificación: 4696 distancias en Cassandra,
457013 registros en `lakehouse.flights.training_data`, predicción end-to-end
operativa desde el formulario web.

### 1. Crear cluster kind

```bash
cd deployment/k8s
kind create cluster --config kind-config.yaml
```

El config expone 8 NodePorts al host (30050, 30090-30091, 30180-30183, 30002, 30094).

### 2. Construir y cargar imágenes propias en el cluster

Para que kind pueda usar las imágenes que no están en DockerHub:

```bash
# Reconstruir si hubo cambios
cd ~/practica_big_data/deployment/docker
docker compose build spark-master web

# Cargar en el cluster (cada vez que cambie la imagen)
kind load docker-image bigdata/spark:4.1.1 --name bigdata
kind load docker-image bigdata/web:1.0     --name bigdata
```

### 3. Aplicar manifiestos por orden

```bash
cd ~/practica_big_data/deployment/k8s

kubectl apply -f 00-base/
kubectl apply -f 10-minio/
sleep 30                              # MinIO debe estar Ready antes de los bootstraps de los demás
kubectl apply -f 20-kafka/
kubectl apply -f 30-cassandra/
kubectl apply -f 40-iceberg-rest/
kubectl apply -f 50-spark/01-master-deployment.yaml
kubectl apply -f 50-spark/02-master-services.yaml
kubectl apply -f 50-spark/03-worker-deployment.yaml
kubectl apply -f 50-spark/04-worker-service.yaml
kubectl apply -f 60-web/

kubectl -n bigdata get pods           # esperar a que todos estén Ready
```

Primera ejecución: tarda 5-10 min en descargar imágenes oficiales (Kafka,
Cassandra, Iceberg REST son las más pesadas). Las siguientes ejecuciones
del cluster (sin destruirlo) son instantáneas.

### 4. Subir datasets y modelos a MinIO en el cluster

Los Jobs K8s leen los datasets desde MinIO (no del host). Primera vez:

```bash
cd ~/practica_big_data

docker run --rm --network host --entrypoint sh \
  -v "$(pwd)/data:/data:ro" \
  -v "$(pwd)/models:/models:ro" \
  minio/mc:RELEASE.2025-04-08T15-39-49Z \
  -c "
    mc alias set local http://localhost:30091 minioadmin minioadmin123 >/dev/null
    mc cp /data/origin_dest_distances.jsonl       local/lakehouse/raw/
    mc cp /data/simple_flight_delay_features.jsonl.bz2 local/lakehouse/raw/
    mc cp --recursive /models/                    local/lakehouse/models/
  "
```

### 5. Lanzar Jobs de bootstrap

```bash
# Carga de 4696 distancias a Cassandra (usa los datos subidos a MinIO)
kubectl -n bigdata delete job cassandra-load-distances 2>/dev/null
kubectl apply -f 30-cassandra/04-load-distances-job.yaml
kubectl -n bigdata wait --for=condition=complete job/cassandra-load-distances --timeout=180s
```

### 6. Ingesta Iceberg (con cuidado de cores)

Importante: el cluster tiene un solo worker con 2 cores. Si el streaming
está corriendo ocupa los 2 cores y la ingesta se queda esperando. Conviene
parar el streaming, ingestar y relanzar.

```bash
# Si el streaming está activo, escalarlo a 0
kubectl -n bigdata scale deployment/spark-streaming --replicas=0 2>/dev/null

# Ingestar
kubectl apply -f 50-spark/05-ingest-job.yaml
kubectl -n bigdata wait --for=condition=complete job/iceberg-ingest --timeout=300s

# Relanzar streaming
kubectl apply -f 50-spark/06-streaming-deployment.yaml
kubectl -n bigdata scale deployment/spark-streaming --replicas=1
```

### 7. Acceder a las interfaces (NodePort)

| Servicio | URL |
|---|---|
| Web | http://localhost:30050 |
| MinIO Console | http://localhost:30090 |
| MinIO S3 API | http://localhost:30091 |
| Spark Master UI | http://localhost:30180 |
| Spark Worker UI | http://localhost:30181 |
| Iceberg REST | http://localhost:30183 |

### 8. Smoke test end-to-end

Abrir http://localhost:30050, rellenar el formulario con datos de un vuelo
(p.ej. ATL->SFO, AA, 2026-12-25, DepDelay 0) y pulsar "Predecir". La
predicción aparece en pocos segundos.

### 9. Verificación de los puntos obligatorios

```bash
# Punto 1: datos en Iceberg
kubectl -n bigdata logs job/iceberg-ingest -c ingest | grep "Registros"
# Debe mostrar: ">>> Registros en la tabla Iceberg: 457013"

# Punto 2: distancias en Cassandra
kubectl -n bigdata run cassandra-verify --rm -i --restart=Never \
  --image=cassandra:5.0 -- \
  cqlsh cassandra -e "SELECT COUNT(*) FROM flight_db.origin_dest_distances;"
# Debe mostrar: count = 4696

# Punto 3 y 4: predicciones llegan a Cassandra
kubectl -n bigdata run cassandra-verify --rm -i --restart=Never \
  --image=cassandra:5.0 -- \
  cqlsh cassandra -e "SELECT COUNT(*) FROM flight_db.flight_delay_predictions;"
# Debe ir aumentando según haces predicciones desde el web
```

### 10. Borrar el cluster

```bash
kind delete cluster --name bigdata
```

---

## Decisiones técnicas relevantes

### Sobre Kubernetes

1. **`enableServiceLinks: false`** en todos los pods de Spark y Web.
   Razón: K8s inyecta env vars automáticas para cada Service del namespace
   (`SPARK_MASTER_PORT="tcp://10.96.x.y:7077"`) que colisionan con las
   variables nativas de Spark (que esperan un entero como puerto). Sin esta
   flag, el master crashea con `NumberFormatException`.

2. **`spark.driver.host=$POD_IP`** + `bindAddress=0.0.0.0` + puertos fijos
   (40000/40001) en todos los jobs `spark-submit`.
   Razón: el executor del worker debe poder conectar al driver del pod.
   Sin esto, la app se queda en "Initial job has not accepted any resources"
   indefinidamente.

3. **Datos, scripts y modelos dentro de la imagen Spark** (no bind mount).
   En docker-compose se usaban bind mounts. En K8s no existen, y los pods
   driver y executor son independientes. Por eso `Dockerfile` incluye
   `COPY jobs/`, `COPY tests/`, `COPY simple_flight_delay_features.jsonl.bz2`
   y `COPY models_to_embed/`. Cada cambio en estos requiere rebuild +
   `kind load docker-image`.

4. **Sondas TCP en lugar de exec con JVM cliente**. Probar con
   `kafka-topics.sh --list` arranca una JVM de 5-8s en cold start y la
   sonda con timeout 5s falla. Solución: `tcpSocket: { port: 9092 }`.

5. **StatefulSet + headless Service** para servicios con identidad
   persistente (MinIO, Kafka, Cassandra). PVC dinámicos vía
   local-path-provisioner incluido en kind.

6. **Streaming como Deployment de larga duración**, no Job. Razón:
   estos pods deben reiniciarse si caen. K8s lo gestiona automáticamente.

7. **`enableServiceLinks: false`** también en Web. Aunque Flask no tiene
   colisión directa, lo aplicamos como buena práctica para evitar sorpresas
   futuras.

### Sobre la arquitectura general

1. **Iceberg REST en vez de Hadoop catalog** (más simple, sin dependencias
   hadoop-aws).
2. **S3FileIO directo** desde Spark/Iceberg (no necesita JARs adicionales).
3. **PostgreSQL único** para Airflow y MLflow (dos schemas lógicos).
4. **MLflow con artifact store en MinIO** (no en disco local del tracking
   server).

---

## Limitaciones conocidas

- En el cluster local con 1 worker de 2 cores, ingesta y streaming no pueden
  correr a la vez. Operativa: parar uno, ejecutar el otro, relanzar.
- La imagen Iceberg REST en kind no persiste su catálogo entre recreaciones
  del cluster (usa SQLite en `/tmp`). Las tablas se reconstruyen relanzando
  los Jobs de ingesta. En GKE se le pondría un PVC.
- El cliente Kafka de Python en Web puede crashear si arranca antes de que
  Kafka esté Ready. Solución: `kubectl rollout restart deployment/web`
  cuando todos los servicios estén listos.

---

## Licencia y autoría

Basado en https://github.com/Big-Data-ETSIT/practica_creativa
(plantilla docente ETSIT-UPM, derivada a su vez de
https://github.com/rjurney/Agile_Data_Code_2).

Modificaciones, infraestructura, manifiestos K8s y documentación:
djfug, 2025-2026.
