# Práctica Big Data 2026 – Parte II

Predicción de retrasos de vuelos en tiempo real con Spark, Kafka, Cassandra,
Iceberg Lakehouse, MLflow, Airflow, MinIO y Flask. Despliegue local (Docker
Compose y Kubernetes con kind) con observabilidad integrada (Prometheus +
Grafana).

---

## Tabla de contenidos

1. [Estado de cumplimiento](#estado-de-cumplimiento)
2. [Arquitectura](#arquitectura)
3. [Componentes y versiones](#componentes-y-versiones)
4. [Estructura del repositorio](#estructura-del-repositorio)
5. [Prerrequisitos del sistema](#prerrequisitos-del-sistema)
6. [Instalación de herramientas](#instalación-de-herramientas)
7. [Permisos y preparación previa](#permisos-y-preparación-previa)
8. [Despliegue A: Docker Compose](#despliegue-a-docker-compose-puntos-obligatorios--airflowmlflow)
9. [Despliegue B: Kubernetes local con kind](#despliegue-b-kubernetes-local-con-kind)
10. [Despliegue C: Observabilidad](#despliegue-c-observabilidad-prometheus--grafana-en-k8s)
11. [Decisiones técnicas relevantes](#decisiones-técnicas-relevantes)
12. [Troubleshooting completo](#troubleshooting-completo)
13. [Limitaciones conocidas](#limitaciones-conocidas)
14. [Comandos de cierre](#comandos-de-cierre)

---
### Qué componentes corren en cada despliegue

| Componente | Docker Compose | Kubernetes (kind) |
|------------|---------------|------------------|
| MinIO (S3 compatible) | Sí | Sí |
| Kafka (KRaft) | Sí | Sí |
| Cassandra | Sí | Sí |
| Iceberg REST catalog | Sí | Sí |
| Spark master + worker | Sí | Sí |
| Spark Streaming job (Scala) | Sí | Sí |
| Web Flask (predicción) | Sí | Sí |
| PostgreSQL | Sí | No (solo con Airflow/MLflow) |
| MLflow Tracking | Sí | No |
| Airflow (DAGs retrain + cleanup) | Sí | No |
| Prometheus | No | Sí |
| Grafana + dashboards | No | Sí |

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
       |     Airflow DAGs     | -> |  MLflow  |     (solo Docker Compose)
       | (retrain + cleanup)  |    | Tracking |
       +----------------------+    +----------+

   Observabilidad (solo Kubernetes):
       +-------------+              +------------+
       | Prometheus  | <- scrape -> |  Grafana   |
       | (5d ret.)   |              | (dashboard)|
       +-------------+              +------------+
              ^
              | ServiceMonitor + endpoint /metrics
              |
       Spark master, Spark worker, kube-state-metrics, node-exporter
```

---

## Componentes y versiones

| Componente | Versión | Notas |
|---|---|---|
| Apache Spark | 4.1.1 | Scala 2.13, Java 17 |
| Apache Kafka | 4.2.0 | KRaft (sin Zookeeper) |
| Apache Iceberg | 1.10.1 | REST catalog |
| Apache Cassandra | 5.0 | -- |
| Apache Airflow | 2.10.5 | LocalExecutor |
| MLflow | 2.18.0 | Backend: PostgreSQL, Artifacts: MinIO |
| MinIO | RELEASE.2025-04-08 | S3 compatible |
| PostgreSQL | 16-alpine | Backend Airflow + MLflow |
| Flask | 3.0 | + Flask-SocketIO |
| Kubernetes (kind) | 1.31.0 | Cluster local |
| Helm | 3.21 | Para kube-prometheus-stack |
| kube-prometheus-stack | 86.1.1 | Chart oficial Prometheus community |

---

## Estructura del repositorio

```
practica_big_data/
├── data/                                 # Datasets (incluidos en repo)
│   ├── origin_dest_distances.jsonl              (218 KB)
│   └── simple_flight_delay_features.jsonl.bz2   (4.5 MB)
├── models/                               # Modelos pre-entrenados (incluidos)
├── src/                                  # Códigos fuente
│   ├── flight_prediction/                # Job Scala (Spark Streaming)
│   ├── scripts/                          # Scripts auxiliares
│   │   └── load_distances_cassandra.py   # Carga distancias en Cassandra
│   └── web/                              # Servidor Flask
├── deployment/
│   ├── docker/                           # Stack Docker Compose
│   │   ├── compose.yaml
│   │   ├── .env.example
│   │   ├── bootstrap/                    # Schema CQL Cassandra
│   │   ├── spark/                        # Dockerfile + conf + jobs
│   │   ├── web/                          # Dockerfile Flask
│   │   ├── airflow/                      # Dockerfile (Python + Docker CLI)
│   │   └── mlflow/                       # Dockerfile MLflow
│   ├── airflow/dags/                     # DAGs (retrain, cleanup)
│   └── k8s/                              # Manifiestos Kubernetes
│       ├── kind-config.yaml
│       ├── 00-base/
│       ├── 10-minio/
│       ├── 20-kafka/
│       ├── 30-cassandra/
│       ├── 40-iceberg-rest/
│       ├── 50-spark/
│       ├── 60-web/
│       └── 70-monitoring/
└── README.md
```

---

## Prerrequisitos del sistema

### Hardware mínimo recomendado

- 16 GB RAM (10 GB asignados a WSL2, si se trabaja con Windows)
- 4 cores
- 30 GB libres en disco

### Software base

- Sistema operativo: Linux (probado en WSL2 Ubuntu 24.04 LTS sobre Windows 11)
- Docker Engine 28+ con Docker Compose plugin 2.30+
- Git
- `curl`, `unzip`, `python3` (vienen por defecto en Ubuntu)

---

## Instalación de herramientas

### Paso 1: Docker + Docker Compose

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin

sudo usermod -aG docker $USER
```

Tras `sudo usermod -aG docker`, hay que **reiniciar la sesión** para que el
grupo `docker` se active. En WSL2:

```powershell
# Desde PowerShell
wsl --shutdown
```

Luego reabre la distribución WSL2.

### Paso 2: kubectl

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl
```

### Paso 3: kind

```bash
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind
```

### Paso 4: Helm

```bash
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod +x get_helm.sh
./get_helm.sh
rm get_helm.sh
```

### Paso 5: Clonar el repositorio

El repositorio incluye todos los datasets, modelos y el JAR de streaming. No
hace falta descargar nada extra.

```bash
cd ~
git clone https://github.com/djfuga/practica-bigdata-2026.git practica_big_data
cd practica_big_data
```

### Paso 6: Comprobación final

```bash
docker version --format 'Server: {{.Server.Version}}'
docker compose version --short
kubectl version --client -o yaml | grep gitVersion | head -1
kind version
helm version --short

ls -lh data/*.jsonl*
ls -lh deployment/docker/spark/jobs/*.jar
free -h | head -2
```

---

## Permisos y preparación previa

**Este paso es obligatorio antes del primer `docker compose up`.** Sin él,
Airflow no arrancará correctamente porque no podrá escribir en sus carpetas
de logs.

### Permisos para Airflow

Airflow corre como usuario `airflow` (UID 50000) y necesita escribir en las
carpetas de logs y procesador de DAGs. Tras clonar el repo, estas carpetas
no existen (están en `.gitignore`) y hay que crearlas con permisos amplios:

```bash
cd ~/practica_big_data/deployment

# Crear las carpetas que Airflow espera
mkdir -p airflow/logs/dag_processor_manager
mkdir -p airflow/logs/scheduler
mkdir -p airflow/plugins
mkdir -p airflow/config

# Permisos amplios para que UID 50000 (airflow) y GID 0 (root) puedan escribir
sudo chmod -R 777 airflow/logs/
sudo chmod -R 755 airflow/dags/
sudo chmod -R 755 airflow/plugins/
sudo chmod -R 755 airflow/config/

# Verificar
ls -la airflow/
```

Si te saltas este paso, verás errores tipo `PermissionError: '/opt/airflow/logs/scheduler'`
en los logs de Airflow y el webserver no responderá (HTTP 000).

### Permisos en la red Docker

Docker creará una red `bigdata-net` al levantar el stack. Si te aparece
`Pool overlaps with other one on this address space`, limpia redes huérfanas:

```bash
docker network prune -f
```

### Permisos para los datasets (lectura)

Los datasets se montan en read-only (`:ro`) en los contenedores. Los
permisos por defecto del clone (644) son suficientes. Pero verifica:

```bash
cd ~/practica_big_data
ls -la data/
# Debe mostrar -rw-r--r-- para los dos ficheros
```

Si por algún motivo los permisos son más restrictivos (p.ej. 600), arregla:

```bash
chmod 644 data/*
```

### Permisos especiales para Kubernetes

Cuando subimos los datasets a MinIO en Kubernetes (Paso 6 del Despliegue B),
el contenedor `minio/mc` lee de un bind mount `data/` del host. Si los
permisos del host son restrictivos, falla. Asegúralo con:

```bash
chmod 755 data/
chmod 644 data/*
chmod 755 models/
find models/ -type d -exec chmod 755 {} \;
find models/ -type f -exec chmod 644 {} \;
```

---

## Despliegue A: Docker Compose (puntos obligatorios + Airflow/MLflow)

### Paso 1: Variables de entorno

```bash
cd ~/practica_big_data/deployment/docker
cp .env.example .env
```

Credenciales por defecto:
- MinIO: `minioadmin` / `minioadmin123`
- Postgres: `bigdata` / `bigdata-secret-2026`
- Airflow: `admin` / `admin`

### Paso 2: Construcción de imágenes propias

```bash
cd ~/practica_big_data/deployment/docker

# Tarda 10-15 min la primera vez con buena conexión
docker compose build spark-master web mlflow airflow-webserver
```

Imágenes resultantes (~6 GB total):
- `bigdata/spark:4.1.1` (~2.2 GB)
- `bigdata/web:1.0` (~170 MB)
- `bigdata/mlflow:2.18.0` (~940 MB)
- `bigdata/airflow:2.10.5` (~2 GB)

### Paso 3: Levantar el stack completo

```bash
docker compose up -d

# Esperar a que los healthchecks pasen (3-5 min)
sleep 90
docker compose ps
```

> **IMPORTANTE: SIEMPRE usa `docker compose up -d`, NO `docker compose start`.**
> 
> El comando `start` reutiliza contenedores existentes pero **NO respeta
> los `depends_on: condition: service_healthy`**. Esto causa que Web, MLflow
> y Airflow arranquen ANTES que Kafka/Postgres estén Ready y queden en
> `Restarting` o `unhealthy`.
> 
> Si tienes que parar el stack: `docker compose stop`.
> Para retomarlo: `docker compose down` (sin `-v`, mantiene volúmenes) + `docker compose up -d`.

Todos los servicios deben estar `(healthy)` o `Up`. `bigdata-airflow-init`
saldrá como `Exited (0)` — es correcto, es un Job de un solo uso.

### Paso 4: Ingesta inicial del lakehouse Iceberg (punto 1)

```bash
cd ~/practica_big_data/deployment/docker

docker compose exec spark-master spark-submit \
  /opt/spark/jobs/ingest_training_data.py
```

Resultado esperado:

```
>>> Registros leidos: 457013
>>> Registros en la tabla Iceberg: 457013
INGESTA ICEBERG: OK
```

### Paso 4.5: Carga de distancias en Cassandra (punto 2 OBLIGATORIO)

El contenedor `cassandra-init` solo crea las tablas vacías. La carga de las
4696 distancias origen-destino se hace con un script Python aparte
(`src/scripts/load_distances_cassandra.py`) que necesita el driver
`cassandra-driver` instalado.

Levantamos un contenedor Python efímero, instalamos el driver y ejecutamos
el script en una sola operación:

```bash
cd ~/practica_big_data

NETWORK=$(docker network ls --format '{{.Name}}' | grep -i bigdata | head -1)

docker run --rm --network "$NETWORK" \
  -v ~/practica_big_data/data:/data:ro \
  -v ~/practica_big_data/src/scripts:/scripts:ro \
  python:3.11-slim \
  sh -c "pip install --quiet cassandra-driver && python /scripts/load_distances_cassandra.py /data/origin_dest_distances.jsonl"
```

Tarda unos 2 minutos. Salida esperada al final:

```
>>> 4696 distancias insertadas
>>> Total filas en Cassandra: 4696
>>> Test lectura ATL->SFO: distance=2139.0
CARGA DISTANCIAS CASSANDRA: OK
```

Verificación adicional con cqlsh:

```bash
cd ~/practica_big_data/deployment/docker
docker compose exec cassandra cqlsh cassandra -e \
  "SELECT COUNT(*) FROM flight_db.origin_dest_distances;"
# Debe devolver: count = 4696
```

### Paso 5: Entrenamiento del modelo (opcional)

Los modelos ya vienen pre-entrenados en `models/`. Solo es necesario
re-entrenar si quieres regenerarlos:

```bash
cd ~/practica_big_data/deployment/docker
docker compose exec spark-master spark-submit \
  /opt/spark/jobs/train_spark_mllib_model.py
```

Tarda 4-6 minutos. Sobrescribe los modelos en `/opt/spark/models` (montado
a `./models` del host) y registra el experimento en MLflow.

### Paso 6: Lanzar el job de predicción streaming (puntos 3 y 4)

```bash
cd ~/practica_big_data/deployment/docker

# Lanzar en background (-d) para que siga corriendo
docker compose exec -d spark-master spark-submit \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/flight_prediction.jar

# Verificar que está corriendo
docker compose exec spark-master sh -c "ps aux | grep MakePrediction | grep -v grep"
```

### Paso 7: Acceder a las interfaces

| Servicio | URL | Credenciales |
|---|---|---|
| Web (formulario predicción) | http://localhost:5001 | -- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master UI | http://localhost:8080 | -- |
| Spark Worker UI | http://localhost:8081 | -- |
| Iceberg REST | http://localhost:8181 | -- |
| Airflow | http://localhost:8082 | admin / admin |
| MLflow | http://localhost:5002 | -- |

### Paso 8: Smoke test (predicción end-to-end)

1. Abrir http://localhost:5001
2. Rellenar formulario:
   - Date: 2026-12-25
   - Carrier: AA
   - Origin: ATL
   - Destination: SFO
   - Departure Delay: 0
3. Pulsar "Predict"
4. La predicción aparece en pocos segundos en la UI (vía WebSocket)
5. Verificar persistencia:

```bash
cd ~/practica_big_data/deployment/docker
docker compose exec cassandra cqlsh cassandra -e \
  "SELECT origin, dest, prediction, distance, dep_delay FROM flight_db.flight_delay_predictions LIMIT 5;"
```

La predicción debe tener `distance=2139` (millas reales ATL→SFO).

### Paso 9: Operar Airflow (punto 7)

```bash
cd ~/practica_big_data/deployment/docker

# Despausar los 2 DAGs
docker compose exec airflow-scheduler airflow dags unpause retrain_flight_model
docker compose exec airflow-scheduler airflow dags unpause cleanup_old_predictions

# Disparar manualmente (opcional)
docker compose exec airflow-scheduler airflow dags trigger cleanup_old_predictions
```

> **Si Airflow no responde (HTTP 000) tras `compose up -d`:**
> 
> Verifica los logs:
> ```bash
> docker compose logs airflow-webserver --tail 30
> ```
> 
> Si ves "You need to initialize the database", ejecuta la migración manualmente:
> ```bash
> docker compose stop airflow-webserver airflow-scheduler
> docker compose run --rm --no-deps airflow-webserver airflow db migrate
> docker compose run --rm --no-deps airflow-webserver airflow users create \
>   --username admin --password admin \
>   --firstname Admin --lastname User \
>   --role Admin --email admin@example.com
> docker compose up -d airflow-webserver airflow-scheduler
> ```
> 
> Esto puede pasar si el contenedor `airflow-init` falló silenciosamente en
> el primer arranque (los permisos de logs no estaban bien). Ver
> [Troubleshooting #5](#troubleshooting-completo) para detalles.

### Paso 10: Parar el stack

```bash
cd ~/practica_big_data/deployment/docker

docker compose stop     # Mantiene los contenedores y volúmenes
# o
docker compose down     # Borra contenedores, mantiene volúmenes (datos persisten)
# o
docker compose down -v  # Borra TODO incluido los datos
```

> **Para reanudar después: SIEMPRE `down` + `up -d`, NUNCA `start`** (ver
> nota en Paso 3).

---

## Despliegue B: Kubernetes local con kind

**Importante**: Docker Compose y kind NO pueden correr a la vez en WSL2
con 10 GB RAM. Antes de arrancar kind, parar Docker Compose:

```bash
cd ~/practica_big_data/deployment/docker
docker compose stop
```

### Paso 1: Verificar permisos

Antes de arrancar K8s, asegúrate que los permisos de datos son correctos
(ver sección [Permisos](#permisos-y-preparación-previa)):

```bash
cd ~/practica_big_data
chmod 755 data/ models/
chmod 644 data/*
find models/ -type d -exec chmod 755 {} \;
find models/ -type f -exec chmod 644 {} \;
```

### Paso 2: Construir imágenes propias

Si no las tienes ya construidas del Despliegue A:

```bash
cd ~/practica_big_data/deployment/docker
docker compose build spark-master web
```

### Paso 3: Crear cluster kind

```bash
cd ~/practica_big_data/deployment/k8s
kind create cluster --config kind-config.yaml
kubectl cluster-info --context kind-bigdata
```

### Paso 4: Cargar imágenes propias en el cluster

```bash
kind load docker-image bigdata/spark:4.1.1 --name bigdata
kind load docker-image bigdata/web:1.0     --name bigdata
```

### Paso 5: Aplicar manifiestos base + MinIO

```bash
cd ~/practica_big_data/deployment/k8s

kubectl apply -f 00-base/
kubectl apply -f 10-minio/
kubectl -n bigdata wait --for=condition=ready pod/minio-0 --timeout=180s
kubectl -n bigdata wait --for=condition=complete job/minio-bootstrap --timeout=120s
```

### Paso 6: Subir datasets y modelos a MinIO

Este paso es donde los compañeros suelen tener más problemas. Atención
especial a los permisos de las carpetas `data/` y `models/` (ver sección
[Permisos especiales para Kubernetes](#permisos-especiales-para-kubernetes)).

```bash
cd ~/practica_big_data

# Verificar que las rutas y permisos son correctos antes de empezar
ls -la data/
ls -la models/

# Subir todo a MinIO usando mc en un contenedor ad-hoc
docker run --rm --network host \
  -v "$(pwd)/data:/data:ro" \
  -v "$(pwd)/models:/models:ro" \
  --entrypoint sh \
  minio/mc:RELEASE.2025-04-03T17-07-56Z \
  -c "
    mc alias set local http://localhost:30091 minioadmin minioadmin123 >/dev/null
    mc mb --ignore-existing local/lakehouse
    mc cp /data/origin_dest_distances.jsonl       local/lakehouse/raw/
    mc cp /data/simple_flight_delay_features.jsonl.bz2 local/lakehouse/raw/
    mc cp --recursive /models/                    local/lakehouse/models/
    echo '=== Subida completada ==='
    mc ls --recursive local/lakehouse/ | head -20
  "
```

**Si falla con "permission denied"**, los archivos del host no son
legibles por el contenedor `mc`. Ejecuta:

```bash
cd ~/practica_big_data
chmod -R a+rX data/ models/
```

Y vuelve a ejecutar el `docker run` anterior.

**Si falla con "Connection refused" en `localhost:30091`**, MinIO no está
listo. Espera y verifica:

```bash
kubectl -n bigdata wait --for=condition=ready pod/minio-0 --timeout=300s
kubectl -n bigdata get svc minio
curl -s http://localhost:30091/minio/health/live -o /dev/null -w "%{http_code}\n"
# Debe devolver 200
```

**Si falla con "mc: command not found"**, la imagen de mc cambió su
entrypoint. Usa `--entrypoint sh` como en el comando anterior (ya está).

### Paso 7: Aplicar resto del stack

```bash
cd ~/practica_big_data/deployment/k8s

kubectl apply -f 20-kafka/
kubectl apply -f 30-cassandra/
kubectl apply -f 40-iceberg-rest/
kubectl apply -f 50-spark/01-master-deployment.yaml
kubectl apply -f 50-spark/02-master-services.yaml
kubectl apply -f 50-spark/03-worker-deployment.yaml
kubectl apply -f 50-spark/04-worker-service.yaml
kubectl apply -f 60-web/

kubectl -n bigdata wait --for=condition=ready pod/kafka-0 --timeout=600s
kubectl -n bigdata wait --for=condition=ready pod/cassandra-0 --timeout=600s
kubectl -n bigdata wait --for=condition=available deployment/iceberg-rest --timeout=180s
kubectl -n bigdata wait --for=condition=available deployment/spark-master --timeout=180s
kubectl -n bigdata wait --for=condition=available deployment/spark-worker --timeout=180s
```

### Paso 8: Reiniciar Web Flask

```bash
kubectl -n bigdata rollout restart deployment/web
kubectl -n bigdata wait --for=condition=available deployment/web --timeout=120s

curl -s http://localhost:30050/health
```

### Paso 9: Relanzar `cassandra-load-distances` si falló

```bash
# Verificar
kubectl -n bigdata get jobs

# Solo si "Failed":
kubectl -n bigdata delete job cassandra-load-distances
kubectl apply -f 30-cassandra/04-load-distances-job.yaml
kubectl -n bigdata wait --for=condition=complete job/cassandra-load-distances --timeout=180s
```

Verificación:

```bash
kubectl -n bigdata run cassandra-verify --rm -i --restart=Never \
  --image=cassandra:5.0 -- \
  cqlsh cassandra -e "SELECT COUNT(*) FROM flight_db.origin_dest_distances;"
```

### Paso 10: Ingesta Iceberg

```bash
cd ~/practica_big_data/deployment/k8s

# Escalar streaming a 0 (libera cores)
kubectl -n bigdata scale deployment/spark-streaming --replicas=0 2>/dev/null

kubectl apply -f 50-spark/05-ingest-job.yaml
kubectl -n bigdata wait --for=condition=complete job/iceberg-ingest --timeout=300s
```

### Paso 11: Lanzar streaming

```bash
kubectl apply -f 50-spark/06-streaming-deployment.yaml
kubectl -n bigdata wait --for=condition=available deployment/spark-streaming --timeout=120s
```

### Paso 12: Acceder a las interfaces

| Servicio | URL |
|---|---|
| Web | http://localhost:30050 |
| MinIO Console | http://localhost:30090 |
| MinIO S3 API | http://localhost:30091 |
| Spark Master UI | http://localhost:30180 |
| Spark Worker UI | http://localhost:30181 |
| Iceberg REST | http://localhost:30183 |

### Paso 13: Borrar el cluster

```bash
kind delete cluster --name bigdata
```

---

## Despliegue C: Observabilidad (Prometheus + Grafana en K8s)

Stack `kube-prometheus-stack` v86.1.1.

### Paso 1: Instalar el chart

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

cd ~/practica_big_data/deployment/k8s/70-monitoring

helm install kube-prometheus-stack \
  prometheus-community/kube-prometheus-stack \
  --version 86.1.1 \
  --namespace monitoring \
  --create-namespace \
  --values values.yaml \
  --wait \
  --timeout 10m
```

### Paso 2: Aplicar ServiceMonitor Spark y NodePort Grafana

```bash
kubectl apply -f 01-spark-servicemonitor.yaml
kubectl apply -f 02-grafana-nodeport.yaml
```

### Paso 3: Importar dashboard Spark

```bash
sleep 30

DASHBOARD_JSON=$(cat spark-dashboard.json)

curl -s -X POST http://admin:bigdata2026@localhost:30002/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d "{
    \"dashboard\": $DASHBOARD_JSON,
    \"overwrite\": true,
    \"folderUid\": \"\"
  }"
```

### Paso 4: Acceder a Grafana

URL: http://localhost:30002 — admin / bigdata2026

Navegar a Dashboards y abrir **Spark - Practica BigData 2026**.

---

## Decisiones técnicas relevantes

### Sobre la inclusión de datasets y modelos en el repo

Los datasets (`data/`), modelos pre-entrenados (`models/`) y JAR de
streaming (`deployment/docker/spark/jobs/flight_prediction.jar`) están
incluidos en el repositorio para garantizar reproducibilidad.

### Sobre Kubernetes

1. **`enableServiceLinks: false`** en todos los pods de Spark y Web. K8s
   inyecta env vars automáticas que colisionan con variables Spark nativas.

2. **`spark.driver.host=$POD_IP`** + `bindAddress=0.0.0.0` + puertos fijos
   (40000/40001) en todos los `spark-submit`.

3. **Datos, scripts y modelos dentro de la imagen Spark** (no bind mount).
   K8s no soporta bind mounts.

4. **Sondas TCP en lugar de exec con JVM cliente** para Kafka readiness.

5. **StatefulSet + headless Service** para servicios con identidad
   persistente.

6. **Streaming como Deployment de larga duración**, no Job.

7. **Labels explícitas en los Services** para que los ServiceMonitor de
   Prometheus las encuentren.

### Sobre el Dockerfile de Spark

`ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"` se incluye explícitamente
en el Dockerfile para que `docker compose exec spark-master spark-submit ...`
funcione directamente. Sin este `ENV`, `docker exec` no carga el `.bashrc`
y `spark-submit` no se encuentra.

### Sobre el `user` de Airflow

Los servicios `airflow-webserver` y `airflow-scheduler` usan `user: "50000:0"`
(UID airflow + GID root). El GID 0 es **necesario** para que Airflow pueda
escribir en `/opt/airflow/logs/`. El acceso al socket Docker (necesario para
que los DAGs invoquen `docker exec`) se garantiza vía `group_add: ["108"]`
(GID secundario), no vía GID primario.

### Sobre Observabilidad

1. **Sink PrometheusServlet nativo de Spark** en lugar de exporters externos.
2. **Dashboard custom** con queries que coinciden con las métricas reales.
3. **Grafana via NodePort** (30002) en vez de port-forward.
4. **Recursos Grafana ajustados** a 768Mi tras detectar OOM con 256Mi.

---

## Troubleshooting completo

### Problemas con Docker Compose

#### 1. `Pool overlaps with other one on this address space`

```bash
docker compose down
docker network prune -f
docker compose up -d
```

#### 2. Servicios "unhealthy" tras varios minutos

```bash
docker compose ps
docker compose logs <servicio> --tail 50
docker compose restart <servicio>
```

#### 3. Airflow webserver `HTTP 000` o `ERROR: You need to initialize the database`

**Causa más frecuente**: el contenedor `airflow-init` falló silenciosamente
(porque las carpetas de logs no tenían permisos) y la BD nunca se inicializó.

**Diagnóstico**:
```bash
docker compose logs airflow-init --tail 30
# Si ves "PermissionError" o "ValueError: Unable to configure handler 'processor'":
```

**Fix completo**:
```bash
# 1. Permisos correctos en logs
cd ~/practica_big_data/deployment
mkdir -p airflow/logs/dag_processor_manager airflow/logs/scheduler
sudo chmod -R 777 airflow/logs/

# 2. Reiniciar la BD de Airflow desde cero
cd ~/practica_big_data/deployment/docker
docker compose stop airflow-webserver airflow-scheduler

docker compose run --rm --no-deps airflow-webserver airflow db migrate
docker compose run --rm --no-deps airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

docker compose up -d airflow-webserver airflow-scheduler
```

#### 4. `spark-submit: executable file not found in $PATH`

**Causa**: `docker compose exec` no carga el `.bashrc` del contenedor.

**Solución**: ya aplicada en este repo (Dockerfile incluye `ENV PATH`). Si
te pasa, reconstruye:

```bash
cd ~/practica_big_data/deployment/docker
docker compose build spark-master
docker compose up -d --force-recreate spark-master spark-worker
```

#### 5. Cassandra solo tiene 1 distancia (debería tener 4696)

**Causa**: el contenedor `cassandra-init` solo crea el schema, no carga datos.

**Solución**: ejecutar el script de carga manualmente (Paso 4.5 del Despliegue A).

#### 6. MLflow: `Connection timed out` a postgres o `unhealthy`

**Causa**: MLflow arrancó antes que Postgres estuviera Ready (típicamente
tras usar `docker compose start` en lugar de `up -d`).

**Solución**:
```bash
docker compose down       # mantiene los volúmenes
docker compose up -d      # respeta depends_on healthchecks
```

#### 7. Web en `Restarting` con `NoBrokersAvailable`

**Causa**: el cliente Kafka del web no tiene retry largo y crashea si Kafka
no respondió aún. Pasa típicamente tras `docker compose start`.

**Solución**: igual que #6, usar `down` + `up -d` en lugar de `start`.

### Problemas con Kubernetes (kind)

#### 8. `kind create cluster` falla con `unable to mount cgroup`

En `%USERPROFILE%\.wslconfig`:
```ini
[wsl2]
kernelCommandLine = cgroup_no_v1=all
```
Luego `wsl --shutdown`.

#### 9. Paso 6 falla con "permission denied" al subir datasets

**Causa**: los archivos `data/` o `models/` del host no son legibles por
el contenedor `mc` (UID distinto).

**Solución**:
```bash
cd ~/practica_big_data
chmod -R a+rX data/ models/
ls -la data/ models/
# Verificar que el modo es 755 (carpetas) y 644 (archivos)
```

Y reintentar el `docker run` con `minio/mc`.

#### 10. Paso 6 falla con "Connection refused" en `localhost:30091`

MinIO aún no expone el NodePort. Espera:

```bash
kubectl -n bigdata wait --for=condition=ready pod/minio-0 --timeout=300s
kubectl -n bigdata get svc minio
curl -s http://localhost:30091/minio/health/live -o /dev/null -w "%{http_code}\n"
```

#### 11. Pods en `ContainerCreating` mucho tiempo

Causa: descarga lenta de imágenes oficiales. Esperar.

#### 12. Spark master: `NumberFormatException: For input string: "tcp://..."`

Solución: el manifest debe tener `enableServiceLinks: false`.

#### 13. Spark job: `Initial job has not accepted any resources`

Solución: usar `spark.driver.host=$POD_IP`.

#### 14. Pod streaming: `Failed to load class ...`

El nombre correcto es `es.upm.dit.ging.predictor.MakePrediction`.

#### 15. ServiceMonitor no descubre targets

Causa: el Service no tiene `metadata.labels`. Añadirlas.

#### 16. Kafka readiness probe timeout

Cambiar a `tcpSocket: { port: 9092 }`.

#### 17. Web pod en `CrashLoopBackOff` con `NoBrokersAvailable`

```bash
kubectl -n bigdata rollout restart deployment/web
```

#### 18. `cassandra-load-distances` Job en estado `Failed`

```bash
kubectl -n bigdata delete job cassandra-load-distances
kubectl apply -f deployment/k8s/30-cassandra/04-load-distances-job.yaml
```

#### 19. Iceberg ingest atascado

```bash
kubectl -n bigdata scale deployment/spark-streaming --replicas=0
kubectl -n bigdata delete job iceberg-ingest
kubectl apply -f deployment/k8s/50-spark/05-ingest-job.yaml
```

### Problemas con Observabilidad

#### 20. Grafana en `CrashLoopBackOff` (OOM)

Aumentar memoria en `values.yaml`:
```yaml
grafana:
  resources:
    limits:
      memory: 768Mi
```

Y aplicar:
```bash
helm upgrade kube-prometheus-stack ... --values values.yaml --reuse-values
```

#### 21. `kubectl port-forward` se cae

Usar NodePort. Grafana ya está en 30002.

#### 22. Dashboard Grafana con paneles "No data"

Verificar el UID del datasource en /api/datasources.

### Problemas generales

#### 23. WSL2 se queda sin RAM

En `%USERPROFILE%\.wslconfig`:
```ini
[wsl2]
memory=12GB
processors=8
```

#### 24. `helm install` falla con timeout

```bash
helm install ... --timeout 15m
```

#### 25. Puertos del host ocupados

```bash
sudo lsof -i :5001
```

#### 26. Modelos perdidos tras `docker compose down -v`

Los modelos pre-entrenados ya están en el repo. Si los regeneras:

```bash
docker compose exec spark-master spark-submit \
  /opt/spark/jobs/train_spark_mllib_model.py
```

#### 27. Pager `less` se queda en `:` con git diff

Pulsar `q` para salir. Para desactivar:
```bash
git config --global pager.diff false
```

#### 28. Permission denied al ejecutar `docker` tras instalación

Causa: el grupo `docker` no está activo en la sesión.

Solución: cerrar sesión WSL2 (`wsl --shutdown` desde PowerShell) y reabrir.

#### 29. Tras `docker compose stop` largo, los servicios no arrancan bien

**Causa**: `docker compose start` no respeta los healthchecks de `depends_on`.

**Solución**: usar SIEMPRE `down` + `up -d` para reanudar:

```bash
docker compose down       # NO uses -v (mantiene los datos)
docker compose up -d      # respeta el orden correcto
```

---

## Limitaciones conocidas

- En el cluster local con 1 worker de 2 cores, ingesta y streaming no
  pueden correr a la vez.
- La imagen Iceberg REST en kind no persiste su catálogo entre
  recreaciones del cluster.
- El cliente Kafka de Python en Web puede crashear si arranca antes que
  Kafka (mitigado con `depends_on: condition: service_healthy`).
- Docker Compose y kind no pueden correr a la vez en WSL2 con 10 GB.
- El contenedor `airflow-init` no usa `set -e`, así que puede salir con
  código 0 incluso si hubo errores intermedios. Si Airflow no arranca tras
  `docker compose up -d`, ejecutar `db migrate` manualmente (ver
  Troubleshooting #3).

---

## Comandos de cierre

### Cierre temporal

```bash
# Docker Compose
cd ~/practica_big_data/deployment/docker
docker compose down       # mantiene volúmenes y datos

# Kubernetes
kind delete cluster --name bigdata
```

### Cierre completo

```bash
cd ~/practica_big_data/deployment/docker
docker compose down -v

docker rmi bigdata/spark:4.1.1 bigdata/web:1.0 \
  bigdata/mlflow:2.18.0 bigdata/airflow:2.10.5

kind delete cluster --name bigdata
docker network prune -f
```

---
