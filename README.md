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
7. [Despliegue A: Docker Compose](#despliegue-a-docker-compose-puntos-obligatorios--airflowmlflow)
8. [Despliegue B: Kubernetes local con kind](#despliegue-b-kubernetes-local-con-kind)
9. [Despliegue C: Observabilidad](#despliegue-c-observabilidad-prometheus--grafana-en-k8s)
10. [Decisiones técnicas relevantes](#decisiones-técnicas-relevantes)
11. [Troubleshooting completo](#troubleshooting-completo)
12. [Limitaciones conocidas](#limitaciones-conocidas)
13. [Comandos de cierre](#comandos-de-cierre)
14. [Licencia y autoría](#licencia-y-autoría)

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

**Decisión arquitectónica**: Airflow + MLflow se quedan en Docker Compose
porque la rúbrica del punto 7 dice "entrenar el modelo con Apache Airflow y
MLflow en el cluster spark con docker", lo cual deja claro que el escenario
esperado es Docker. La observabilidad se ha implementado en K8s porque ahí
es donde el stack del cluster (kube-state-metrics, node-exporter, etc.)
aporta más valor.

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
│   ├── arrival_bucketizer_2.0.bin/
│   ├── numeric_vector_assembler.bin/
│   ├── spark_random_forest_classifier.flight_delays.5.0.bin/
│   ├── string_indexer_model_Carrier.bin/
│   ├── string_indexer_model_Dest.bin/
│   ├── string_indexer_model_Origin.bin/
│   └── string_indexer_model_Route.bin/
├── src/                                  # Códigos fuente
│   ├── flight_prediction/                # Job Scala (Spark Streaming)
│   └── web/                              # Servidor Flask
├── deployment/
│   ├── docker/                           # Stack Docker Compose
│   │   ├── compose.yaml
│   │   ├── .env.example
│   │   ├── bootstrap/                    # Scripts init (postgres, kafka)
│   │   ├── spark/                        # Dockerfile + conf + jobs PySpark
│   │   │   ├── jobs/
│   │   │   │   └── flight_prediction.jar # JAR Scala compilado (5.8 MB)
│   │   │   ├── simple_flight_delay_features.jsonl.bz2  # Empotrado para K8s
│   │   │   └── models_to_embed/          # Modelos empotrados para K8s
│   │   ├── web/                          # Dockerfile Flask
│   │   ├── airflow/                      # Dockerfile (Python + Docker CLI)
│   │   └── mlflow/                       # Dockerfile MLflow
│   ├── airflow/dags/                     # DAGs (retrain, cleanup)
│   └── k8s/                              # Manifiestos Kubernetes
│       ├── kind-config.yaml
│       ├── 00-base/                      # Namespace + Secrets + ConfigMap
│       ├── 10-minio/                     # StatefulSet + Services + bootstrap
│       ├── 20-kafka/                     # StatefulSet KRaft + Services
│       ├── 30-cassandra/                 # StatefulSet + schema + load-distances
│       ├── 40-iceberg-rest/              # Deployment + Services
│       ├── 50-spark/                     # Master, worker, ingest, streaming
│       ├── 60-web/                       # Deployment Flask + Services
│       └── 70-monitoring/                # Prometheus + Grafana
│           ├── values.yaml
│           ├── 01-spark-servicemonitor.yaml
│           ├── 02-grafana-nodeport.yaml
│           └── spark-dashboard.json
└── README.md
```

**Nota sobre duplicación intencionada de datasets/modelos**:

- `data/` y `models/` (raíz): se usan en Docker Compose vía bind mount
- `deployment/docker/spark/simple_flight_delay_features.jsonl.bz2` y
  `deployment/docker/spark/models_to_embed/`: copias **incluidas en el build
  context** de la imagen Spark. Necesarias porque K8s no soporta bind
  mounts: la imagen las copia DENTRO con `COPY` en el Dockerfile y así los
  pods Spark en K8s tienen acceso a los ficheros sin depender del host.

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

### Paso 1: Docker + Docker Compose (si no está instalado)

```bash
# En WSL2 Ubuntu 24.04
sudo apt update
sudo apt install -y ca-certificates curl gnupg

# Repositorio oficial Docker
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

# Añadir tu usuario al grupo docker (evita 'sudo' constante)
sudo usermod -aG docker $USER
# Cerrar y reabrir sesión WSL2 para que coja el grupo

# Verificar
docker version
docker compose version
```

### Paso 2: kubectl

```bash
# Última versión estable
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"

sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl

# Verificar
kubectl version --client
```

### Paso 3: kind (Kubernetes IN Docker)

```bash
# Versión 0.24.0 (estable, con K8s 1.31)
curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.24.0/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

# Verificar
kind version
```

### Paso 4: Helm

```bash
# Última estable
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod +x get_helm.sh
./get_helm.sh
rm get_helm.sh

# Verificar
helm version --short
```

### Paso 5: Clonar el repositorio

```bash
# El repositorio YA INCLUYE los datasets, modelos y JAR de streaming.
# No es necesario descargar nada adicional.
cd ~
git clone https://github.com/djfuga/practica-bigdata-2026.git practica_big_data
cd practica_big_data

# Verificar contenido (debe mostrar ~10 MB de ficheros)
echo "=== Datasets ==="
ls -lh data/*.jsonl*

echo ""
echo "=== Modelos pre-entrenados ==="
ls -1 models/

echo ""
echo "=== JAR de streaming Scala ==="
ls -lh deployment/docker/spark/jobs/*.jar
```

Output esperado:

```
=== Datasets ===
-rw-r--r-- 218K  data/origin_dest_distances.jsonl
-rw-r--r-- 4.5M  data/simple_flight_delay_features.jsonl.bz2

=== Modelos pre-entrenados ===
arrival_bucketizer_2.0.bin
numeric_vector_assembler.bin
spark_random_forest_classifier.flight_delays.5.0.bin
string_indexer_model_Carrier.bin
string_indexer_model_Dest.bin
string_indexer_model_Origin.bin
string_indexer_model_Route.bin

=== JAR de streaming Scala ===
-rw-r--r-- 5.8M  deployment/docker/spark/jobs/flight_prediction.jar
```

### Paso 6: Comprobación completa de herramientas

Ejecuta este check antes de continuar:

```bash
echo "=== Versiones instaladas ==="
echo "Docker:         $(docker version --format 'Server: {{.Server.Version}}')"
echo "Compose:        $(docker compose version --short)"
echo "kubectl:        $(kubectl version --client -o yaml | grep gitVersion | head -1 | awk '{print $2}')"
echo "kind:           $(kind version | awk '{print $2}')"
echo "helm:           $(helm version --short)"

echo ""
echo "=== Datasets disponibles ==="
ls -lh data/*.jsonl* 2>/dev/null

echo ""
echo "=== Modelos disponibles ==="
ls -1 models/ 2>/dev/null

echo ""
echo "=== JAR disponible ==="
ls -lh deployment/docker/spark/jobs/*.jar 2>/dev/null

echo ""
echo "=== RAM disponible ==="
free -h | head -2
echo "Mínimo recomendado: 10 GB libres"
```

---

## Despliegue A: Docker Compose (puntos obligatorios + Airflow/MLflow)

Despliegue rápido sin Kubernetes. Permite ejecutar y validar los 6 puntos
de la práctica: obligatorios (1-5) + Airflow/MLflow (7).

**Componentes que arranca este despliegue**:

- MinIO + bucket lakehouse
- Kafka KRaft + 2 topics (`flight-delay-request`, `flight-delay-classification-response`)
- Cassandra + keyspace `flight_db` + 2 tablas + carga de 4696 distancias
- Iceberg REST catalog
- Spark master + worker
- Web Flask (con WebSocket)
- PostgreSQL (backend Airflow + MLflow)
- MLflow tracking server
- Airflow (init + webserver + scheduler) con 2 DAGs

### Paso 1: Variables de entorno

```bash
cd ~/practica_big_data/deployment/docker
cp .env.example .env

# Editar .env solo si quieres cambiar credenciales por defecto
# Las credenciales por defecto son:
#   MinIO:    minioadmin / minioadmin123
#   Postgres: bigdata / bigdata-secret-2026
#   Airflow:  admin / admin
```

### Paso 2: Construcción de imágenes propias

```bash
cd ~/practica_big_data/deployment/docker

# Construir las 4 imágenes propias (10-15 min la primera vez)
# - bigdata/spark:4.1.1   (~2 GB, base Apache Spark + conectores Iceberg/Cassandra)
# - bigdata/web:1.0       (~170 MB, Flask + SocketIO)
# - bigdata/mlflow:2.18.0 (~940 MB, MLflow + boto3)
# - bigdata/airflow:2.10.5 (~2 GB, Airflow + Docker CLI)

docker compose build spark-master web mlflow airflow-webserver

# Verificar imágenes
docker images | grep "bigdata/"
```

### Paso 3: Levantar el stack completo

```bash
cd ~/practica_big_data/deployment/docker

docker compose up -d

# Esperar a que los healthchecks pasen (3-5 min)
sleep 60
docker compose ps

# Todos los servicios deben estar "(healthy)" o "Up"
# Si alguno está "unhealthy", ver sección Troubleshooting
```

### Paso 4: Ingesta inicial del lakehouse Iceberg (punto 1)

```bash
# Ingestar dataset crudo a tabla Iceberg
docker compose exec spark-master spark-submit \
  /opt/spark/jobs/ingest_training_data.py

# Resultado esperado al final del log:
#   >>> Registros en la tabla Iceberg: 457013
#   INGESTA ICEBERG: OK
```

### Paso 5: Entrenamiento del modelo (opcional)

Los modelos ya vienen pre-entrenados en `models/`. Solo es necesario
re-entrenar si quieres regenerarlos:

```bash
docker compose exec spark-master spark-submit \
  /opt/spark/jobs/train_spark_mllib_model.py

# Tarda 4-6 minutos
# Sobrescribe los modelos en /opt/spark/models (montado a ./models del host)
# Y registra el experimento en MLflow
```

### Paso 6: Lanzar el job de predicción streaming (puntos 3 y 4)

```bash
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
| Web (formulario predicción) | http://localhost:5000 | -- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master UI | http://localhost:8080 | -- |
| Spark Worker UI | http://localhost:8081 | -- |
| Iceberg REST | http://localhost:8181 | -- |
| Airflow | http://localhost:8082 | admin / admin |
| MLflow | http://localhost:5050 | -- |

### Paso 8: Smoke test (predicción end-to-end)

1. Abrir http://localhost:5000
2. Rellenar formulario:
   - Date: 2026-12-25
   - Carrier: AA
   - Origin: ATL
   - Destination: SFO
   - Departure Delay: 0
3. Pulsar "Predict"
4. La predicción aparece en pocos segundos en la UI (vía WebSocket)
5. Verificar que se guardó en Cassandra:

```bash
docker compose exec cassandra cqlsh -e \
  "SELECT COUNT(*) FROM flight_db.flight_delay_predictions;"
```

### Paso 9: Operar Airflow (punto 7)

```bash
# Despausar los 2 DAGs
docker compose exec airflow-scheduler airflow dags unpause retrain_flight_model
docker compose exec airflow-scheduler airflow dags unpause cleanup_old_predictions

# Disparar manualmente (opcional)
docker compose exec airflow-scheduler airflow dags trigger retrain_flight_model
docker compose exec airflow-scheduler airflow dags trigger cleanup_old_predictions

# Verificar en UI: http://localhost:8082
```

### Paso 10: Parar el stack

```bash
cd ~/practica_big_data/deployment/docker

docker compose stop     # Mantiene los volúmenes (datos persisten)
# o
docker compose down     # Borra contenedores, mantiene volúmenes
# o
docker compose down -v  # Borra TODO incluido los datos
```

---

## Despliegue B: Kubernetes local con kind

Despliegue completo en Kubernetes. Cubre puntos 1-6 (todos los obligatorios
+ K8s). Airflow + MLflow se quedan en Docker Compose (por la rúbrica del
punto 7).

**Componentes que arranca este despliegue**:

- MinIO (StatefulSet + 3 Services + bootstrap Job)
- Kafka KRaft (StatefulSet + 3 Services + bootstrap Job)
- Cassandra (StatefulSet + 2 Services + schema Job + load-distances Job)
- Iceberg REST (Deployment + 2 Services)
- Spark master + worker (2 Deployments + Services)
- Spark Streaming job (Deployment, MakePrediction.jar)
- Web Flask (Deployment + 2 Services)

**Nota importante**: Docker Compose y kind NO pueden correr a la vez en WSL2
con 10 GB RAM. Antes de arrancar kind, parar Docker Compose.

### Paso 1: Parar Docker Compose (si está activo)

```bash
cd ~/practica_big_data/deployment/docker
docker compose stop

# Verificar que no hay contenedores activos del stack
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

### Paso 2: Construir imágenes propias

La imagen Spark incluye en su build el `flight_prediction.jar`, los
modelos pre-entrenados y el dataset (necesario porque K8s no soporta bind
mounts).

```bash
cd ~/practica_big_data/deployment/docker

# Si ya construiste en el Despliegue A, no es necesario rehacer
docker compose build spark-master web

# Verificar
docker images | grep -E "bigdata/spark|bigdata/web"
```

### Paso 3: Crear cluster kind

```bash
cd ~/practica_big_data/deployment/k8s

# El config expone 8 NodePorts al host:
#  30050 (web), 30090-91 (MinIO), 30180-83 (Spark+Iceberg),
#  30002 (Grafana), 30094 (Kafka external)
kind create cluster --config kind-config.yaml

# Verificar
kubectl cluster-info --context kind-bigdata
kubectl get nodes
```

### Paso 4: Cargar imágenes propias en el cluster

kind crea un nodo aislado del Docker host. Hay que copiar las imágenes
manualmente con `kind load`:

```bash
# Tarda 1-2 min cada una
kind load docker-image bigdata/spark:4.1.1 --name bigdata
kind load docker-image bigdata/web:1.0     --name bigdata

# Verificar
docker exec bigdata-control-plane crictl images | grep "bigdata/"
```

### Paso 5: Aplicar manifiestos en orden

```bash
cd ~/practica_big_data/deployment/k8s

# 1. Base (namespace, secrets, configmaps)
kubectl apply -f 00-base/

# 2. MinIO + esperar a que esté Ready (los demás dependen)
kubectl apply -f 10-minio/
kubectl -n bigdata wait --for=condition=ready pod/minio-0 --timeout=180s
kubectl -n bigdata wait --for=condition=complete job/minio-bootstrap --timeout=120s
```

### Paso 6: Subir datasets y modelos a MinIO

Los Jobs K8s leen los datasets desde MinIO (no del host filesystem).
Primera vez (y cada vez que se recree el cluster):

```bash
cd ~/practica_big_data

docker run --rm --network host --entrypoint sh \
  -v "$(pwd)/data:/data:ro" \
  -v "$(pwd)/models:/models:ro" \
  minio/mc:RELEASE.2025-04-08T15-39-49Z \
  -c "
    mc alias set local http://localhost:30091 minioadmin minioadmin123 >/dev/null
    echo '>>> Subiendo distancias...'
    mc cp /data/origin_dest_distances.jsonl       local/lakehouse/raw/
    echo '>>> Subiendo dataset training...'
    mc cp /data/simple_flight_delay_features.jsonl.bz2 local/lakehouse/raw/
    echo '>>> Subiendo modelos...'
    mc cp --recursive /models/                    local/lakehouse/models/
    echo ''
    echo '--- Contenido lakehouse/raw/ ---'
    mc ls local/lakehouse/raw/
  "
```

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

# Esperar a que todo esté Ready (5-10 min primera vez por descargas)
kubectl -n bigdata wait --for=condition=ready pod/kafka-0 --timeout=600s
kubectl -n bigdata wait --for=condition=ready pod/cassandra-0 --timeout=600s
kubectl -n bigdata wait --for=condition=available deployment/iceberg-rest --timeout=180s
kubectl -n bigdata wait --for=condition=available deployment/spark-master --timeout=180s
kubectl -n bigdata wait --for=condition=available deployment/spark-worker --timeout=180s
```

### Paso 8: Reiniciar Web Flask

El pod Web puede crashear durante el arranque inicial porque intenta
conectar a Kafka antes de que esté Ready. Tras la espera, basta con un
restart:

```bash
kubectl -n bigdata rollout restart deployment/web
kubectl -n bigdata wait --for=condition=available deployment/web --timeout=120s

# Verificar
curl -s http://localhost:30050/health
# Debe devolver: {"status":"healthy"}
```

### Paso 9: Validar Jobs de bootstrap

```bash
kubectl -n bigdata get jobs

# Esperado:
#   minio-bootstrap            Complete
#   kafka-bootstrap            Complete
#   cassandra-schema           Complete
#   cassandra-load-distances   Complete  -> puede aparecer "Failed", ver paso 10
```

### Paso 10: Relanzar cassandra-load-distances si falló

Es un caso recurrente (el Job arranca antes de tiempo y se queda sin
reintentos):

```bash
# Solo si "Failed":
kubectl -n bigdata delete job cassandra-load-distances
kubectl apply -f 30-cassandra/04-load-distances-job.yaml
kubectl -n bigdata wait --for=condition=complete job/cassandra-load-distances --timeout=180s
```

Verificación:

```bash
kubectl -n bigdata run cassandra-verify --rm -i --restart=Never \
  --image=cassandra:5.0 -- \
  cqlsh cassandra -e "SELECT COUNT(*) FROM flight_db.origin_dest_distances;" 2>/dev/null
# Debe devolver: count = 4696
```

### Paso 11: Ingesta Iceberg (punto 1)

Atención al uso de cores: el cluster tiene 1 worker con 2 cores. Si el
streaming está corriendo ocupa los 2 cores y la ingesta se queda bloqueada.
Conviene parar el streaming, ingestar, relanzar.

```bash
cd ~/practica_big_data/deployment/k8s

# 1. Si el streaming está activo, escalarlo a 0
kubectl -n bigdata scale deployment/spark-streaming --replicas=0 2>/dev/null

# 2. Ingesta
kubectl apply -f 50-spark/05-ingest-job.yaml
kubectl -n bigdata wait --for=condition=complete job/iceberg-ingest --timeout=300s

# Verificar
POD=$(kubectl -n bigdata get pods -l job-name=iceberg-ingest -o jsonpath='{.items[0].metadata.name}')
kubectl -n bigdata logs $POD -c ingest | grep -E "Registros|INGESTA"
# Debe mostrar:
#   >>> Registros leidos: 457013
#   >>> Registros en la tabla Iceberg: 457013
#   INGESTA ICEBERG: OK
```

### Paso 12: Lanzar streaming (puntos 3 y 4)

```bash
kubectl apply -f 50-spark/06-streaming-deployment.yaml
kubectl -n bigdata wait --for=condition=available deployment/spark-streaming --timeout=120s

# Esperar 20s a que se registre en Spark Master
sleep 20

# Verificar
kubectl -n bigdata exec deployment/spark-master -- \
  curl -s http://localhost:8080/json/ 2>/dev/null | python3 -c "
import json, sys
d = json.load(sys.stdin)
apps = d.get('activeapps', [])
print(f'Apps activas: {len(apps)}')
for a in apps:
    print(f\"  - {a.get('name')}\")
"
# Debe mostrar: Apps activas: 1, - FlightDelayStreamingPredictor
```

### Paso 13: Acceder a las interfaces

| Servicio | URL |
|---|---|
| Web (predicción) | http://localhost:30050 |
| MinIO Console | http://localhost:30090 |
| MinIO S3 API | http://localhost:30091 |
| Spark Master UI | http://localhost:30180 |
| Spark Worker UI | http://localhost:30181 |
| Iceberg REST | http://localhost:30183 |

### Paso 14: Smoke test end-to-end

1. Abrir http://localhost:30050
2. Rellenar formulario (mismos valores que en Despliegue A paso 8)
3. Pulsar "Predict"
4. La predicción aparece en segundos
5. Verificar persistencia:

```bash
kubectl -n bigdata run cassandra-verify --rm -i --restart=Never \
  --image=cassandra:5.0 -- \
  cqlsh cassandra -e "SELECT COUNT(*) FROM flight_db.flight_delay_predictions;" 2>/dev/null
```

### Paso 15: Borrar el cluster

```bash
kind delete cluster --name bigdata

# Verificar
kind get clusters
free -h | head -2
```

---

## Despliegue C: Observabilidad (Prometheus + Grafana en K8s)

Stack `kube-prometheus-stack` v86.1.1 (chart oficial). Instala Prometheus +
Grafana + Alertmanager + node-exporter + kube-state-metrics + Prometheus
Operator de forma atómica.

**Requisito previo**: cluster kind ya creado y el stack del Despliegue B
funcionando.

### Paso 1: Añadir repo Helm e instalar el chart

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

El fichero `values.yaml` personaliza la instalación para WSL2:

- Alertmanager deshabilitado (no necesario en local)
- Prometheus con 5GB de retención y recursos modestos
- Grafana con 768Mi RAM (ajustado tras detectar OOM con 256Mi)
- Componentes K8s no scrapeables en kind deshabilitados
- `serviceMonitorSelectorNilUsesHelmValues: false` para descubrir
  ServiceMonitors de todos los namespaces

Verificar que los pods están Running:

```bash
kubectl -n monitoring get pods
# Esperado: 5 pods running (grafana, operator, kube-state-metrics,
#           node-exporter, prometheus)
```

### Paso 2: Aplicar ServiceMonitor para Spark y NodePort de Grafana

```bash
cd ~/practica_big_data/deployment/k8s/70-monitoring

kubectl apply -f 01-spark-servicemonitor.yaml
kubectl apply -f 02-grafana-nodeport.yaml

# Verificar
kubectl -n monitoring get servicemonitor
kubectl -n monitoring get svc grafana-external
```

### Paso 3: Importar dashboard de Spark

```bash
cd ~/practica_big_data/deployment/k8s/70-monitoring

# Esperar 30s a que Prometheus haga el primer scrape
sleep 30

DASHBOARD_JSON=$(cat spark-dashboard.json)

curl -s -X POST http://admin:bigdata2026@localhost:30002/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d "{
    \"dashboard\": $DASHBOARD_JSON,
    \"overwrite\": true,
    \"folderUid\": \"\"
  }" | python3 -m json.tool
```

### Paso 4: Acceder a Grafana

URL: http://localhost:30002

Usuario: `admin`
Password: `bigdata2026`

Navegar a Dashboards y abrir **Spark - Practica BigData 2026**. Debe
mostrar 8 paneles con datos:

- Workers vivos = 1
- Apps activas = 1 (FlightDelayStreamingPredictor)
- Apps en espera = 0
- Heap total = 130 MB (aprox.)
- JVM Heap por componente (timeseries)
- JVM Non-heap memory (timeseries)
- GC rate (timeseries)
- JVM total memory usage (timeseries)

Además, los **dashboards default del chart** muestran el estado del cluster
K8s entero. Especialmente útiles:

- Kubernetes / Compute Resources / Namespace (Pods) -> filtrar por `bigdata`
- Node Exporter / Nodes
- Kubernetes / Persistent Volumes

### Paso 5: Acceder a Prometheus directamente (opcional)

```bash
# Port-forward (limitado: se puede caer si idle por mucho tiempo)
kubectl -n monitoring port-forward svc/kube-prometheus-stack-prometheus 9090:9090 &
# Luego abrir http://localhost:9090

# Para parar:
pkill -f "port-forward.*prometheus"
```

### Paso 6: Desinstalar (cuando ya no se necesite)

```bash
helm uninstall kube-prometheus-stack -n monitoring
kubectl delete namespace monitoring
```

---

## Decisiones técnicas relevantes

### Sobre la inclusión de datasets y modelos en el repo

Los datasets (`data/`), modelos pre-entrenados (`models/`) y JAR de
streaming (`deployment/docker/spark/jobs/flight_prediction.jar`) están
incluidos en el repositorio para garantizar reproducibilidad.


Duplicación intencionada:

- `data/simple_flight_delay_features.jsonl.bz2` (raíz) y
  `deployment/docker/spark/simple_flight_delay_features.jsonl.bz2`
  (build context Spark) son la misma cosa.
- Similar para `models/` y `deployment/docker/spark/models_to_embed/`.

La razón: el Dockerfile de Spark necesita los ficheros en el build
context para empotrarlos en la imagen (porque K8s no soporta bind mounts).
Mantenerlos también en raíz permite el bind mount de Docker Compose.

### Sobre Kubernetes

1. **`enableServiceLinks: false`** en todos los pods de Spark y Web.
   K8s inyecta env vars automáticas para cada Service del namespace
   (`SPARK_MASTER_PORT="tcp://10.96.x.y:7077"`) que colisionan con las
   variables nativas de Spark (que esperan un entero como puerto). Sin
   esta flag, el master crashea con `NumberFormatException`.

2. **`spark.driver.host=$POD_IP`** + `bindAddress=0.0.0.0` + puertos fijos
   (40000/40001) en todos los `spark-submit`. El executor del worker debe
   poder conectar al driver del pod. Sin esto, la app se queda en "Initial
   job has not accepted any resources" indefinidamente.

3. **Datos, scripts y modelos dentro de la imagen Spark** (no bind mount).
   En docker-compose se usaban bind mounts. En K8s no existen, y los pods
   driver y executor son independientes. Por eso el `Dockerfile` incluye
   `COPY jobs/`, `COPY tests/`,
   `COPY simple_flight_delay_features.jsonl.bz2` y `COPY models_to_embed/`.
   Cada cambio en estos ficheros requiere rebuild + `kind load docker-image`
   + recreación de los pods.

4. **Sondas TCP en lugar de exec con JVM cliente**. Probar con
   `kafka-topics.sh --list` arranca una JVM cliente con cold start de 5-8s
   y la sonda con timeout 5s falla. Solución: `tcpSocket: { port: 9092 }`.

5. **StatefulSet + headless Service** para servicios con identidad
   persistente (MinIO, Kafka, Cassandra). PVC dinámicos vía
   local-path-provisioner incluido en kind.

6. **Streaming como Deployment de larga duración**, no Job. Razón: estos
   pods deben reiniciarse si caen. K8s lo gestiona automáticamente con
   Deployment.

7. **Labels explícitas en los Services** (`metadata.labels`), no solo en
   los pods. Sin ellas, los ServiceMonitor de Prometheus no encuentran
   match (selector busca por labels del Service, no del Pod).

### Sobre Observabilidad

1. **Sink PrometheusServlet nativo de Spark** en lugar de exporters
   externos. Activado en `spark-defaults.conf` con
   `spark.metrics.conf.*.sink.prometheusServlet.*`. Expone métricas en
   formato Prometheus directamente en los puertos UI del master (8080) y
   worker (8081), sin necesidad de JMX exporter como sidecar.

2. **Dashboard custom** en lugar de los de la comunidad. Los dashboards
   conocidos (ID 7890, 19632) asumen naming convention de JMX exporter
   (`jvm_memory_used_bytes`) pero Spark con PrometheusServlet usa nombres
   distintos (`metrics_jvm_heap_used_Value`). Se ha creado uno propio
   con queries que coinciden con las métricas reales.

3. **Grafana via NodePort** (30002) en vez de port-forward. `kubectl
   port-forward` es inestable: se cae con frecuencia ("lost connection
   to pod") y consume RAM en el cliente. NodePort vive en iptables del
   nodo y no se cae.

4. **Recursos Grafana ajustados** a 768Mi tras detectar OOM con 256Mi.
   El pod tiene 3 containers (Grafana + 2 sidecars de
   dashboards/datasources) que juntos exceden 256Mi.

### Sobre la arquitectura general

1. **Iceberg REST en vez de Hadoop catalog**. Más simple, sin
   dependencias hadoop-aws.

2. **S3FileIO directo** desde Spark/Iceberg. No necesita JARs adicionales.

3. **PostgreSQL único** para Airflow y MLflow (dos schemas lógicos).

4. **MLflow con artifact store en MinIO** (no en disco local del tracking
   server). Esto garantiza que los artefactos sobreviven a restarts del
   pod MLflow.

---

## Troubleshooting completo

Lista de problemas que pueden aparecer durante el despliegue, con
diagnóstico y solución probada.

### Problemas con Docker Compose

#### 1. `Pool overlaps with other one on this address space`

```
Error response from daemon: Pool overlaps with other one on this address space
```

Causa: Docker tiene redes residuales de despliegues anteriores que ocupan
el subnet que pide compose.yaml.

Solución:
```bash
docker compose down
docker network prune -f
docker compose up -d
```

#### 2. Servicios "unhealthy" tras varios minutos

Diagnóstico:
```bash
docker compose ps
docker compose logs <servicio> --tail 50
```

Solución habitual:
```bash
docker compose restart <servicio>
sleep 30
docker compose ps
```

#### 3. Airflow webserver: `airflow.exceptions.AirflowConfigException`

Causa: el contenedor `airflow-init` no terminó correctamente.

Solución:
```bash
docker compose down -v   # Borra TODO incluido el volumen postgres
docker compose up -d postgres
sleep 20
docker compose up -d
```

#### 4. MLflow: `Could not connect to MinIO`

Causa: MinIO no está accesible cuando MLflow arranca.

Solución: añadir `depends_on` apropiado en `compose.yaml` ya está hecho,
pero si pasa de todos modos:
```bash
docker compose restart mlflow
```

#### 5. Spark streaming: `Failed to load class es.upm.dit.ging.predictor.MakePrediction`

Causa: la clase Scala se ha tipeado mal en el `spark-submit`.

Solución: usar EXACTAMENTE este nombre de clase:
```
es.upm.dit.ging.predictor.MakePrediction
```

### Problemas con Kubernetes (kind)

#### 6. `kind create cluster` falla con `unable to mount cgroup`

Causa: WSL2 no tiene cgroups v2 habilitado.

Solución (en Windows):
```powershell
# Como administrador, editar %USERPROFILE%\.wslconfig
[wsl2]
kernelCommandLine = cgroup_no_v1=all
```
Luego: `wsl --shutdown` y reabrir WSL2.

#### 7. Pods en `ContainerCreating` por mucho tiempo

Causa: descarga lenta de imágenes oficiales (Kafka, Cassandra son grandes).

Diagnóstico:
```bash
kubectl -n bigdata describe pod <pod-name> | tail -20
```

Si los eventos muestran `Pulling image "apache/kafka:4.2.0"` -> solo
esperar. Primera ejecución puede tardar 10 minutos.

#### 8. Spark master: `NumberFormatException: For input string: "tcp://10.96.x.y:7077"`

Causa: K8s inyecta env vars automáticas que colisionan con variables
Spark nativas.

Solución: comprobar que el manifest tiene `enableServiceLinks: false`:
```yaml
spec:
  template:
    spec:
      enableServiceLinks: false   # CRÍTICO
      containers:
        - name: spark-master
          ...
```

#### 9. Spark job: `Initial job has not accepted any resources` (infinito)

Causa: el executor del worker no puede conectar al driver del pod.

Solución: el `spark-submit` debe incluir:
```bash
--conf spark.driver.host=$POD_IP \
--conf spark.driver.bindAddress=0.0.0.0 \
--conf spark.driver.port=40000 \
--conf spark.driver.blockManager.port=40001
```

Y el pod debe tener:
```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

#### 10. Pod streaming: `Failed to load class es.upm.dit.bigdata.MakePrediction`

Causa: clase Scala mal especificada (typo en el package).

Solución: verificar el package real con:
```bash
unzip -l deployment/docker/spark/jobs/flight_prediction.jar | grep MakePrediction
# Output: es/upm/dit/ging/predictor/MakePrediction.class
```

El `--class` correcto es: `es.upm.dit.ging.predictor.MakePrediction`.

#### 11. ServiceMonitor no descubre targets (0 targets)

Causa: el Service no tiene `metadata.labels`, solo `spec.selector`.
ServiceMonitor matchea por labels del Service.

Solución: añadir labels al Service:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: spark-master
  namespace: bigdata
  labels:               # AÑADIR
    app: spark-master   # AÑADIR
spec:
  ...
```

#### 12. Kafka readiness probe timeout

Causa: la sonda usa `exec` con `kafka-topics.sh` que arranca una JVM
cliente lenta.

Solución: cambiar a `tcpSocket`:
```yaml
readinessProbe:
  tcpSocket:
    port: 9092
  initialDelaySeconds: 20
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 6
```

#### 13. Web pod en `CrashLoopBackOff` con `NoBrokersAvailable`

Causa: Web arrancó antes que Kafka.

Solución:
```bash
kubectl -n bigdata rollout restart deployment/web
```

#### 14. `cassandra-load-distances` Job en estado `Failed`

Causa: el Job agotó reintentos (`backoffLimit`) mientras Cassandra o MinIO
aún no estaban Ready.

Solución:
```bash
kubectl -n bigdata delete job cassandra-load-distances
kubectl apply -f deployment/k8s/30-cassandra/04-load-distances-job.yaml
kubectl -n bigdata wait --for=condition=complete \
  job/cassandra-load-distances --timeout=180s
```

#### 15. Iceberg ingest atascado: `Initial job has not accepted any resources`

Causa: el spark-streaming está ocupando los 2 cores del worker y la
ingesta se queda esperando.

Solución:
```bash
kubectl -n bigdata scale deployment/spark-streaming --replicas=0
kubectl -n bigdata delete job iceberg-ingest
kubectl apply -f deployment/k8s/50-spark/05-ingest-job.yaml
kubectl -n bigdata wait --for=condition=complete \
  job/iceberg-ingest --timeout=300s
# Una vez termina:
kubectl -n bigdata scale deployment/spark-streaming --replicas=1
```

### Problemas con Observabilidad

#### 16. Grafana en `CrashLoopBackOff` o reinicia constantemente

Causa: OOM (Out Of Memory). El pod tiene 3 containers que requieren más
de 256Mi conjuntamente.

Diagnóstico:
```bash
kubectl -n monitoring get pods -l app.kubernetes.io/name=grafana
# Ver columna RESTARTS - si está creciendo, OOM probable
kubectl -n monitoring describe pod <grafana-pod> | grep -A2 -i "oom"
```

Solución: aumentar limit en `values.yaml`:
```yaml
grafana:
  resources:
    requests:
      cpu: 50m
      memory: 256Mi
    limits:
      cpu: 200m
      memory: 768Mi    # antes 256Mi
```

Y aplicar:
```bash
helm upgrade kube-prometheus-stack \
  prometheus-community/kube-prometheus-stack \
  --version 86.1.1 \
  -n monitoring \
  --values values.yaml --reuse-values \
  --wait --timeout 5m
```

#### 17. `kubectl port-forward` se cae con `lost connection to pod`

Causa: limitación conocida de `kubectl port-forward` (no robusto a
inactividad o renegociación HTTP/2).

Solución: usar NodePort en lugar de port-forward. Para Grafana ya está
configurado (puerto 30002). Para Prometheus, si lo necesitas estable,
añadir un Service NodePort similar.

Alternativa (auto-restart del port-forward):
```bash
nohup bash -c 'while true; do
  kubectl -n monitoring port-forward svc/<servicio> <puerto-host>:<puerto-svc>
  sleep 2
done' > /tmp/pf.log 2>&1 &
```

#### 18. Dashboard Grafana importado pero todos los paneles dicen "No data"

Causa A: UID del datasource incorrecto en el JSON.

Diagnóstico:
```bash
curl -s http://admin:bigdata2026@localhost:30002/api/datasources \
  | python3 -m json.tool | grep -E "name|uid"
```

Solución: el UID debe coincidir con el del datasource Prometheus en
Grafana (suele ser literalmente `"prometheus"`).

Causa B: nombre de métrica mal escrito en la query.

Diagnóstico: ir a Grafana -> Explore, escribir la query del panel y ver
si devuelve datos. Si no, ajustar el nombre.

Atención especial: las métricas con guión (`-`) en el nombre, como
`metrics_jvm_non-heap_used_Value`, pueden dar problemas. Algunos parsers
las interpretan como resta. Verificar con la API:
```bash
curl -s 'http://localhost:9090/api/v1/label/__name__/values' \
  | python3 -c "
import json, sys
names = json.load(sys.stdin).get('data', [])
print([n for n in names if 'non' in n and 'heap' in n])
"
```

#### 19. Prometheus targets en estado `DOWN`

Diagnóstico:
```bash
curl -s http://localhost:9090/api/v1/targets | python3 -c "
import json, sys
data = json.load(sys.stdin)
for t in data.get('data', {}).get('activeTargets', []):
    if t.get('health') != 'up':
        print(f\"{t.get('labels', {}).get('job')}: {t.get('lastError', '')}\")
"
```

Causas típicas:
- Endpoint `/metrics` no responde 200 -> revisar configuración del servicio
- Network policy bloqueando conexión -> no aplicable en kind por defecto
- Path mal escrito en el ServiceMonitor

### Problemas generales

#### 20. WSL2 se queda sin RAM y mata procesos

Causa: 10 GB de RAM asignados no son suficientes con todos los servicios
+ navegador + IDE.

Solución temporal: cerrar el navegador, IDE, herramientas mientras corre
el cluster.

Solución permanente (en Windows, `%USERPROFILE%\.wslconfig`):
```ini
[wsl2]
memory=12GB
processors=8
```
Luego `wsl --shutdown` y reabrir.

#### 21. `helm install` falla con `context deadline exceeded`

Causa: descarga lenta del chart o pods que no estabilizan en 10 min.

Solución:
```bash
# Aumentar el timeout
helm install ... --timeout 15m

# O instalar sin --wait y luego verificar manualmente
helm install ... --no-hooks
kubectl -n monitoring get pods -w
```

#### 22. Puertos del host ocupados (3000, 5000, 8080, etc.)

Diagnóstico:
```bash
sudo lsof -i :3000
```

Solución: matar el proceso que ocupa o cambiar el puerto expuesto en
el manifest correspondiente.

#### 23. Modelos perdidos tras `docker compose down -v`

Causa: `-v` borra los volúmenes y la carpeta `./models` del host se
mantiene pero los modelos generados en el volumen Spark sí se pierden.

Solución: los modelos pre-entrenados ya están en el repo. Si quieres
regenerarlos, ejecutar el entrenamiento:
```bash
docker compose exec spark-master spark-submit \
  /opt/spark/jobs/train_spark_mllib_model.py
```

#### 24. Página de Git diff que se queda en `:` (prompt del pager)

Causa: `git diff --stat` con mucho output abre el pager `less`.

Solución: pulsar `q` para salir. Si quieres evitar el pager:
```bash
git --no-pager diff --cached --stat
# o configurar permanentemente:
git config --global pager.diff false
```

---

## Limitaciones conocidas

- En el cluster local con 1 worker de 2 cores, ingesta y streaming no
  pueden correr a la vez. Operativa: parar uno, ejecutar el otro, relanzar.
- La imagen Iceberg REST en kind no persiste su catálogo entre
  recreaciones del cluster (usa SQLite en `/tmp`). Las tablas se
  reconstruyen relanzando los Jobs de ingesta. En GKE se le pondría un PVC.
- El cliente Kafka de Python en Web puede crashear si arranca antes de
  que Kafka esté Ready. Solución: `kubectl rollout restart deployment/web`
  cuando todos los servicios estén listos.
- El stack Docker Compose y el cluster kind no pueden correr a la vez en
  WSL2 con 10 GB de RAM. Antes de arrancar uno, parar el otro.
- Los datasets (`data/`), modelos (`models/`) y JAR (`flight_prediction.jar`)
  vienen incluidos en el repo para reproducibilidad. Si los modificas
  manualmente, no usar `git clean` o se perderán.

---

## Comandos de cierre

### Cierre temporal (mantiene los datos para próxima sesión)

```bash
# Docker Compose
cd ~/practica_big_data/deployment/docker
docker compose stop

# Kubernetes (kind)
kind delete cluster --name bigdata
# Los datos se pierden con kind delete, pero los manifiestos los regeneran
```

### Cierre completo (limpieza total)

```bash
# Docker Compose: borrar volúmenes
cd ~/practica_big_data/deployment/docker
docker compose down -v

# Borrar imágenes propias (opcional, libera ~5 GB)
docker rmi bigdata/spark:4.1.1 bigdata/web:1.0 \
  bigdata/mlflow:2.18.0 bigdata/airflow:2.10.5

# Borrar cluster kind
kind delete cluster --name bigdata

# Limpiar networks Docker huérfanas
docker network prune -f

# Verificar
docker ps -a
docker volume ls
kind get clusters
```

---

## Licencia y autoría

Basado en https://github.com/Big-Data-ETSIT/practica_creativa (plantilla
docente ETSIT-UPM, derivada a su vez de
https://github.com/rjurney/Agile_Data_Code_2).

