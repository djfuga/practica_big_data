# Práctica Big Data 2026 — Parte II

Predicción de retrasos de vuelos con arquitectura Lakehouse en streaming.

> **Asignatura**: Big Data — Máster GISD, ETSIT UPM
> **Curso**: 2025-2026
> **Basado en**: https://github.com/Big-Data-ETSIT/practica_creativa

## 📐 Arquitectura

```
                    ┌───────────────┐
                    │ Apache Airflow│ (orquestación + reentrenamiento)
                    └──────┬────────┘
                           │
                           ▼
        ┌──────────────────────────────┐
        │       Apache Spark 4.1.1     │
        │  ┌─────────┐  ┌────────────┐ │
        │  │Training │  │ Streaming  │ │
        │  └────┬────┘  └──────┬─────┘ │
        └───────┼──────────────┼───────┘
                │              │
       ┌────────┴───┐    ┌─────┴────────┐
       │   MinIO    │    │    Kafka     │
       │  (S3+IC)   │    │  (KRaft 4.2) │
       │  Lakehouse │    └──────┬───────┘
       └────────────┘           │
                                │
       ┌────────────────────────┴──────┐
       │      Cassandra 5.0            │
       │ (distancias + predicciones)   │
       └──────────────┬────────────────┘
                      │
                      ▼
              ┌──────────────────┐
              │  Flask + SocketIO│
              └──────────────────┘
```

## 🔢 Stack de versiones

Consultar [`docs/VERSIONS.md`](docs/VERSIONS.md) para la matriz completa y justificada.

## 📁 Estructura del repositorio

```
practica_big_data/
├── deployment/         # Todo lo relativo a infraestructura
│   ├── gcp/            # Comandos y scripts de GCP (Compute Engine, GKE)
│   ├── docker/         # Dockerfiles y docker-compose.yml
│   ├── k8s/            # Manifiestos Kubernetes
│   └── airflow/        # DAGs y configuración Airflow
├── src/                # Código fuente
│   ├── flight_prediction/  # Job Scala de Spark Streaming
│   ├── web/                # Servidor Flask + WebSockets
│   ├── training/           # Script PySpark de entrenamiento
│   └── scripts/            # Utilidades (bootstrap, kafka producer test, etc.)
├── docs/               # Documentación
│   ├── VERSIONS.md
│   ├── DEPLOYMENT.md
│   └── ARCHITECTURE.md
├── data/               # Datasets (en .gitignore; los gestiona MinIO)
├── models/             # Modelos entrenados (en .gitignore)
└── source_original/    # Repo original como referencia (no se modifica)
```

## 🚀 Despliegue rápido

(Se completará al finalizar cada fase)

### Local (Docker Compose)
```bash
cd deployment/docker
docker compose up -d
```

### Kubernetes (GKE)
```bash
cd deployment/k8s
./deploy.sh
```

## 📋 Cumplimiento de la rúbrica Parte II

| Criterio | Puntos | Estado |
|---|---|---|
| Data Lakehouse con Iceberg (S3/MinIO) | 1 obl. | ⏳ |
| Distancias en Cassandra | 1 obl. | ⏳ |
| Predicciones en Kafka + WebSockets + Cassandra | 1 obl. | ⏳ |
| Training lee/escribe en Lakehouse | 1 obl. | ⏳ |
| Docker + docker-compose completo | 1 obl. | ⏳ |
| Despliegue en Kubernetes | 3 | ⏳ |
| Airflow + MLflow | 1 | ⏳ |
| Despliegue en GCloud | 1 | ⏳ |
| Observabilidad/Optimización | 1 | ⏳ |

## ⚙️ Operación

- **Levantar VM de GCP**: `./deployment/gcp/start.sh`
- **Apagar VM de GCP**: `./deployment/gcp/stop.sh`
- **Estado del crédito**: `gcloud billing accounts list`
