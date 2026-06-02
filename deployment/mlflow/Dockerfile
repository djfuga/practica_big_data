# =============================================================
# Imagen MLflow 2.18.0 con drivers PostgreSQL y soporte S3/MinIO
# =============================================================
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    mlflow==2.18.0 \
    psycopg2-binary==2.9.10 \
    boto3==1.35.99

EXPOSE 5000

# El comando real se pasa desde compose.yaml para mantener
# centralizada la configuración (backend URI, artifact root, etc.)
ENTRYPOINT ["mlflow"]
