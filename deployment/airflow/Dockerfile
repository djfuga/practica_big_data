# =============================================================
# Imagen Airflow 2.10.5 + cliente Docker CLI + providers necesarios
# El cliente Docker permite a los DAGs ejecutar 'docker exec' contra
# el cluster Spark y Cassandra que ya tenemos en el compose.
# =============================================================
FROM apache/airflow:2.10.5-python3.11

USER root

# Instalar Docker CLI (solo el cliente, no el daemon)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | \
      gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    rm -rf /var/lib/apt/lists/*

# Volver al usuario airflow (UID 50000) para instalar Python deps
USER airflow

# Providers que usaremos en los DAGs
RUN pip install --no-cache-dir \
      apache-airflow-providers-postgres==5.13.1 \
      cassandra-driver==3.29.2 \
      mlflow==2.18.0
