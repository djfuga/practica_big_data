#!/bin/bash
# Crea las dos bases de datos lógicas que necesitan Airflow y MLflow
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE airflow;
    CREATE DATABASE mlflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE mlflow TO $POSTGRES_USER;
EOSQL

echo "OK: bases de datos 'airflow' y 'mlflow' creadas"
