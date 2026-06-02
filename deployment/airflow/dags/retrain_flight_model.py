"""
DAG: retrain_flight_model
=========================
Re-entrena semanalmente el modelo de prediccion de retrasos:
  1. Ejecuta train_spark_mllib_model.py en el cluster Spark
     (lee Iceberg, entrena, guarda los 7 modelos en /opt/spark/models)
  2. Sube los modelos al Lakehouse (MinIO)
  3. El training registra en MLflow automaticamente.

Cumple los puntos obligatorios 4 (training en Lakehouse) y 7 (Airflow+MLflow).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "bigdata-2026",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retrain_flight_model",
    description="Re-entrenamiento semanal del modelo RandomForest con MLflow tracking",
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bigdata", "training", "mlflow"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Entrenar en el cluster Spark via docker exec.
    #    El script registra en MLflow automaticamente (variable de entorno).
    train = BashOperator(
        task_id="train_model",
        bash_command=(
            "docker exec "
            "-e MLFLOW_TRACKING_URI=http://mlflow:5000 "
            "-e MLFLOW_EXPERIMENT_NAME=flight_delay_training "
            "-e MLFLOW_S3_ENDPOINT_URL=http://minio:9000 "
            "-e AWS_ACCESS_KEY_ID=bigdata "
            "-e AWS_SECRET_ACCESS_KEY=bigdata-secret-2026 "
            "-e AWS_DEFAULT_REGION=us-east-1 "
            "bigdata-spark-master "
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "/opt/spark/jobs/train_spark_mllib_model.py"
        ),
    )

    # 2. Subir los modelos a MinIO usando 'mc' desde el contenedor minio.
    upload_models = BashOperator(
        task_id="upload_models_to_lakehouse",
        bash_command=(
            "docker cp bigdata-spark-master:/opt/spark/models /tmp/airflow-models-$$ && "
            "docker cp /tmp/airflow-models-$$ bigdata-minio:/tmp/models-new && "
            "docker exec bigdata-minio sh -c \""
            "  mc alias set local http://localhost:9000 minioadmin minioadmin123 >/dev/null 2>&1 && "
            "  mc cp --recursive /tmp/models-new/ local/lakehouse/models/ && "
            "  rm -rf /tmp/models-new"
            "\" && "
            "rm -rf /tmp/airflow-models-$$"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> train >> upload_models >> end
