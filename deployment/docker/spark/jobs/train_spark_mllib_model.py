#!/usr/bin/env python
"""
train_spark_mllib_model.py (v3 - con MLflow)
============================================
Entrena el modelo RandomForest de prediccion de retrasos.
  - Lee datos del Lakehouse Iceberg (punto obl. 4)
  - Guarda modelos en /opt/spark/models (luego subido a MinIO)
  - Registra params + metricas en MLflow (punto 7)

Si MLflow no esta disponible (libreria no instalada), entrena sin tracking
y deja un aviso en logs. Esto permite que el script funcione tambien fuera
de Airflow.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat
from pyspark.ml.feature import Bucketizer, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

try:
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    print(">>> AVISO: MLflow no disponible, training sin tracking")

SOURCE_TABLE = "lakehouse.flights.training_data"
MODELS_DIR = "/opt/spark/models"
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT_NAME", "flight_delay_training")


def main():
    spark = SparkSession.builder.appName("TrainFlightDelayModel").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(f"Spark {spark.version} | master={spark.sparkContext.master}")
    if MLFLOW_AVAILABLE:
        print(f"MLflow: {MLFLOW_TRACKING_URI} -> exp '{MLFLOW_EXPERIMENT}'")
    print("=" * 60)

    if MLFLOW_AVAILABLE:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT)
        mlflow_run = mlflow.start_run()
        print(f">>> Run MLflow: {mlflow_run.info.run_id}")
    else:
        mlflow_run = None

    try:
        # 1. Leer datos
        features = spark.read.table(SOURCE_TABLE)
        total = features.count()
        print(f">>> Registros del Lakehouse: {total}")

        if MLFLOW_AVAILABLE:
            mlflow.log_param("training_records", total)
            mlflow.log_param("source_table", SOURCE_TABLE)
            mlflow.log_param("algorithm", "RandomForestClassifier")

        # 2. Feature engineering
        features_with_route = features.withColumn(
            "Route", concat(features.Origin, lit("-"), features.Dest)
        )

        splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
        bucketizer = Bucketizer(splits=splits, inputCol="ArrDelay", outputCol="ArrDelayBucket")
        bucketizer.write().overwrite().save(f"{MODELS_DIR}/arrival_bucketizer_2.0.bin")
        print(">>> Bucketizer guardado")

        ml_bucketized = bucketizer.transform(features_with_route)

        for column in ["Carrier", "Origin", "Dest", "Route"]:
            si = StringIndexer(inputCol=column, outputCol=column + "_index")
            m = si.fit(ml_bucketized)
            ml_bucketized = m.transform(ml_bucketized).drop(column)
            m.write().overwrite().save(f"{MODELS_DIR}/string_indexer_model_{column}.bin")
            print(f">>> StringIndexer {column} guardado")

        numeric_cols = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
        index_cols = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]
        va = VectorAssembler(inputCols=numeric_cols + index_cols, outputCol="Features_vec")
        final = va.transform(ml_bucketized)
        va.write().overwrite().save(f"{MODELS_DIR}/numeric_vector_assembler.bin")
        print(">>> VectorAssembler guardado")
        for c in index_cols:
            final = final.drop(c)

        # 3. RandomForest
        rf_params = {"maxBins": 4657, "maxMemoryInMB": 1024, "numTrees": 20}
        if MLFLOW_AVAILABLE:
            for k, v in rf_params.items():
                mlflow.log_param(k, v)

        print(">>> Entrenando RF...")
        rfc = RandomForestClassifier(
            featuresCol="Features_vec", labelCol="ArrDelayBucket",
            predictionCol="Prediction", **rf_params
        )
        model = rfc.fit(final)
        model.write().overwrite().save(
            f"{MODELS_DIR}/spark_random_forest_classifier.flight_delays.5.0.bin"
        )
        print(">>> Modelo RF guardado")

        # 4. Evaluar
        predictions = model.transform(final)
        accuracy = MulticlassClassificationEvaluator(
            predictionCol="Prediction", labelCol="ArrDelayBucket", metricName="accuracy"
        ).evaluate(predictions)
        f1 = MulticlassClassificationEvaluator(
            predictionCol="Prediction", labelCol="ArrDelayBucket", metricName="f1"
        ).evaluate(predictions)

        print(f">>> Accuracy={accuracy:.4f} F1={f1:.4f}")

        if MLFLOW_AVAILABLE:
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1", f1)
            print(">>> Metricas en MLflow")

        predictions.groupBy("Prediction").count().show()

        print("=" * 60)
        print("ENTRENAMIENTO: OK")
        print("=" * 60)

    finally:
        if MLFLOW_AVAILABLE and mlflow_run:
            mlflow.end_run()
        spark.stop()


if __name__ == "__main__":
    main()
