#!/usr/bin/env python
"""
train_spark_mllib_model.py  (adaptado Parte II)
================================================
Entrena el modelo RandomForest de prediccion de retrasos.

CAMBIOS respecto al original:
  - LEE los datos de la tabla Iceberg lakehouse.flights.training_data
    (en vez del JSONL local)  -> punto obligatorio 4 (leer del Lakehouse)
  - GUARDA los modelos en /opt/spark/models (volumen) que luego se sube
    a s3://lakehouse/models con mc  -> punto obligatorio 4 (modelos en Lakehouse)
  - Se ejecuta en modo distribuido en el cluster Spark

Uso (dentro del contenedor spark-master):
  spark-submit --master spark://spark-master:7077 \
    /opt/spark/jobs/train_spark_mllib_model.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat
from pyspark.ml.feature import Bucketizer, StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Tabla Iceberg de entrada (creada en la Fase 1.5)
SOURCE_TABLE = "lakehouse.flights.training_data"

# Directorio local (volumen) donde se guardan los modelos.
# Un paso posterior con 'mc' los sube a s3://lakehouse/models/
MODELS_DIR = "/opt/spark/models"


def main():
    spark = (
        SparkSession.builder
        .appName("TrainFlightDelayModel")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(f"Spark {spark.version} | master={spark.sparkContext.master}")
    print(f"Leyendo datos de la tabla Iceberg: {SOURCE_TABLE}")
    print("=" * 60)

    # -----------------------------------------------------------------
    # 1. LEER del Lakehouse (tabla Iceberg) en vez del JSONL local
    # -----------------------------------------------------------------
    features = spark.read.table(SOURCE_TABLE)
    total = features.count()
    print(f">>> Registros leidos del Lakehouse: {total}")

    # Comprobar nulos
    null_counts = [
        (c, features.where(features[c].isNull()).count())
        for c in features.columns
    ]
    cols_with_nulls = [x for x in null_counts if x[1] > 0]
    print(f">>> Columnas con nulos: {cols_with_nulls}")

    # -----------------------------------------------------------------
    # 2. Feature engineering (identico al original)
    # -----------------------------------------------------------------
    # Variable Route = Origin-Dest
    features_with_route = features.withColumn(
        "Route",
        concat(features.Origin, lit("-"), features.Dest)
    )

    # Bucketizar ArrDelay en 4 categorias (on-time, slight, late, very late)
    splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
    arrival_bucketizer = Bucketizer(
        splits=splits,
        inputCol="ArrDelay",
        outputCol="ArrDelayBucket"
    )
    arrival_bucketizer.write().overwrite().save(
        f"{MODELS_DIR}/arrival_bucketizer_2.0.bin"
    )
    print(">>> Bucketizer guardado")

    ml_bucketized_features = arrival_bucketizer.transform(features_with_route)

    # StringIndexer para columnas categoricas
    for column in ["Carrier", "Origin", "Dest", "Route"]:
        string_indexer = StringIndexer(
            inputCol=column,
            outputCol=column + "_index"
        )
        string_indexer_model = string_indexer.fit(ml_bucketized_features)
        ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
        ml_bucketized_features = ml_bucketized_features.drop(column)
        string_indexer_model.write().overwrite().save(
            f"{MODELS_DIR}/string_indexer_model_{column}.bin"
        )
        print(f">>> StringIndexer {column} guardado")

    # VectorAssembler
    numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
    index_columns = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]
    vector_assembler = VectorAssembler(
        inputCols=numeric_columns + index_columns,
        outputCol="Features_vec"
    )
    final_vectorized_features = vector_assembler.transform(ml_bucketized_features)
    vector_assembler.write().overwrite().save(
        f"{MODELS_DIR}/numeric_vector_assembler.bin"
    )
    print(">>> VectorAssembler guardado")

    for column in index_columns:
        final_vectorized_features = final_vectorized_features.drop(column)

    # -----------------------------------------------------------------
    # 3. Entrenar RandomForest
    # -----------------------------------------------------------------
    print(">>> Entrenando RandomForest (puede tardar)...")
    rfc = RandomForestClassifier(
        featuresCol="Features_vec",
        labelCol="ArrDelayBucket",
        predictionCol="Prediction",
        maxBins=4657,
        maxMemoryInMB=1024
    )
    model = rfc.fit(final_vectorized_features)
    model.write().overwrite().save(
        f"{MODELS_DIR}/spark_random_forest_classifier.flight_delays.5.0.bin"
    )
    print(">>> Modelo RandomForest guardado")

    # -----------------------------------------------------------------
    # 4. Evaluar
    # -----------------------------------------------------------------
    predictions = model.transform(final_vectorized_features)
    evaluator = MulticlassClassificationEvaluator(
        predictionCol="Prediction",
        labelCol="ArrDelayBucket",
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print(f">>> Accuracy = {accuracy}")
    predictions.groupBy("Prediction").count().show()

    print("=" * 60)
    print("ENTRENAMIENTO: OK")
    print(f"Modelos guardados en {MODELS_DIR} (subir a s3://lakehouse/models con mc)")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
