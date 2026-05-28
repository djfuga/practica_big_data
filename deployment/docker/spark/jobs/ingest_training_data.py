"""
ingest_training_data.py
Ingesta el dataset de vuelos (JSONL local montado) como tabla Iceberg.
Cumple punto obligatorio 1: datos de entrenamiento en Lakehouse Iceberg.
Tabla resultante: lakehouse.flights.training_data
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType
)

TARGET_TABLE = "lakehouse.flights.training_data"
DEFAULT_RAW = "/opt/spark/data/simple_flight_delay_features.jsonl.bz2"

SCHEMA = StructType([
    StructField("ArrDelay", DoubleType(), True),
    StructField("CRSArrTime", TimestampType(), True),
    StructField("CRSDepTime", TimestampType(), True),
    StructField("Carrier", StringType(), True),
    StructField("DayOfMonth", IntegerType(), True),
    StructField("DayOfWeek", IntegerType(), True),
    StructField("DayOfYear", IntegerType(), True),
    StructField("DepDelay", DoubleType(), True),
    StructField("Dest", StringType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("FlightDate", DateType(), True),
    StructField("FlightNum", StringType(), True),
    StructField("Origin", StringType(), True),
])


def main():
    raw_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RAW

    spark = (
        SparkSession.builder
        .appName("IngestTrainingDataToIceberg")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(f"Spark {spark.version} | master={spark.sparkContext.master}")
    print(f"Leyendo JSONL crudo desde: {raw_path}")
    print("=" * 60)

    df = spark.read.json(raw_path, schema=SCHEMA)
    total = df.count()
    print(f">>> Registros leidos: {total}")
    df.show(5)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.flights")
    print(">>> Namespace lakehouse.flights listo")

    print(f">>> Escribiendo tabla Iceberg {TARGET_TABLE}...")
    df.writeTo(TARGET_TABLE).using("iceberg").createOrReplace()
    print(">>> Tabla Iceberg creada/reemplazada")

    count_iceberg = spark.sql(f"SELECT COUNT(*) AS n FROM {TARGET_TABLE}").collect()[0]["n"]
    print(f">>> Registros en la tabla Iceberg: {count_iceberg}")

    print(">>> Snapshots:")
    spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {TARGET_TABLE}.snapshots").show(truncate=False)

    ok = (count_iceberg == total and total > 0)
    print("=" * 60)
    print("INGESTA ICEBERG: OK" if ok else "INGESTA ICEBERG: FALLO")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
