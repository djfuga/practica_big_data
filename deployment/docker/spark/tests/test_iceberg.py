"""
Smoke test: crear y leer una tabla Iceberg en MinIO desde Spark cluster.
Valida: modo distribuido + Iceberg 1.10.1 sobre Spark 4.1.1 + S3A/MinIO.
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergSmokeTest")
    # La config de Iceberg/S3A viene de spark-defaults.conf,
    # pero la repetimos aqui para que el test sea autocontenido.
    .getOrCreate()
)

print("=" * 60)
print(f"Spark version: {spark.version}")
print(f"Master: {spark.sparkContext.master}")
print("=" * 60)

# 1. Crear namespace (database) en el catalogo Iceberg 'lakehouse'
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.test_db")
print(">>> Namespace lakehouse.test_db creado")

# 2. Crear tabla Iceberg
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.test_db.smoke (
        id BIGINT,
        nombre STRING,
        valor DOUBLE
    ) USING iceberg
""")
print(">>> Tabla lakehouse.test_db.smoke creada")

# 3. Insertar datos
spark.sql("""
    INSERT INTO lakehouse.test_db.smoke VALUES
        (1, 'alpha', 10.5),
        (2, 'beta', 20.0),
        (3, 'gamma', 30.7)
""")
print(">>> 3 filas insertadas")

# 4. Leer de vuelta
print(">>> Contenido de la tabla:")
spark.sql("SELECT * FROM lakehouse.test_db.smoke ORDER BY id").show()

# 5. Contar
count = spark.sql("SELECT COUNT(*) AS n FROM lakehouse.test_db.smoke").collect()[0]["n"]
print(f">>> Total filas: {count}")

# 6. Mostrar historico Iceberg (prueba de que es Iceberg de verdad)
print(">>> Snapshots Iceberg:")
spark.sql("SELECT snapshot_id, committed_at FROM lakehouse.test_db.smoke.snapshots").show(truncate=False)

print("=" * 60)
print("SMOKE TEST ICEBERG: OK" if count == 3 else "SMOKE TEST ICEBERG: FALLO")
print("=" * 60)

spark.stop()
