# Datos de la práctica

Los datasets NO se versionan en Git (son pesados). Se descargan así:

```bash
cd data/
curl -Lo simple_flight_delay_features.jsonl.bz2 \
  "http://s3.amazonaws.com/agile_data_science/simple_flight_delay_features.jsonl.bz2"
curl -Lo origin_dest_distances.jsonl \
  "http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl"
```

## Ficheros

| Fichero | Tamaño | Registros | Uso |
|---|---|---|---|
| `simple_flight_delay_features.jsonl.bz2` | 4.5 MB | 457.013 | Entrenamiento del modelo (→ tabla Iceberg) |
| `origin_dest_distances.jsonl` | 218 KB | ~4.700 | Distancias origen-destino (→ Cassandra) |

## Ingesta al Lakehouse

Una vez levantado el stack y con los datos descargados:

```bash
cd deployment/docker
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/ingest_training_data.py
```

Esto crea la tabla Iceberg `lakehouse.flights.training_data`.
