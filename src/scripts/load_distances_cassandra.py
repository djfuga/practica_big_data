#!/usr/bin/env python3
"""
load_distances_cassandra.py
Carga las distancias origen-destino del JSONL en Cassandra
flight_db.origin_dest_distances. (punto obligatorio 2)
"""
import sys
import json
import time

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
KEYSPACE = "flight_db"
TABLE = "origin_dest_distances"


def wait_for_cassandra(max_retries=10, delay=5):
    for i in range(max_retries):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            print(f">>> Conectado a Cassandra en intento {i + 1}")
            return cluster, session
        except Exception as e:
            print(f"    intento {i + 1}/{max_retries}: Cassandra no lista ({e})")
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a Cassandra")


def main():
    jsonl_path = sys.argv[1] if len(sys.argv) > 1 else "/data/origin_dest_distances.jsonl"

    cluster, session = wait_for_cassandra()
    session.set_keyspace(KEYSPACE)

    insert_stmt = session.prepare(
        f"INSERT INTO {TABLE} (origin, dest, distance) VALUES (?, ?, ?)"
    )

    print(f">>> Cargando distancias desde {jsonl_path}...")
    count = 0
    batch = BatchStatement()
    batch_size = 0

    with open(jsonl_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            batch.add(insert_stmt, (rec["Origin"], rec["Dest"], float(rec["Distance"])))
            batch_size += 1
            count += 1
            if batch_size >= 50:
                session.execute(batch)
                batch = BatchStatement()
                batch_size = 0

    if batch_size > 0:
        session.execute(batch)

    print(f">>> {count} distancias insertadas")

    row = session.execute(f"SELECT COUNT(*) AS n FROM {TABLE}").one()
    print(f">>> Total filas en Cassandra: {row.n}")

    test = session.execute(
        f"SELECT distance FROM {TABLE} WHERE origin=%s AND dest=%s", ("ATL", "SFO")
    ).one()
    if test:
        print(f">>> Test lectura ATL->SFO: distance={test.distance}")

    print("=" * 50)
    print("CARGA DISTANCIAS CASSANDRA: OK" if count > 0 else "CARGA: FALLO")
    print("=" * 50)

    cluster.shutdown()


if __name__ == "__main__":
    main()
