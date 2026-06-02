"""
DAG: cleanup_old_predictions
============================
Limpieza diaria de la tabla flight_db.flight_delay_predictions:
  1. count_before:                cuenta filas totales en la tabla
  2. delete_older_than_30_days:   borra predicciones con created_at < NOW - 30 dias
  3. count_after:                 cuenta filas tras la limpieza y reporta

Mantiene la tabla de predicciones con un tamaño razonable.
Cumple parte del punto 7 (Airflow) y mantiene el sistema saneado.

Configuración via env vars:
  CASSANDRA_HOST     (default: cassandra)
  CASSANDRA_PORT     (default: 9042)
  CASSANDRA_KEYSPACE (default: flight_db)
  RETENTION_DAYS     (default: 30)
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Defaults configurables
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "flight_db")
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "30"))


def _get_session():
    """Crea una CqlSession lista para usar (datacenter1 = default Cassandra)."""
    from cassandra.cluster import Cluster
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    return cluster, cluster.connect(CASSANDRA_KEYSPACE)


def count_predictions(**context):
    """Cuenta el total de filas en flight_delay_predictions."""
    cluster, session = _get_session()
    try:
        row = session.execute("SELECT COUNT(*) FROM flight_delay_predictions").one()
        count = row[0] if row else 0
        print(f">>> Total predicciones en Cassandra: {count}")
        # Guardar el valor para que la siguiente task lo pueda leer (XCom)
        return count
    finally:
        cluster.shutdown()


def delete_old_predictions(**context):
    """Borra predicciones con created_at < (NOW - RETENTION_DAYS)."""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    print(f">>> Borrando predicciones con created_at < {cutoff.isoformat()}")

    cluster, session = _get_session()
    try:
        # Cassandra requiere ALLOW FILTERING porque created_at no es partition key.
        # Para una tabla pequeña (predicciones) es aceptable.
        # En produccion se usaria una tabla con TTL o una particion temporal.
        rows = session.execute(
            "SELECT uuid FROM flight_delay_predictions "
            "WHERE created_at < %s ALLOW FILTERING",
            (cutoff,)
        )
        uuids_to_delete = [r.uuid for r in rows]
        print(f">>> {len(uuids_to_delete)} filas candidatas a borrar")

        if uuids_to_delete:
            delete_stmt = session.prepare(
                "DELETE FROM flight_delay_predictions WHERE uuid = ?"
            )
            for uuid in uuids_to_delete:
                session.execute(delete_stmt, (uuid,))
            print(f">>> Borradas {len(uuids_to_delete)} predicciones antiguas")
        else:
            print(">>> Nada que borrar (no hay filas más viejas que el corte)")

        return len(uuids_to_delete)
    finally:
        cluster.shutdown()


def report_result(**context):
    """Reporta el antes/despues comparando los XCom de las tareas previas."""
    ti = context["ti"]
    before = ti.xcom_pull(task_ids="count_before")
    deleted = ti.xcom_pull(task_ids="delete_older_than_30_days")
    after = count_predictions()

    print("=" * 50)
    print(f"Limpieza diaria de predicciones (retencion: {RETENTION_DAYS} dias)")
    print(f"  Filas antes:    {before}")
    print(f"  Filas borradas: {deleted}")
    print(f"  Filas después:  {after}")
    print("=" * 50)
    return {"before": before, "deleted": deleted, "after": after}


default_args = {
    "owner": "bigdata-2026",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cleanup_old_predictions",
    description=f"Limpieza diaria predicciones antiguas (>{RETENTION_DAYS} dias) en Cassandra",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bigdata", "cassandra", "maintenance"],
) as dag:

    start = EmptyOperator(task_id="start")

    count_before = PythonOperator(
        task_id="count_before",
        python_callable=count_predictions,
    )

    delete_old = PythonOperator(
        task_id="delete_older_than_30_days",
        python_callable=delete_old_predictions,
    )

    report = PythonOperator(
        task_id="count_after_and_report",
        python_callable=report_result,
    )

    end = EmptyOperator(task_id="end")

    start >> count_before >> delete_old >> report >> end
