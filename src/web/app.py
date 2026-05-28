#!/usr/bin/env python3
"""
app.py - Servidor web Flask (Practica Big Data 2026, Parte II)
==============================================================
Flujo:
  1. El usuario envia los datos del vuelo por el formulario web.
  2. Flask calcula la Distance leyendola de CASSANDRA (punto obl. 2).
  3. Flask produce la peticion al topic Kafka 'flight-delay-request'.
  4. Spark Streaming predice y escribe a Kafka 'flight-delay-classification-response'
     (y a Cassandra).
  5. Un consumer Kafka en background dentro de Flask escucha el topic de respuesta
     y emite la prediccion al navegador por WEBSOCKET (punto obl. 3).
"""
import os
import json
import uuid
import threading
from datetime import datetime

import iso8601
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster

# ---------------------------------------------------------------
# Configuracion (via variables de entorno, con defaults para local)
# ---------------------------------------------------------------
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
REQUEST_TOPIC = os.environ.get("REQUEST_TOPIC", "flight-delay-request")
RESPONSE_TOPIC = os.environ.get("RESPONSE_TOPIC", "flight-delay-classification-response")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "flight_db")

# ---------------------------------------------------------------
# App Flask + SocketIO
# ---------------------------------------------------------------
app = Flask(__name__)
app.config["SECRET_KEY"] = "bigdata-practica-2026"
# async_mode threading: usa hilos, sin dependencias extra (eventlet/gevent)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ---------------------------------------------------------------
# Conexiones (se inicializan en el arranque)
# ---------------------------------------------------------------
producer = None
cassandra_session = None
distance_stmt = None


def init_kafka_producer():
    global producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"[init] Kafka producer conectado a {KAFKA_BROKER}")


def init_cassandra():
    global cassandra_session, distance_stmt
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    cassandra_session = cluster.connect(CASSANDRA_KEYSPACE)
    # Sentencia preparada para leer distancias (punto obl. 2)
    distance_stmt = cassandra_session.prepare(
        "SELECT distance FROM origin_dest_distances WHERE origin=? AND dest=?"
    )
    print(f"[init] Cassandra conectada a {CASSANDRA_HOST}:{CASSANDRA_PORT}")


def get_flight_distance(origin, dest):
    """Lee la distancia origen-destino desde CASSANDRA (antes era Mongo)."""
    row = cassandra_session.execute(distance_stmt, (origin, dest)).one()
    return row.distance if row else 0.0


def get_date_features(iso_date):
    """Deriva DayOfYear, DayOfMonth, DayOfWeek de una fecha ISO."""
    dt = iso8601.parse_date(iso_date)
    return {
        "DayOfYear": dt.timetuple().tm_yday,
        "DayOfMonth": dt.day,
        "DayOfWeek": dt.weekday(),
    }


# ---------------------------------------------------------------
# Consumer Kafka en background -> emite por WebSocket (punto obl. 3)
# ---------------------------------------------------------------
def kafka_response_consumer():
    """Escucha el topic de respuestas y emite cada prediccion por WebSocket."""
    consumer = KafkaConsumer(
        RESPONSE_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="flask-websocket-emitter",
    )
    print(f"[consumer] Escuchando {RESPONSE_TOPIC} en {KAFKA_BROKER}")
    for message in consumer:
        prediction = message.value
        print(f"[consumer] Prediccion recibida: {prediction}")
        # Emitir a TODOS los clientes conectados; el cliente filtra por su UUID
        socketio.emit("prediction", prediction)


# ---------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------
@app.route("/")
def index():
    form_config = [
        {"field": "DepDelay", "label": "Departure Delay", "value": 5},
        {"field": "Carrier", "label": "Carrier", "value": "AA"},
        {"field": "FlightDate", "label": "Date", "value": "2016-12-25"},
        {"field": "Origin", "label": "Origin", "value": "ATL"},
        {"field": "Dest", "label": "Destination", "value": "SFO"},
    ]
    return render_template("predict.html", form_config=form_config)


@app.route("/flights/delays/predict/classify_realtime", methods=["POST"])
def classify_realtime():
    """Recibe el formulario, calcula features y produce a Kafka."""
    # 1. Leer campos del formulario
    field_types = {
        "DepDelay": float, "Carrier": str, "FlightDate": str,
        "Dest": str, "FlightNum": str, "Origin": str,
    }
    features = {}
    for name, typ in field_types.items():
        val = request.form.get(name, type=typ)
        if val is not None:
            features[name] = val

    # FlightNum puede no venir; lo ponemos por defecto
    features.setdefault("FlightNum", "0")

    # 2. Distancia desde CASSANDRA (punto obl. 2)
    features["Distance"] = get_flight_distance(features["Origin"], features["Dest"])

    # 3. Features de fecha
    features.update(get_date_features(features["FlightDate"]))

    # 4. Timestamp y UUID
    features["Timestamp"] = datetime.utcnow().isoformat()
    unique_id = str(uuid.uuid4())
    features["UUID"] = unique_id

    # 5. Producir a Kafka (topic de peticiones)
    producer.send(REQUEST_TOPIC, features)
    producer.flush()
    print(f"[producer] Peticion enviada UUID={unique_id}")

    return jsonify({"status": "OK", "id": unique_id})


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# ---------------------------------------------------------------
# Arranque
# ---------------------------------------------------------------
def start_background():
    init_kafka_producer()
    init_cassandra()
    # Lanzar el consumer Kafka en un hilo de fondo
    t = threading.Thread(target=kafka_response_consumer, daemon=True)
    t.start()


if __name__ == "__main__":
    start_background()
    socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
