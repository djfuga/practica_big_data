#!/bin/sh
# =============================================================
# Bootstrap de Kafka: crea los topics de la practica
# Idempotente: si los topics existen, no falla
# =============================================================

set -e

BOOTSTRAP="kafka:9092"
TOPICS_DIR="/opt/kafka/bin"

echo "==> Esperando a que Kafka responda en $BOOTSTRAP..."
# Healthcheck del compose ya garantiza esto, pero pongo retry por seguridad
for i in 1 2 3 4 5; do
  if "$TOPICS_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; then
    echo "    Kafka OK"
    break
  fi
  echo "    intento $i/5..."
  sleep 3
done

# --- Funcion auxiliar: crea topic si no existe ---
create_topic() {
  local TOPIC=$1
  local PARTITIONS=$2
  local RF=$3

  if "$TOPICS_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC"; then
    echo "    [skip] '$TOPIC' ya existe"
  else
    "$TOPICS_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
      --create \
      --topic "$TOPIC" \
      --partitions "$PARTITIONS" \
      --replication-factor "$RF"
    echo "    [+]    '$TOPIC' creado ($PARTITIONS particiones, RF=$RF)"
  fi
}

echo "==> Creando topics de la practica..."
# Flask -> Spark : peticiones de prediccion
create_topic "flight-delay-request" 3 1

# Spark -> Flask : resultados de prediccion (NUEVO Parte II)
create_topic "flight-delay-classification-response" 3 1

echo ""
echo "==> Topics existentes en el cluster:"
"$TOPICS_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "==> Detalle de los topics de la practica:"
"$TOPICS_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic flight-delay-request
"$TOPICS_DIR/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic flight-delay-classification-response

echo ""
echo "===================================================="
echo "  Kafka bootstrap completado"
echo "  - Bootstrap server interno:  kafka:9092"
echo "  - Bootstrap server externo:  localhost:9094"
echo "  - Topics creados:"
echo "      * flight-delay-request"
echo "      * flight-delay-classification-response"
echo "===================================================="
