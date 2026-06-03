# =============================================================
# Imagen Spark 4.1.1 + Scala 2.13 + conectores (DEFINITIVA v2)
# =============================================================
FROM apache/spark:4.1.1-scala2.13-java17-python3-ubuntu

USER root

# 1. Utilidades + Python deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir --upgrade pip && \
    python3 -m pip install --no-cache-dir \
    numpy==2.1.3 \
    boto3==1.35.99 \
    iso8601==2.1.0 \
    mlflow==2.18.0

# 2. Descarga de JARs desde Maven Central
ENV MVN=https://repo1.maven.org/maven2
ENV JARS=/opt/spark/jars

# --- Iceberg runtime + AWS bundle (SDK v2 completo) ---
RUN curl -fSL "${MVN}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar" -o "${JARS}/iceberg-spark-runtime-4.0_2.13-1.10.1.jar" && \
    curl -fSL "${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar" -o "${JARS}/iceberg-aws-bundle-1.10.1.jar"


# --- Spark Kafka connector + transitivas ---
RUN curl -fSL "${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar" -o "${JARS}/spark-sql-kafka-0-10_2.13-4.1.1.jar" && \
    curl -fSL "${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.1/spark-token-provider-kafka-0-10_2.13-4.1.1.jar" -o "${JARS}/spark-token-provider-kafka-0-10_2.13-4.1.1.jar" && \
    curl -fSL "${MVN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar" -o "${JARS}/kafka-clients-3.9.1.jar" && \
    curl -fSL "${MVN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar" -o "${JARS}/commons-pool2-2.12.0.jar"

# --- Cassandra Java driver + dependencias minimas ---
RUN curl -fSL "${MVN}/com/datastax/oss/java-driver-core-shaded/4.17.0/java-driver-core-shaded-4.17.0.jar" -o "${JARS}/java-driver-core-shaded-4.17.0.jar" && \
    curl -fSL "${MVN}/com/datastax/oss/java-driver-query-builder/4.17.0/java-driver-query-builder-4.17.0.jar" -o "${JARS}/java-driver-query-builder-4.17.0.jar" && \
    curl -fSL "${MVN}/com/datastax/oss/java-driver-shaded-guava/25.1-jre-graal-sub-1/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar" -o "${JARS}/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar" && \
    curl -fSL "${MVN}/com/datastax/oss/native-protocol/1.5.1/native-protocol-1.5.1.jar" -o "${JARS}/native-protocol-1.5.1.jar" && \
    curl -fSL "${MVN}/com/typesafe/config/1.4.3/config-1.4.3.jar" -o "${JARS}/config-1.4.3.jar"

# --- Verificacion ---
RUN echo "=== Conectores instalados ===" && \
    echo "=== OK ==="

# 3. Configuracion
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# 3.2 Scripts PySpark y tests (necesarios para K8s, donde no hay bind mounts)
COPY jobs/  /opt/spark/jobs/
COPY tests/ /opt/spark/tests/

# 3.3 Dataset training empotrado (necesario para K8s - sin bind mounts)
COPY simple_flight_delay_features.jsonl.bz2 /opt/spark/data/simple_flight_delay_features.jsonl.bz2

# 3.4 Modelos ML entrenados empotrados (MakePrediction.jar los lee de /opt/spark/models/)
COPY models_to_embed/ /opt/spark/models/

# 4. Usuario no-root
USER spark
WORKDIR /opt/spark
