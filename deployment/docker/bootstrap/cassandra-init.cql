-- =============================================================
-- Schema Cassandra - Practica Big Data 2026 Parte II
-- Idempotente: usa IF NOT EXISTS en todo
-- =============================================================

-- Keyspace principal
-- SimpleStrategy + RF=1 porque es single-node (dev)
-- En produccion multi-DC usariamos NetworkTopologyStrategy
CREATE KEYSPACE IF NOT EXISTS flight_db
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
  };

USE flight_db;

-- -------------------------------------------------------------
-- Tabla 1: distancias origen-destino
-- Reemplaza la coleccion 'origin_dest_distances' de MongoDB
-- Consulta tipica: SELECT distance WHERE origin=? AND dest=?
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS origin_dest_distances (
    origin text,
    dest text,
    distance double,
    PRIMARY KEY ((origin, dest))
);

-- -------------------------------------------------------------
-- Tabla 2: predicciones de retraso
-- Almacena el resultado calculado por Spark Streaming
-- Consulta tipica: SELECT * WHERE uuid=?
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS flight_delay_predictions (
    uuid text,
    origin text,
    dest text,
    flight_date text,
    carrier text,
    dep_delay double,
    distance double,
    prediction int,
    created_at timestamp,
    PRIMARY KEY (uuid)
);

-- Verificacion: insertar una distancia de prueba
INSERT INTO origin_dest_distances (origin, dest, distance)
  VALUES ('ATL', 'SFO', 2139.0) IF NOT EXISTS;

-- Mostrar las tablas creadas
DESCRIBE TABLES;
