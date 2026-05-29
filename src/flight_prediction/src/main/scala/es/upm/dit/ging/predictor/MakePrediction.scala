package es.upm.dit.ging.predictor

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import java.net.InetSocketAddress
import java.time.Instant

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{col, concat, from_json, lit, struct, to_json}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/**
 * Job de prediccion de retrasos de vuelos (Practica Big Data 2026 - Parte II).
 *
 * Flujo:
 *  1. Consume peticiones de Kafka topic 'flight-delay-request'.
 *  2. Aplica el pipeline ML (bucketizer + 4 string indexers + vector assembler + RF).
 *  3. Escribe la prediccion a:
 *      - Kafka topic 'flight-delay-classification-response' (Flask lo emite por WebSocket).
 *      - Cassandra tabla flight_db.flight_delay_predictions (persistencia / auditoria).
 *
 * Configuracion via env vars (con defaults para docker-compose):
 *  - KAFKA_BROKER (default: kafka:9092)
 *  - REQUEST_TOPIC (default: flight-delay-request)
 *  - RESPONSE_TOPIC (default: flight-delay-classification-response)
 *  - CASSANDRA_HOST (default: cassandra)
 *  - CASSANDRA_PORT (default: 9042)
 *  - CASSANDRA_KEYSPACE (default: flight_db)
 *  - MODELS_BASE_PATH (default: /opt/spark/models)
 */
object MakePrediction {

  // ---------------------------------------------------------------
  // Configuracion (env vars con defaults)
  // ---------------------------------------------------------------
  private val kafkaBroker = sys.env.getOrElse("KAFKA_BROKER", "kafka:9092")
  private val requestTopic = sys.env.getOrElse("REQUEST_TOPIC", "flight-delay-request")
  private val responseTopic = sys.env.getOrElse("RESPONSE_TOPIC", "flight-delay-classification-response")
  private val cassandraHost = sys.env.getOrElse("CASSANDRA_HOST", "cassandra")
  private val cassandraPort = sys.env.getOrElse("CASSANDRA_PORT", "9042").toInt
  private val cassandraKeyspace = sys.env.getOrElse("CASSANDRA_KEYSPACE", "flight_db")
  private val modelsBasePath = sys.env.getOrElse("MODELS_BASE_PATH", "/opt/spark/models")
  private val checkpointDir = sys.env.getOrElse("CHECKPOINT_DIR", "/tmp/spark-checkpoints")

  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Flight predictor starting...")
    println(s"  Kafka broker:    $kafkaBroker")
    println(s"  Request topic:   $requestTopic")
    println(s"  Response topic:  $responseTopic")
    println(s"  Cassandra:       $cassandraHost:$cassandraPort/$cassandraKeyspace")
    println(s"  Models path:     $modelsBasePath")
    println("=" * 60)

    val spark = SparkSession.builder
      .appName("FlightDelayStreamingPredictor")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ---------------------------------------------------------------
    // 1. Cargar todos los modelos del pipeline ML
    //    (entrenados en train_spark_mllib_model.py, guardados en MODELS)
    // ---------------------------------------------------------------
    val arrivalBucketizer = Bucketizer.load(s"$modelsBasePath/arrival_bucketizer_2.0.bin")

    val columns = Seq("Carrier", "Origin", "Dest", "Route")
    val stringIndexers: Map[String, StringIndexerModel] = columns.map { col =>
      col -> StringIndexerModel.load(s"$modelsBasePath/string_indexer_model_$col.bin")
    }.toMap

    val vectorAssembler = VectorAssembler.load(s"$modelsBasePath/numeric_vector_assembler.bin")
    val rfc = RandomForestClassificationModel.load(s"$modelsBasePath/spark_random_forest_classifier.flight_delays.5.0.bin")

    println(">>> Modelos cargados correctamente")

    // ---------------------------------------------------------------
    // 2. Leer stream de peticiones desde Kafka
    // ---------------------------------------------------------------
    val rawKafkaDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", requestTopic)
      .option("startingOffsets", "latest")
      .load()

    // El payload que envia Flask
    val requestSchema = new StructType()
      .add("UUID", DataTypes.StringType)
      .add("Carrier", DataTypes.StringType)
      .add("Origin", DataTypes.StringType)
      .add("Dest", DataTypes.StringType)
      .add("FlightDate", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Distance", DataTypes.DoubleType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("Timestamp", DataTypes.StringType)

    val requestsDf = rawKafkaDf
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", requestSchema).as("data"))
      .select("data.*")
      .withColumn("Route", concat(col("Origin"), lit("-"), col("Dest")))

    // ---------------------------------------------------------------
    // 3. Aplicar el pipeline ML
    // ---------------------------------------------------------------
    // Aplicar StringIndexers (con handleInvalid=keep por si llegan valores nuevos)
    val withCarrierIdx = stringIndexers("Carrier").setHandleInvalid("keep").transform(requestsDf)
    val withOriginIdx = stringIndexers("Origin").setHandleInvalid("keep").transform(withCarrierIdx)
    val withDestIdx = stringIndexers("Dest").setHandleInvalid("keep").transform(withOriginIdx)
    val withRouteIdx = stringIndexers("Route").setHandleInvalid("keep").transform(withDestIdx)

    // VectorAssembler espera columnas: DepDelay, Distance, DayOfMonth, DayOfWeek, DayOfYear,
    //                                  Carrier_index, Origin_index, Dest_index, Route_index
    val withFeatures = vectorAssembler.setHandleInvalid("keep").transform(withRouteIdx)

    // RandomForest -> columna 'Prediction'
    val withPrediction = rfc.transform(withFeatures)

    // Quedarnos solo con columnas relevantes para la respuesta
    val responseDf = withPrediction.select(
      col("UUID"),
      col("Carrier"),
      col("Origin"),
      col("Dest"),
      col("FlightDate"),
      col("DepDelay"),
      col("Distance"),
      col("Prediction").cast(DataTypes.IntegerType).as("Prediction"),
      col("Timestamp")
    )

    // ---------------------------------------------------------------
    // 4. Sink A: Kafka response (Flask lo emitira por WebSocket)
    // ---------------------------------------------------------------
    val kafkaSinkDf = responseDf.select(
      col("UUID").as("key"),
      to_json(struct(col("*"))).as("value")
    )

    val kafkaQuery = kafkaSinkDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", responseTopic)
      .option("checkpointLocation", s"$checkpointDir/kafka")
      .outputMode("append")
      .start()

    println(s">>> Sink Kafka activo -> topic=$responseTopic")

    // ---------------------------------------------------------------
    // 5. Sink B: Cassandra (foreachBatch + driver Java nativo)
    //    Usamos driver Java en vez de spark-cassandra-connector porque
    //    este ultimo no es compatible con Spark 4.x.
    // ---------------------------------------------------------------
    val cassandraQuery = responseDf.writeStream
      .foreachBatch { (batchDf: Dataset[Row], batchId: Long) =>
        // Coalesce a 1 particion para no abrir 200 sesiones Cassandra
        val rows = batchDf.coalesce(1).collect()
        if (rows.nonEmpty) {
          val session = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(cassandraHost, cassandraPort))
            .withLocalDatacenter("datacenter1")
            .withKeyspace(cassandraKeyspace)
            .build()
          try {
            val ps: PreparedStatement = session.prepare(
              """INSERT INTO flight_delay_predictions
                |(uuid, origin, dest, flight_date, carrier, dep_delay, distance, prediction, created_at)
                |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin
            )
            rows.foreach { row =>
              val bound = ps.bind(
                row.getAs[String]("UUID"),
                row.getAs[String]("Origin"),
                row.getAs[String]("Dest"),
                row.getAs[String]("FlightDate"),
                row.getAs[String]("Carrier"),
                java.lang.Double.valueOf(row.getAs[Double]("DepDelay")),
                java.lang.Double.valueOf(row.getAs[Double]("Distance")),
                java.lang.Integer.valueOf(row.getAs[Int]("Prediction")),
                Instant.now()
              )
              session.execute(bound)
            }
            println(s">>> Batch $batchId: ${rows.length} predicciones persistidas en Cassandra")
          } finally {
            session.close()
          }
        }
      }
      .option("checkpointLocation", s"$checkpointDir/cassandra")
      .outputMode("append")
      .start()

    println(s">>> Sink Cassandra activo -> $cassandraKeyspace.flight_delay_predictions")

    // ---------------------------------------------------------------
    // 6. Esperar a que ambos sinks terminen (en streaming, no terminan)
    // ---------------------------------------------------------------
    spark.streams.awaitAnyTermination()
  }
}
