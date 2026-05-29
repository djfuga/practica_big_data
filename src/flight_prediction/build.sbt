name := "flight_prediction"
version := "0.2"
scalaVersion := "2.13.16"

val sparkVersion = "4.1.1"

Compile / mainClass := Some("es.upm.dit.ging.predictor.MakePrediction")

resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  // Spark (provided: vienen de la imagen Spark al hacer spark-submit)
  "org.apache.spark" %% "spark-core"           % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib"          % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming"      % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",

  // Cassandra Java driver (tambien viene en la imagen Spark)
  "com.datastax.oss" % "java-driver-core" % "4.17.0" % "provided"
)

// Assembly: como solo tenemos codigo propio (todo lo demas es 'provided'),
// el JAR final sera ligero (~KB) en lugar de un fat-jar (~MB)
assembly / assemblyJarName := "flight_prediction.jar"

// Estrategia de merge por si surgen conflictos
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _                            => MergeStrategy.first
}
