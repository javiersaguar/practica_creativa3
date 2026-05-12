name := "flight_prediction"
version := "0.1"
assembly / assemblyJarName := "flight_prediction_2.13-0.1.jar"
scalaVersion := "2.13.16"
val sparkVersion = "4.1.1"
Compile / mainClass := Some("es.upm.dit.ging.predictor.MakePrediction")
resolvers ++= Seq(
  "apache-snapshots" at "https://repository.apache.org/snapshots/"
)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.1" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.1" % "provided"
)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
  case PathList("META-INF", "native-image", _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}
