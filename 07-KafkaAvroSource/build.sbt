name := "KafkaAvroSource"
organization := "spark.streaming"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion
)

libraryDependencies ++= sparkDependencies
