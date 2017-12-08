name := "KafkaConsumer-DatafactZ"

version := "1.0"

scalaVersion := "2.10.4"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.2",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
  "org.apache.commons" % "commons-csv" % "1.1"
)

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.1"
libraryDependencies += "org.json" % "json" % "20160212"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "dibbhatt" % "kafka-spark-consumer" % "1.0.6"
