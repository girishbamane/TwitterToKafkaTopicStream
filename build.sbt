name := "TwitterToKafkaTopicStream"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.1"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.7"
resolvers += " CakeSolutions" at "https://dl.bintray.com/cakesolutions/maven/"
libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "1.0.0"

assemblyJarName in assembly := s"TwitterTokafkaTopicStream_assembly_2.3.1_${scalaVersion.value}.jar"
mainClass in assembly := Some("kafka.twitter.flow.TwitterToKafkaStream")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}