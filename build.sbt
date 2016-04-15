organization := "com.cloudera"

name := "kafka-consumer-groups-maker"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "0.8.2.2" % "provided"
      exclude("javax.jms", "jms")
      exclude("com.sun.jdmk", "jmxtools")
      exclude("com.sun.jmx", "jmxri")
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
