name := "reactive-kafka-example"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-M2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"


libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.7.12"

libraryDependencies +="ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
