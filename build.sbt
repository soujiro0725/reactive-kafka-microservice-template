import Dependencies._

name := "reactive-kafka-microservice-template"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  akka_http_core,
  akka_http_testkit,
  akka_testkit,
  akka_slf4j,
  logback,
  log4j_over_slf4j,
  io_spray,
  play_json,
  scalatest,
  kinesis
)

//Run tests Sequentially
parallelExecution in Test := false
