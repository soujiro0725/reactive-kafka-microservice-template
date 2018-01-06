package com.soujiro0725

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import com.soujiro0725.consumers.ConsumerStreamManager.InitializeConsumerStream
import com.soujiro0725.consumers.{ConsumerStreamManager, DataConsumer, EventConsumer}
import com.soujiro0725.http.HttpService
import com.soujiro0725.producers.ProducerStreamManager.InitializeProducerStream
import com.soujiro0725.producers.{DataProducer, EventProducer, ProducerStreamManager}
import com.soujiro0725.settings.Settings
import com.soujiro0725.shared.AkkaStreams
import com.soujiro0725.shared.KinesisMessages.{ExampleAppEvent, KinesisMessage}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.StdIn

/**
  * This starts the Reactive Kinesis Microservice Template
  */

object Main extends App with HttpService with AkkaStreams {
  implicit val system = ActorSystem("akka-reactive-kinesis-app")
  val log = Logging(system, this.getClass.getName)

  //Start the akka-http server and listen for http requests
  val akkaHttpServer = startAkkaHTTPServer()

  //Create the Producer Stream Manager and Consumer Stream Manager
  val producerStreamManager = system.actorOf(Props(new ProducerStreamManager), "producerStreamManager")
  val consumerStreamManager = system.actorOf(Props(new ConsumerStreamManager), "consumerStreamManager")

  //Create actor to publish event messages to kinesis stream.
  val eventProducer = system.actorOf(EventProducer.props, "eventProducer")
  producerStreamManager ! InitializeProducerStream(eventProducer, ExampleAppEvent)

  //Create actor to consume event messages from kinesis stream.
  val eventConsumer = system.actorOf(EventConsumer.props, "eventConsumer")
  consumerStreamManager ! InitializeConsumerStream(eventConsumer, ExampleAppEvent)

  //Create actor to publish data messages to kinesis stream.
  val dataProducer = system.actorOf(DataProducer.props, "dataProducer")
  producerStreamManager ! InitializeProducerStream(dataProducer, KinesisMessage)

  //Create actor to consume data messages from kinesis stream.
  val dataConsumer = system.actorOf(DataConsumer.props, "dataConsumer")
  consumerStreamManager ! InitializeConsumerStream(dataConsumer, KinesisMessage)

  //Shutdown
  shutdownApplication()

  private def startAkkaHTTPServer(): Future[ServerBinding] = {
    val settings = Settings(system).Http
    val host = settings.host

    println(s"Specify the TCP port do you want to host the HTTP server at (e.g. 8001, 8080..etc)? \nHit Return when finished:")
    val portNum = StdIn.readInt()

    println(s"Waiting for http requests at http://$host:$portNum/")
    Http().bindAndHandle(routes, host, portNum)
  }

  private def shutdownApplication(): Unit = {
    scala.sys.addShutdownHook({
      println("Terminating the Application...")
      akkaHttpServer.flatMap(_.unbind())
      system.terminate()
      Await.result(system.whenTerminated, 30 seconds)
      println("Application Terminated")
    })
  }
}



