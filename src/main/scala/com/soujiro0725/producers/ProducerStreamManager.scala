package com.soujiro0725.producers

import akka.actor._
import com.soujiro0725.producers.ProducerStreamManager.InitializeProducerStream
import com.soujiro0725.settings.Settings
import com.soujiro0725.shared.EventMessages.ActivatedProducerStream
import com.soujiro0725.shared.JsonMessageConversion.Conversion
import com.soujiro0725.shared.KinesisMessages.{ExampleAppEvent, KinesisMessage}

/**
  * This actor is responsible for creating and terminating the publishing akka-kinesis streams.
  * Upon receiving an InitializeProducerStream message with a corresponding message type
  * (KinesisMessage or ExampleAppEvent) and producer source actor reference, this manager initializes the stream,
  * sends an ActivatedProducerStream message to the source actor and finally publishes a local event to the
  * Akka Event Bus.
  */

object ProducerStreamManager {

  //CommandMessage
  case class InitializeProducerStream(producerActorRef: ActorRef, msgType: Any)

  def props: Props = Props(new ProducerStreamManager)
}

class ProducerStreamManager extends Actor with ProducerStream {
  implicit val system = context.system

  //Get Kinesis Producer Config Settings
  val settings = Settings(system).KinesisProducers

  //Edit this receive method with any new Streamed message types
  def receive: Receive = {
    case InitializeProducerStream(producerActorRef, KinesisMessage) => {

      //Get producer properties
      val producerProperties = settings.KinesisProducerInfo("KinesisMessage")
      startProducerStream[KinesisMessage](producerActorRef, producerProperties)
    }
    case InitializeProducerStream(producerActorRef, ExampleAppEvent) => {

      //Get producer properties
      val producerProperties = settings.KinesisProducerInfo("ExampleAppEvent")
      startProducerStream[ExampleAppEvent](producerActorRef, producerProperties)
    }
    case other => println(s"Producer Stream Manager got unknown message: $other")
  }


  def startProducerStream[msgType: Conversion](producerActorSource: ActorRef, producerProperties: Map[String, String]) = {
    val streamSource = createStreamSource[msgType]
    val streamFlow = createStreamFlow[msgType](producerProperties)
    val streamSink = createStreamSink(producerProperties)
    val producerStream = streamSource.via(streamFlow).to(streamSink).run()

    //Send the completed stream reference to the actor who wants to publish to it
    val kinesisTopic = producerProperties("publish-topic")
    producerActorSource ! ActivatedProducerStream(producerStream, kinesisTopic)
    publishLocalEvent(ActivatedProducerStream(producerStream, kinesisTopic))
  }
}
