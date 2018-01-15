package com.soujiro0725.consumers

import akka.actor._
import akka.event.Logging
import akka.kinesis.scaladsl.Consumer.Control
import com.soujiro0725.consumers.ConsumerStreamManager.{InitializeConsumerStream, TerminateConsumerStream}
import com.soujiro0725.settings.Settings
import com.soujiro0725.shared.EventMessages.{ActivatedConsumerStream, TerminatedConsumerStream}
import com.soujiro0725.shared.JsonMessageConversion.Conversion
import com.soujiro0725.shared.KinesisMessages.{ExampleAppEvent, KinesisMessage}

import scala.collection.mutable


/**
  * This actor is responsible for creating and terminating the consuming akka-kinesis streams.
  * Upon receiving an InitializeConsumerStream message with a corresponding message type
  * (KinesisMessage or ExampleAppEvent) and consumer sink actor reference, this manager initializes the stream,
  * sends an ActivatedConsumerStream message to the sink actor and finally publishes a local event to the
  * Akka Event Bus.
  *
  * When this actor receives a TerminateConsumerStream message along with an associated kinesis topic, he gets
  * the corresponding stream reference from its activeConsumerStreams collection and then shuts down the stream.
  */

object ConsumerStreamManager {

  //Command Messages
  case class InitializeConsumerStream(consumerActorRef: ActorRef, msgType: Any)

  case class TerminateConsumerStream(kinesisTopic: String)

  def props: Props = Props(new ConsumerStreamManager)

}

class ConsumerStreamManager extends Actor with ConsumerStream {
  implicit val system = context.system
  val log = Logging(system, this.getClass.getName)

  //Once the stream is created, we store its reference and associated kinesis topic so we can shut it down on command
  var activeConsumerStreams: mutable.Map[String, Control] = mutable.Map()

  //Get Kinesis Consumer Config Settings
  val settings = Settings(system).KinesisConsumers

  def receive: Receive = {
    case InitializeConsumerStream(consumerActorRef, KinesisMessage) =>

      //Get consumer properties corresponding to that which subscribes to message type KinesisMessage
      val consumerProperties = settings.KinesisConsumerInfo("KinesisMessage")
      startConsumerStream[KinesisMessage](consumerActorRef, consumerProperties)

    case InitializeConsumerStream(consumerActorRef, ExampleAppEvent) =>

      //Get consumer properties corresponding to that which subscribes to the message type ExampleAppEvent
      val consumerProperties = settings.KinesisConsumerInfo("ExampleAppEvent")
      startConsumerStream[ExampleAppEvent](consumerActorRef, consumerProperties)

    case TerminateConsumerStream(kinesisTopic) => terminateConsumerStream(sender, kinesisTopic)

    case other => log.error(s"Consumer Stream Manager got unknown message: $other")
  }


  def startConsumerStream[msgType: Conversion](consumerActorSink: ActorRef, consumerProperties: Map[String, String]) = {
    val streamSource = createStreamSource(consumerProperties)
    val streamFlow = createStreamFlow[msgType]
    val streamSink = createStreamSink(consumerActorSink)
    val consumerStream = streamSource.via(streamFlow).to(streamSink).run()

    //Add the active consumer stream reference and topic to the active stream collection
    val kinesisTopic = consumerProperties("subscription-topic")
    activeConsumerStreams += kinesisTopic -> consumerStream

    //Tell the consumer actor sink the stream has been started for the kinesis topic and publish the event
    consumerActorSink ! ActivatedConsumerStream(kinesisTopic)
    publishLocalEvent(ActivatedConsumerStream(kinesisTopic))
  }


  def terminateConsumerStream(consumerActorSink: ActorRef, kinesisTopic: String) = {
    try {
      println(s"ConsumerStreamManager got TerminateStream command for topic: $kinesisTopic. Terminating stream...")
      val stream = activeConsumerStreams(kinesisTopic)
      val stopped = stream.stop

      stopped.onComplete {
        case _ =>
          stream.shutdown()

          //Remove the topic name from activeConsumerStreams collection
          activeConsumerStreams -= kinesisTopic

          //Publish an app event that the stream was killed. The stream will send an onComplete message to the Sink
          publishLocalEvent(TerminatedConsumerStream(kinesisTopic))
          println(s"Terminated stream for topic: $kinesisTopic.")
      }
    }
    catch {
      case e: NoSuchElementException =>
        consumerActorSink ! "STREAM_DONE"
        log.info(s"Stream Consumer in consuming mode but no stream to consume from: ($consumerActorSink,$kinesisTopic)")
      case e: Exception => log.error(s"Exception during manual termination of the Consumer Stream for topic $kinesisTopic : $e")
    }
  }
}
