package com.soujiro0725.consumers

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.soujiro0725.consumers.ConsumerStreamManager.{InitializeConsumerStream, TerminateConsumerStream}
import com.soujiro0725.consumers.DataConsumer.{ConsumerActorReply, ManuallyInitializeStream, ManuallyTerminateStream}
import com.soujiro0725.settings.Settings
import com.soujiro0725.shared.EventMessages.ActivatedConsumerStream
import com.soujiro0725.shared.EventSourcing
import com.soujiro0725.shared.KinesisMessages.ExampleAppEvent

import scala.collection.mutable.ArrayBuffer


/**
  * This actor serves as a Sink for the kinesis stream that is created by the ConsumerStreamManager.
  * The corresponding stream converts the json from the kinesis topic AppEventChannel to the message type ExampleAppEvent.
  * Once this actor receives a batch of such messages he prints them out.
  *
  * This actor can be started and stopped manually from the HTTP interface, and in doing so, changes between receiving
  * states.
  */

object EventConsumer {

  def props: Props = Props(new EventConsumer)
}

class EventConsumer extends Actor with EventSourcing {
  implicit val system = context.system
  val log = Logging(system, this.getClass.getName)

  //Once stream is started by manager, we save the actor ref of the manager
  var consumerStreamManager: ActorRef = null

  //Get Kinesis Topic
  val kinesisTopic = Settings(system).KinesisConsumers.KinesisConsumerInfo("ExampleAppEvent")("subscription-topic")

  def receive: Receive = {
    case InitializeConsumerStream(_, ExampleAppEvent) =>
      consumerStreamManager ! InitializeConsumerStream(self, ExampleAppEvent)

    case ActivatedConsumerStream(_) => consumerStreamManager = sender()

    case "STREAM_INIT" =>
      sender() ! "OK"
      println("Event Consumer entered consuming state!")
      context.become(consumingEvents)

    case ManuallyTerminateStream => sender() ! ConsumerActorReply("Event Consumer Stream Already Stopped")

    case ManuallyInitializeStream =>
      consumerStreamManager ! InitializeConsumerStream(self, ExampleAppEvent)
      sender() ! ConsumerActorReply("Event Consumer Stream Started")

    case other => log.error("Event Consumer got unknown message while in idle:" + other)
  }

  def consumingEvents: Receive = {
    case ActivatedConsumerStream(_) => consumerStreamManager = sender()

    case consumerMessageBatch: ArrayBuffer[_] =>
      sender() ! "OK"
      consumerMessageBatch.foreach(println)

    case "STREAM_DONE" =>
      context.become(receive)

    case ManuallyInitializeStream => sender() ! ConsumerActorReply("Event Consumer Already Started")

    case ManuallyTerminateStream =>
      consumerStreamManager ! TerminateConsumerStream(kinesisTopic)
      sender() ! ConsumerActorReply("Event Consumer Stream Stopped")

    case other => log.error("Event Consumer got unknown message while in consuming: " + other)
  }
}

