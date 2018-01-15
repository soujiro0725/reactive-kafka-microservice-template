package com.soujiro0725.consumers

import akka.actor._
import akka.event.Logging
import com.soujiro0725.consumers.ConsumerStreamManager.{InitializeConsumerStream, TerminateConsumerStream}
import com.soujiro0725.consumers.DataConsumer.{ConsumerActorReply, ManuallyInitializeStream, ManuallyTerminateStream}
import com.soujiro0725.settings.Settings
import com.soujiro0725.shared.EventMessages.ActivatedConsumerStream
import com.soujiro0725.shared.EventSourcing
import com.soujiro0725.shared.KinesisMessages.KinesisMessage

import scala.collection.mutable.ArrayBuffer


/**
  * This actor serves as a Sink for the kinesis stream that is created by the ConsumerStreamManager.
  * The corresponding stream converts the json from the kinesis topic TestDataChannel to the message type KinesisMessage.
  * Once this actor receives a batch of such messages he prints them out.
  *
  * This actor can be started and stopped manually from the HTTP interface, and in doing so, changes between receiving
  * states.
  */

object DataConsumer {

  //Command Messages
  case object ManuallyInitializeStream

  case object ManuallyTerminateStream

  //Document Messages
  case class ConsumerActorReply(message: String)

  def props: Props = Props(new DataConsumer)
}

class DataConsumer extends Actor with EventSourcing {
  implicit val system = context.system
  val log = Logging(system, this.getClass.getName)

  //Once stream is started by manager, we save the actor ref of the manager
  var consumerStreamManager: ActorRef = null

  //Get Kinesis Topic
  val kinesisTopic = Settings(system).KinesisConsumers.KinesisConsumerInfo("KinesisMessage")("subscription-topic")

  def receive: Receive = {

    case ActivatedConsumerStream(_) => consumerStreamManager = sender()

    case "STREAM_INIT" =>
      sender() ! "OK"
      println("Data Consumer entered consuming state!")
      context.become(consumingData)

    case ManuallyTerminateStream => sender() ! ConsumerActorReply("Data Consumer Stream Already Stopped")

    case ManuallyInitializeStream =>
      consumerStreamManager ! InitializeConsumerStream(self, KinesisMessage)
      sender() ! ConsumerActorReply("Data Consumer Stream Started")

    case other => log.error("Data Consumer got unknown message while in idle:" + other)
  }

  def consumingData: Receive = {
    case ActivatedConsumerStream(_) => consumerStreamManager = sender()

    case consumerMessageBatch: ArrayBuffer[_] =>
      sender() ! "OK"
      consumerMessageBatch.foreach(println)

    case "STREAM_DONE" =>
      context.become(receive)

    case ManuallyInitializeStream => sender() ! ConsumerActorReply("Data Consumer Already Started")

    case ManuallyTerminateStream =>
      consumerStreamManager ! TerminateConsumerStream(kinesisTopic)
      sender() ! ConsumerActorReply("Data Consumer Stream Stopped")

    case other => log.error("Data Consumer got Unknown message while in consuming " + other)
  }
}
