package com.soujiro0725.producers

import akka.actor.{Actor, Props}
import akka.event.Logging
import akka.stream.scaladsl.SourceQueueWithComplete
import com.soujiro0725.shared.EventMessages.ActivatedProducerStream
import com.soujiro0725.shared.EventSourcing
import com.soujiro0725.shared.KinesisMessages.ExampleAppEvent


/**
  * This actor receives local app events called "ExampleAppEvent"s which are initially published to the
  * "internal" Akka Event Bus and which he is subscribed to. He then publishes the event messages to the Kinesis
  * topic called AppEventChannel. The idea would be that another microservice is subscribed to
  * the AppEventChannel topic and can then react to events this microservice emits.
  */

object EventProducer {

  def props: Props = Props(new EventProducer)
}

class EventProducer extends Actor with EventSourcing {

  import context._

  implicit val system = context.system
  val log = Logging(system, this.getClass.getName)

  var producerStream: SourceQueueWithComplete[Any] = null
  val subscribedMessageTypes = Seq(classOf[ExampleAppEvent])

  override def preStart(): Unit = {
    super.preStart()
    subscribedMessageTypes.foreach(system.eventStream.subscribe(self, _))
  }

  override def postStop(): Unit = {
    subscribedMessageTypes.foreach(system.eventStream.unsubscribe(self, _))
    super.postStop()
  }

  def receive: Receive = {
    case ActivatedProducerStream(streamRef, _) =>
      producerStream = streamRef
      become(publishEvent)

    case msg: ExampleAppEvent => if (producerStream == null) self ! msg else producerStream.offer(msg)
    case other => log.error("EventProducer got the unknown message while in idle: " + other)
  }

  def publishEvent: Receive = {
    case msg: ExampleAppEvent => producerStream.offer(msg)
    case other => log.error("EventProducer got the unknown message while producing: " + other)
  }
}
