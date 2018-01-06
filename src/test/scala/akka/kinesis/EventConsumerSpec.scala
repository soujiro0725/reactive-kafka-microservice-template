package akka.kinesis

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestActorRef, TestKit}
import com.soujiro0725.consumers.ConsumerStreamManager.{InitializeConsumerStream, TerminateConsumerStream}
import com.soujiro0725.consumers.DataConsumer.{ConsumerActorReply, ManuallyInitializeStream, ManuallyTerminateStream}
import com.soujiro0725.consumers.EventConsumer
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.mutable.ArrayBuffer


class EventConsumerSpec extends TestKit(ActorSystem("EventConsumerSpec"))
  with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  //Creating the Actors
  val testConsumer = TestActorRef(new EventConsumer)
  val mockStreamAndManager = system.actorOf(Props(new MockStreamAndManager), "mockStreamAndManager")

  override def afterAll: Unit = {
    shutdown()
  }

  class MockStreamAndManager extends Actor {
    val receive: Receive = {
      case InitializeConsumerStream(_, _) => testConsumer ! "STREAM_INIT"
      case TerminateConsumerStream(_) => testConsumer ! "STREAM_DONE"
    }
  }


  "Sending ManuallyTerminateStream to EventConsumer in receive state" should {
    "return a Stream Already Stopped reply " in {
      testConsumer ! ManuallyTerminateStream
      expectMsg(ConsumerActorReply("Event Consumer Stream Already Stopped"))
    }
  }

  "Sending ManuallyInitializeStream to EventConsumer in receive state" should {
    "forward the message to the ConsumerStreamManager and change state to consuming" in {
      testConsumer.underlyingActor.consumerStreamManager = mockStreamAndManager
      testConsumer ! ManuallyInitializeStream
      expectMsg(ConsumerActorReply("Event Consumer Stream Started"))
      //Now check for state change
      Thread.sleep(750)
      testConsumer ! ManuallyInitializeStream
      expectMsg(ConsumerActorReply("Event Consumer Already Started"))
    }
  }

  "Sending STREAM_DONE to EventConsumer while in consuming state" should {
    "change state to idle state" in {
      val consuming = testConsumer.underlyingActor.consumingEvents
      testConsumer.underlyingActor.context.become(consuming)
      testConsumer ! "STREAM_DONE"
      //Now check for state change
      Thread.sleep(750)
      testConsumer ! ManuallyTerminateStream
      expectMsg(ConsumerActorReply("Event Consumer Stream Already Stopped"))
    }
  }
  "Sending ManuallyTerminateStream to EventConsumer while in consuming state" should {
    "forward the message to the ConsumerStreamManager and then upon reply, change state to idle" in {
      val consuming = testConsumer.underlyingActor.consumingEvents
      testConsumer.underlyingActor.context.become(consuming)
      testConsumer ! ManuallyTerminateStream
      expectMsg(ConsumerActorReply("Event Consumer Stream Stopped"))
      //Now check for state change
      Thread.sleep(750)
      testConsumer ! ManuallyTerminateStream
      expectMsg(ConsumerActorReply("Event Consumer Stream Already Stopped"))
    }
  }

  "Sending ConsumerMessageBatch message" should {
    "reply OK" in {
      val msgBatch: ArrayBuffer[String] = ArrayBuffer("test1")
      val consuming = testConsumer.underlyingActor.consumingEvents
      testConsumer.underlyingActor.context.become(consuming)
      testConsumer.underlyingActor.consumerStreamManager = mockStreamAndManager
      testConsumer ! msgBatch
      expectMsg("OK")
    }
  }
}
