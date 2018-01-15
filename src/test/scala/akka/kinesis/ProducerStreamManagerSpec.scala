package akka.kinesis

import akka.actor.ActorSystem
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.testkit.{DefaultTimeout, ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.soujiro0725.producers.ProducerStreamManager
import com.soujiro0725.producers.ProducerStreamManager.InitializeProducerStream
import com.soujiro0725.shared.AkkaStreams
import com.soujiro0725.shared.EventMessages.ActivatedProducerStream
import com.soujiro0725.shared.KinesisMessages.{ExampleAppEvent, KinesisMessage}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}


class ProducerStreamManagerSpec extends TestKit(ActorSystem("ProducerStreamManagerSpec"))
    with DefaultTimeout
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with AkkaStreams {

  val testProducerStreamManager = TestActorRef(new ProducerStreamManager)
  val producerStreamManagerActor = testProducerStreamManager.underlyingActor

  //Create an test event listener for the local message bus
  val testEventListener = TestProbe()
  system.eventStream.subscribe(testEventListener.ref, classOf[ExampleAppEvent])

  override def afterAll: Unit = {
    shutdown()
  }


  "Sending InitializeProducerStream(self, KinesisMessage) to ProducerStreamManager" should {
    "initialize the stream for that particular message type, return ActivatedProducerStream(streaRef, \"TempChannel1\") and produce local event " in {
      testProducerStreamManager ! InitializeProducerStream(self, KinesisMessage)
      Thread.sleep(500)
      var streamRef: SourceQueueWithComplete[Any] = null
      expectMsgPF() {
        case ActivatedProducerStream(sr, kt) => if (kt == "TempChannel1") {
          streamRef = sr; ()
        } else fail()
      }

      Thread.sleep(500)
      val resultMessage = ActivatedProducerStream(streamRef, "TempChannel1")
      testEventListener.expectMsgPF() {
        case ExampleAppEvent(_, _, m) => if (m == resultMessage.toString) () else fail()
      }
    }
  }

  "Sending InitializeProducerStream(self, ExampleAppEvent) to ProducerStreamManager" should {
    "initialize the stream for that particular message type, return ActivatedProducerStream(streaRef, \"TempChannel2\") and produce local event " in {
      testProducerStreamManager ! InitializeProducerStream(self, ExampleAppEvent)
      Thread.sleep(500)
      var streamRef: SourceQueueWithComplete[Any] = null
      expectMsgPF() {
        case ActivatedProducerStream(sr, kt) => if (kt == "TempChannel2") {
          streamRef = sr; ()
        } else fail()
      }

      Thread.sleep(500)
      val resultMessage = ActivatedProducerStream(streamRef, "TempChannel2")
      testEventListener.expectMsgPF() {
        case ExampleAppEvent(_, _, m) => if (m == resultMessage.toString) () else fail()
      }
    }
  }
}
