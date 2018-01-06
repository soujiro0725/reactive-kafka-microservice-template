package akka.kinesis

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit._
import com.soujiro0725.consumers.ConsumerStreamManager.InitializeConsumerStream
import com.soujiro0725.consumers.{ConsumerStream, ConsumerStreamManager}
import com.soujiro0725.producers.ProducerStream
import com.soujiro0725.settings.Settings
import com.soujiro0725.shared.JsonMessageConversion.Conversion
import com.soujiro0725.shared.KinesisMessages.{ExampleAppEvent, KinesisMessage}
//import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._


class ConsumerStreamSpec extends TestKit(ActorSystem("ConsumerStreamSpec"))
  with DefaultTimeout with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll
  with ConsumerStream with ProducerStream {

  //Create an test event listener for the local message bus
  val testEventListener = TestProbe()
  system.eventStream.subscribe(testEventListener.ref, classOf[ExampleAppEvent])

  //Stuff for testing the stream
  val consumerSettings = Settings(system).KafkaConsumers
  val producerSettings = Settings(system).KafkaProducers
  val testConsumerStreamManager = TestActorRef(new ConsumerStreamManager)
  val probe = TestProbe()

  override def afterAll: Unit = {
    shutdown()
  }


  "Consuming KinesisMessages in JSON from from Kinesis" should {
    "be converted to KinesisMessages and all of them then obtained by the Stream Sink " in {

      //Creating KafkaMessage Consumer Stream Components
      val consumerProps = consumerSettings.KafkaConsumerInfo("KafkaMessage")
      val consumerSource = createStreamSource(consumerProps)
      val consumerFlow = createStreamFlow[KafkaMessage]
      val consumerSink = Sink.actorRef(probe.ref, "DONE")
      consumerSource.via(consumerFlow).runWith(consumerSink)

      //Creating collection of received messages to compare sent ones to
      var receivedKinesisMsgs = ArrayBuffer[Any]()

      //Publish some test messages
      val numOfMessages = 10
      val kinesisMsgs = for {i <- 1 to numOfMessages} yield KinesisMessage("sometime", "somestuff", i)
      val producerProps = producerSettings.KafkaProducerInfo("KinesisMessage")
      val producerSource = Source(kinesisMsgs)
      val producerFlow = createStreamFlow[KinesisMessage](producerProps)
      val producerSink = createStreamSink(producerProps)
      producerSource.via(producerFlow).runWith(producerSink)

      while (receivedKinesisMsgs.length < kinesisMsgs.length) {
        probe.expectMsgPF(5 seconds) {
          case msgBatch: ArrayBuffer[_] => for (msg <- msgBatch) {
            if (kinesisMsgs.contains(msg)) {
              receivedKinesisMsgs += msg;
              ()
            } else fail()
          }
          case "complete" => ()
          case other => println("Unknown Message:" + other); ()
        }
      }
    }
  }

  "Consuming ExampleAppEvent messages in JSON from from Kinesis" should {
    "be converted to ExampleAppEvents and all of them then obtained by the Stream Sink " in {

      //Creating KafkaMessage Consumer Stream Components
      val consumerProps = consumerSettings.KafkaConsumerInfo("ExampleAppEvent")
      val consumerSource = createStreamSource(consumerProps)
      val consumerFlow = createStreamFlow[ExampleAppEvent]
      val consumerSink = Sink.actorRef(probe.ref, "DONE")
      consumerSource.via(consumerFlow).runWith(consumerSink)

      //Creating collection of received messages to compare sent ones to
      var receivedEventMsgs = ArrayBuffer[Any]()

      //Publish some test messages
      val numOfMessages = 10
      val eventMsgs = for {i <- 1 to numOfMessages} yield ExampleAppEvent("sometime", "senderID", s"Event number $i/$numOfMessages occured")
      val producerProps = producerSettings.KafkaProducerInfo("ExampleAppEvent")
      val producerSource = Source(eventMsgs)
      val producerFlow = createStreamFlow[ExampleAppEvent](producerProps)
      val producerSink = createStreamSink(producerProps)
      producerSource.via(producerFlow).runWith(producerSink)

      while (receivedEventMsgs.length < eventMsgs.length) {
        probe.expectMsgPF(5 seconds) {
          case msgBatch: ArrayBuffer[_] => for (msg <- msgBatch) {
            if (eventMsgs.contains(msg)) {
              receivedEventMsgs += msg;
              ()
            } else fail()
          }
          case "complete" => ()
        }
      }
    }
  }

  "Consuming KinesisMessages messages in JSON from from Kinesis ExampleAppEventChannel" should {
    "fail to be converted to ExampleAppEvent messages and hence nothing should be obtained by the Stream Sink " in {

      //Manually creating a producer stream with a custom Flow which sends the messages to the wrong topic
      val numOfMessages = 10
      val kinesisMsgs = for {i <- 1 to numOfMessages} yield KinesisMessage("sometime", "somestuff", i)
      val producerProps = producerSettings.KinesisProducerInfo("ExampleAppEvent")
      val producerSource = Source(kinesisMsgs)
      val producerFlow = Flow[KinesisMessage].map { msg =>
        val stringJSONMessage = Conversion[KinesisMessage].convertToJson(msg)
        val topicToPublish = "TempChannel2"
        new ProducerRecord[Array[Byte], String](topicToPublish, 0, null, stringJSONMessage)
      }
      val producerSink = createStreamSink(producerProps)
      producerSource.via(producerFlow).runWith(producerSink)

      //Creating collection of received messages to compare sent ones to
      var receivedEventMsgs = ArrayBuffer[String]()

      //Start a normal consumer stream which will fail to convert the published messages
      testConsumerStreamManager ! InitializeConsumerStream(self, ExampleAppEvent)

      //Using the already materialized and ran ConsumerStream
      while (receivedEventMsgs.length < kinesisMsgs.length) {
        testEventListener.expectMsgPF(5 seconds) {
          case ExampleAppEvent(_, _, msg) =>
            if (msg contains "FailedMessageConversion") {
              receivedEventMsgs += msg
            } else if (msg contains "ActivatedConsumerStream") () else fail()
        }
      }
    }
  }
}
