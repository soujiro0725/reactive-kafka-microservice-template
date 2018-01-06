package akka.kinesis

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit, TestProbe}
import com.soujiro0725.consumers.ConsumerStream
import com.soujiro0725.producers.ProducerStream
import com.soujiro0725.settings.Settings
import com.soujiro0725.shared.JsonMessageConversion.Conversion
import com.soujiro0725.shared.KinesisMessages.{ExampleAppEvent, KinesisMessage}
//import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}


class ProducerStreamSpec extends TestKit(ActorSystem("ProducerStreamSpec"))
    with DefaultTimeout with ImplicitSender
    with WordSpecLike with Matchers with BeforeAndAfterAll
    with ConsumerStream with ProducerStream {

    val settings = Settings(system).KafkaProducers
    val probe = TestProbe()

    override def afterAll: Unit = {
        shutdown()
    }

    "Sending KinesisMessages to the KinesisMessage producerStream" should {
        "be converted to JSON and obtained by the Stream Sink " in {

            //Creating Producer Stream Components for publishing KafkaMessages
            val producerProps = settings.KafkaProducerInfo("KinesisMessage")
            val numOfMessages = 50
            val kinesisMsgs = for { i <- 0 to numOfMessages} yield KinesisMessage("sometime", "somestuff", i)
            val producerSource= Source(kinesisMsgs)
            val producerFlow = createStreamFlow[KinesisMessage](producerProps)
            val producerSink = Sink.actorRef(probe.ref, "complete")

            val jsonKinesisMsgs = for { msg <- kinesisMsgs} yield Conversion[KinesisMessage].convertToJson(msg)

            producerSource.via(producerFlow).runWith(producerSink)
            for (i <- 0 to jsonKinesisMsgs.length) {
                probe.expectMsgPF(){
                    case m: ProducerRecord[_,_] => if (jsonKinesisMsgs.contains(m.value())) () else fail()
                    case "complete" => ()
                }
            }
        }
    }

    "Sending ExampleAppEvent messages to the EventMessage producerStream" should {
        "be converted to JSON and obtained by the Stream Sink " in {

            //Creating Producer Stream Components for publishing ExampleAppEvent messages
            val producerProps = settings.KafkaProducerInfo("ExampleAppEvent")
            val numOfMessages = 50
            val eventMsgs = for { i <- 0 to 50} yield ExampleAppEvent("sometime", "senderID", s"Event number $i occured")

            val producerSource= Source(eventMsgs)
            val producerFlow = createStreamFlow[ExampleAppEvent](producerProps)
            val producerSink = Sink.actorRef(probe.ref, "complete")

            val jsonAppEventMsgs = for{ msg <- eventMsgs} yield Conversion[ExampleAppEvent].convertToJson(msg)
            producerSource.via(producerFlow).runWith(producerSink)
            for (i <- 0 to jsonAppEventMsgs.length){
                probe.expectMsgPF(){
                    case m: ProducerRecord[_,_] => if (jsonAppEventMsgs.contains(m.value())) () else fail()
                    case "complete" => ()
                }
            }
        }
    }
}
