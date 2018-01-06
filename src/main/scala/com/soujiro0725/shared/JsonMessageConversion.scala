package com.soujiro0725.shared

import akka.util.Timeout
import com.soujiro0725.shared.EventMessages.FailedMessageConversion
import com.soujiro0725.shared.KinesisMessages.{ExampleAppEvent, KinesisMessage}
import play.api.libs.json.Json
import spray.json._

import scala.concurrent.duration._


/**
  * Here we define a typeclass which converts case class messages to/from JSON.
  * Currently, we can convert KinesisMessage and ExampleAppEvent messages to/from JSON.
  * Any additional case class types need to have conversion methods defined here.
  */


object JsonMessageConversion {
    implicit val resolveTimeout = Timeout(3 seconds)

    trait Conversion[T] {
        def convertFromJson(msg: String): Either[FailedMessageConversion, T]
        def convertToJson(msg: T): String
    }

    //Here is where we create implicit objects for each Message Type you wish to convert to/from JSON
    object Conversion extends DefaultJsonProtocol {

        implicit object KinesisMessageConversions extends Conversion[KinesisMessage]  {
            implicit val json3 = jsonFormat3(KinesisMessage)

            /**
              * Converts the JSON string from the CommittableMessage to KinesisMessage case class
              * @param msg is the json string to be converted to KinesisMessage case class
              * @return either a KinesisMessage or Unit (if conversion fails)
              */
            def convertFromJson(msg: String): Either[FailedMessageConversion, KinesisMessage] = {
                try {
                    Right(msg.parseJson.convertTo[KinesisMessage])
                }
                catch {
                    case e: Exception => Left(FailedMessageConversion("kinesisTopic", msg, "to: KinesisMessage"))
                }
            }
            def convertToJson(msg: KinesisMessage) = {
                implicit val writes = Json.writes[KinesisMessage]
                Json.toJson(msg).toString
            }
        }

        implicit object ExampleAppEventConversion extends Conversion[ExampleAppEvent] {
            implicit val json3 = jsonFormat3(ExampleAppEvent)

            /**
              * Converts the JSON string from the CommittableMessage to ExampleAppEvent case class
              * @param msg is the json string to be converted to ExampleAppEvent case class
              * @return either a ExampleAppEvent or Unit (if conversion fails)
              */
            def convertFromJson(msg: String): Either[FailedMessageConversion, ExampleAppEvent] = {
                try {
                     Right(msg.parseJson.convertTo[ExampleAppEvent])
                }
                catch {
                    case e: Exception => Left(FailedMessageConversion("kinesisTopic", msg, "to: ExampleAppEvent"))
                }
            }
            def convertToJson(msg: ExampleAppEvent) = {
                implicit val writes = Json.writes[ExampleAppEvent]
                Json.toJson(msg).toString
            }
        }

        //Adding some sweet sweet syntactic sugar
        def apply[T: Conversion] : Conversion[T] = implicitly
    }
}


