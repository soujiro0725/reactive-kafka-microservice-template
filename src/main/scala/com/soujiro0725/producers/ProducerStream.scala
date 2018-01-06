package com.soujiro0725.producers

import akka.actor.{ActorRef, ActorSystem}
// import akka.kafka.ProducerSettings
// import akka.kafka.scaladsl.Producer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Source}
import com.soujiro0725.shared.JsonMessageConversion.Conversion
import com.soujiro0725.shared.{AkkaStreams, EventSourcing}
// import org.apache.kafka.clients.producer.ProducerRecord
// import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import com.amazonaws.services.kinesis.model.ShardIteratorType
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import akka.stream.alpakka.kinesis.javadsl.{KinesisSource,KinesisSink}
import akka.stream.alpakka.kinesis.ShardSettings
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import scala.concurrent.duration._

/**
  * This trait defines the functions for creating the producer stream components.
  */

trait ProducerStream extends AkkaStreams with EventSourcing {
  implicit val system: ActorSystem
  def self: ActorRef

  val streamName = "TestDataChannel"
  val amazonKinesisAsync: AmazonKinesisAsync = AmazonKinesisAsyncClientBuilder.defaultClient()

  def createStreamSource[msgType] = {
        Source.queue[msgType](Int.MaxValue,OverflowStrategy.backpressure)
    }

    def createStreamSink(producerProperties: Map[String, String]) = {
      val producerFlowSettings = KinesisFlowSettings(
        parallelism = 1,
        maxBatchSize = 500,
        maxRecordsPerSecond = 1000,
        maxBytesPerSecond = 1000000,
        maxRetries = 5,
        backoffStrategy = KinesisFlowSettings.Exponential,
        retryInitialTimeout = 100 millis
      )

      //Producer.plainSink(producerSettings)
      KinesisSink(
        streamName,
        producerFlowSettings,
        amazonKinesisAsync
      )
    }

    def createStreamFlow[msgType: Conversion](producerProperties: Map[String, String]) = {
        val numberOfPartitions = producerProperties("num.partitions").toInt -1
        val topicToPublish = producerProperties("publish-topic")
        val rand = new scala.util.Random
        val range = 0 to numberOfPartitions

        Flow[msgType].map { msg =>
            val partition = range(rand.nextInt(range.length))
            val stringJSONMessage = Conversion[msgType].convertToJson(msg)
            new ProducerRecord[Array[Byte], String](topicToPublish, partition, null, stringJSONMessage)
        }
    }
}
