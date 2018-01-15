package com.soujiro0725.producers

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
//import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.amazonaws.services.kinesis.model.{ PutRecordsRequestEntry, PutRecordsResultEntry, ShardIteratorType }
import com.soujiro0725.shared.JsonMessageConversion.Conversion
import com.soujiro0725.shared.{AkkaStreams, EventSourcing}
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import akka.stream.alpakka.kinesis.scaladsl.{KinesisSource,KinesisSink, KinesisFlow}
import akka.stream.alpakka.kinesis.ShardSettings
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import scala.concurrent.duration._
import com.amazonaws.services.kinesis.model.Record

/**
  * This trait defines the functions for creating the producer stream components.
  */

trait ProducerStream extends AkkaStreams with EventSourcing {
  implicit val system: ActorSystem
  def self: ActorRef

  val streamName = "TestDataChannel" // TODO read from application.conf
  implicit val amazonKinesisAsync: AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.defaultClient()

  // TODO read from application.conf
  val shardSettings = ShardSettings(streamName = streamName,
    shardId = "shard-id",
    shardIteratorType = ShardIteratorType.TRIM_HORIZON,
    refreshInterval = 1.second,
    limit = 500)

  val flowSettings = KinesisFlowSettings(
    parallelism = 1,
    maxBatchSize = 500,
    maxRecordsPerSecond = 1000,
    maxBytesPerSecond = 1000000,
    maxRetries = 5,
    backoffStrategy = KinesisFlowSettings.Exponential,
    retryInitialTimeout = 100 millis
  )

  def createStreamSource[msgType]: Source[Record, NotUsed] = {
    // TODO backpressure
    //Source.queue[msgType](Int.MaxValue, OverflowStrategy.backpressure)
    KinesisSource.basic(shardSettings, amazonKinesisAsync)
  }

  def createStreamSink(producerProperties: Map[String, String]):Sink[PutRecordsRequestEntry, NotUsed] = {
    KinesisSink(streamName, flowSettings)
  }

  def createStreamFlow[msgType: Conversion](producerProperties: Map[String, String]):
      Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] = {

    // val numberOfPartitions = producerProperties("num.partitions").toInt -1
    // val topicToPublish = producerProperties("publish-topic")
    // val rand = new scala.util.Random
    // val range = 0 to numberOfPartitions

    KinesisFlow(streamName, flowSettings)

    // Flow[msgType].map { msg =>
    //   val partition = range(rand.nextInt(range.length))
    //   val stringJSONMessage = Conversion[msgType].convertToJson(msg)
    //   new ProducerRecord[Array[Byte], String](topicToPublish, partition, null, stringJSONMessage)
    // }
  }
}
