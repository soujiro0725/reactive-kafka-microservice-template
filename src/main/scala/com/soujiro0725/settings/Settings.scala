package com.soujiro0725.settings

import akka.actor.ActorSystem

/**
  * In this class we read in the application.conf configuration file to get the various consumer/producer settings
  * as well as the akka-http host
  */

class Settings(system:ActorSystem) {
    object Http {
        val host = system.settings.config.getString("http.host")
    }
    object KinesisProducers {
        val numberOfProducers = system.settings.config.getInt("akka.kinesis.producer.num-producers")

        val KinesisProducerInfo: Map[String, Map[String,String]] = (for (i <- 1 to numberOfProducers) yield {
            val kinesisMessageType = system.settings.config.getString(s"akka.kinesis.producer.p$i.message-type")
            val kinesisMessageBrokerIP = system.settings.config.getString(s"akka.kinesis.producer.p$i.bootstrap-servers")
            val kinesisStream = system.settings.config.getString(s"akka.kinesis.producer.p$i.publish-topic")
            val numberOfPartitions = system.settings.config.getString(s"akka.kinesis.producer.p$i.num.partitions")
            kinesisMessageType -> Map("bootstrap-servers" -> kinesisMessageBrokerIP, "publish-topic" -> kinesisStream, "num.partitions" -> numberOfPartitions)
        }).toMap
    }

    object KinesisConsumers {
        val numberOfConsumers = system.settings.config.getInt("akka.kinesis.consumer.num-consumers")

        //TODO: We only have one bootstrap server (kinesis broker) at the moment so we get one IP below)
        val KinesisConsumerInfo: Map[String, Map[String,String]] = (for (i <- 1 to numberOfConsumers) yield {
            val kinesisMessageType = system.settings.config.getString(s"akka.kinesis.consumer.c$i.message-type")
            val kinesisMessageBrokerIP = system.settings.config.getString(s"akka.kinesis.consumer.c$i.bootstrap-servers")
            val kinesisStream = system.settings.config.getString(s"akka.kinesis.consumer.c$i.subscription-topic")
            val groupId = system.settings.config.getString(s"akka.kinesis.consumer.c$i.groupId")
            kinesisMessageType -> Map("bootstrap-servers" -> kinesisMessageBrokerIP, "subscription-topic" -> kinesisStream, "groupId" -> groupId)
        }).toMap
    }
}

object Settings {
    def apply(system: ActorSystem) = new Settings(system)
}
