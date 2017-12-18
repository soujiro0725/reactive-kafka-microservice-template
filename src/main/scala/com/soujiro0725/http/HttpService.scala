package com.soujiro0725.http

import akka.http.scaladsl.server.Directives._
import com.soujiro0725.http.routes.{ConsumerCommands, ProducerCommands}


trait HttpService extends ConsumerCommands with ProducerCommands {
    //Joining the Http Routes
    def routes = producerHttpCommands ~ dataConsumerHttpCommands ~ eventConsumerHttpCommands
}
