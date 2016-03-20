package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, SinkShape}
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.softwaremill.react.kafka.{ProducerMessage, ProducerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticFeeds
import it.dtk.model.Feed
import it.dtk.reactive.util.InfluxDBWrapper
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.reactivestreams.Subscriber

import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.implicitConversions
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by fabiofumarola on 09/03/16.
  */
class FeedsToKafka(configFile: String, kafka: ReactiveKafka)(implicit val system: ActorSystem, implicit val mat: ActorMaterializer) {
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val feedsDocPath = config.as[String]("elastic.docs.feeds")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val writeTopic = config.as[String]("kafka.topics.feeds")

  val interval = config.as[FiniteDuration]("schedulers.feeds.each")

  val client = new ElasticFeeds(esHosts, feedsDocPath, clusterName).client

  val inlufxDB = new InfluxDBWrapper(config)

  def run(): Unit = {
    feedSource().to(kafkaSink()).run()

    Source.tick(0.millis, interval, feedSource()).map { feeds =>
      feeds.to(kafkaSink()).run()

      feeds
        .map(f => 1)
        .toMat(Sink.reduce[Int](_ + _))(Keep.right)
        .run()

    }.runWith(Sink.foreach { future =>
      future.foreach { value =>

        inlufxDB.write("FeedsToKafka",
          Map("written" -> value),
          Map()
        )
      }
    })
  }

  def feedSource(): Source[Feed, NotUsed] = {
    implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

    val publisher: ScrollPublisher = client.publisher(feedsDocPath, keepAlive = "60m")

    Source.fromPublisher(publisher)
      .map(hit => parse(hit.getSourceAsString).extract[Feed])
  }

  def kafkaSink(): Sink[Feed, NotUsed] = {
    val kafkaSub: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] =
      kafka.publish(ProducerProperties(
        bootstrapServers = kafkaBrokers,
        topic = writeTopic,
        valueSerializer = new ByteArraySerializer()
      ))

    Sink.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val transformation = b.add(Flow[Feed].
        map(f => ProducerMessage(f.url.getBytes, write(f).getBytes)))

      transformation ~> Sink.fromSubscriber(kafkaSub)

      SinkShape.of(transformation.in)
    })
  }

}
