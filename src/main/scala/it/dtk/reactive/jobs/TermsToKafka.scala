package it.dtk.reactive.jobs

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, SinkShape}
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.softwaremill.react.kafka.{ProducerMessage, ProducerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticQueryTerms
import it.dtk.model._
import it.dtk.reactive.util.InfluxDBWrapper
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.influxdb.InfluxDBFactory
import org.influxdb.dto._
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.reactivestreams.Subscriber

import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success}

/**
  * Created by fabiofumarola on 08/03/16.
  */
class TermsToKafka(configFile: String, kafka: ReactiveKafka)(implicit val system: ActorSystem, implicit val mat: ActorMaterializer) {

  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val termsDocPath = config.as[String]("elastic.docs.query_terms")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")

  val feedsIndexPath = config.as[String]("elastic.docs.feeds")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val topic = config.as[String]("kafka.topics.query_terms")

  //scheduler params
  val interval = config.as[FiniteDuration]("schedulers.queryTerms.each")

  val client = new ElasticQueryTerms(esHosts, termsDocPath, clusterName).client

  val influxDB = new InfluxDBWrapper(config)

  def run(): Unit = {
    implicit val exec = system.dispatcher
    queryTermSource().to(kafkaSink()).run()

    Source.tick(0 millis, interval, Unit)
      .map { _ =>
        queryTermSource().to(kafkaSink()).run()
        queryTermSource().map(_ => 1).toMat(Sink.reduce[Int](_ + _))(Keep.right).run()
      }.runWith(Sink.foreach { futCount =>
      futCount.onComplete {
        case Success(count) =>
          println(s"extracted  ${count} query terms")

          influxDB.write(
            "TermsToKafka",
            Map("extracted" -> count),
            Map())

        case Failure(ex) =>
          ex.printStackTrace()
      }
    })
  }

  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  def queryTermSource(): Source[QueryTerm, NotUsed] = {
    implicit object QueryTermsHitAs extends HitAs[QueryTerm] {
      override def as(hit: RichSearchHit): QueryTerm = {
        parse(hit.getSourceAsString).extract[QueryTerm]
      }
    }

    val publisher: ScrollPublisher = client.publisher(termsDocPath, keepAlive = "180m")
    Source.fromPublisher(publisher)
      .map(res => res.as[QueryTerm])
  }

  def kafkaSink(): Sink[QueryTerm, NotUsed] = {

    val kafkaSub: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] =
      kafka.publish(ProducerProperties(
        bootstrapServers = kafkaBrokers,
        topic = topic,
        valueSerializer = new ByteArraySerializer()))

    Sink.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val transformation = b.add(Flow[QueryTerm]
        .map(t => ProducerMessage(t.terms.mkString("_").getBytes, write(t).getBytes)))

      transformation ~> Sink.fromSubscriber(kafkaSub)

      SinkShape.of(transformation.in)

    })
  }

}
