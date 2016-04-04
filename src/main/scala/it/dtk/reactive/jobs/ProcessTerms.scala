package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source, _}
import akka.stream.{ThrottleMode, Attributes, ActorMaterializer, ClosedShape}
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.sksamuel.elastic4s.{RichSearchHit, HitAs}
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerMessage, ProducerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticFeeds
import it.dtk.model._
import it.dtk.protobuf.Article
import it.dtk.reactive.jobs.helpers._
import it.dtk.reactive.util.InfluxDBWrapper
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringDeserializer}
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.reactivestreams.Subscriber
import redis.clients.jedis.Jedis
import scala.collection.mutable
import scala.concurrent.duration._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import scala.util.control.Breaks._

import scala.language.implicitConversions

/**
  * Created by fabiofumarola on 08/03/16.
  */
class ProcessTerms(configFile: String, kafka: ReactiveKafka)(implicit val system: ActorSystem, implicit val mat: ActorMaterializer) {
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val feedsIndexPath = config.as[String]("elastic.docs.feeds")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")

  val termsDocPath = config.as[String]("elastic.docs.query_terms")
  //scheduler params
  val interval = config.as[FiniteDuration]("schedulers.queryTerms.each")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val readTopic = config.as[String]("kafka.topics.query_terms")
  val consumerGroup = config.as[String]("kafka.groups.query_terms_group")
  val writeTopic = config.as[String]("kafka.topics.feed_items")

  val kafkaSink: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] =
    kafka.publish(ProducerProperties(
      bootstrapServers = kafkaBrokers,
      topic = writeTopic,
      valueSerializer = new ByteArraySerializer()
    ))

  val client = new ElasticFeeds(esHosts, feedsIndexPath, clusterName).client

  val influxDB = new InfluxDBWrapper(config)

  val redisHost = config.as[String]("redis.host")
  val redisDB = config.as[Int]("redis.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  def run(): Unit = {

    case object Tick

    val articles = Source.tick(10.second, interval, Tick)
      .flatMapConcat(_ => queryTermSourceEs())
      .flatMapConcat { qt =>
        Thread.sleep(10000)
        val urls = terms.generateUrls(qt.terms, qt.lang, hostname)

        var results = List.empty[Article]
        var extracted = true

        for (url <- urls; if extracted) {
          val data = terms.getResultsAsArticles(url)
          extracted = data.nonEmpty
          if (!extracted) {
            println(s"Does not extract data for query terms ${qt.terms} and url $url")
            Thread.sleep(10000)
          }

          results ++= data
        }
        results
      }.filterNot(a => duplicatedUrl(a.uri))
      .map(gander.mainContent)

    val feedsSink = elasticFeedSink(client, feedsIndexPath, batchSize, 2)

    val saveGraph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[Article](2))
      val printArticle = Flow[Article].map { a =>
        println(s"Processed article ${a.uri} from Query Terms")

        influxDB.write(
          "ProcessTerms",
          Map("url" -> a.uri, "main_content" -> a.cleanedText.nonEmpty),
          Map("publisher" -> a.publisher)
        )
        a
      }

      articles ~> bcast.in
      bcast.out(0) ~> printArticle ~> Flow[Article].map(a => ProducerMessage(a.uri.getBytes, a.toByteArray())) ~> Sink.fromSubscriber(kafkaSink)
      bcast.out(1) ~> feedFlow() ~> feedsSink
      ClosedShape
    }

    RunnableGraph.fromGraph(saveGraph).run()
  }

  def duplicatedUrl(uri: String): Boolean = {
    val found = Option(jedis.get(uri))
    if (found.isEmpty) jedis.set(uri, "1")
    else jedis.incr(uri)

    found.isDefined
  }

  def feedFlow(): Flow[Article, Feed, NotUsed] = Flow[Article]
    .map(a => a.publisher -> html.host(a.uri))
    .filter(_._2.nonEmpty)
    .map(f => (f._1, f._2.get))
    .filterNot(_._2.contains("comment"))
    .flatMapConcat {
      case (newsPublisher, url) =>
        val rss = html.findMapRssTitle(url).keySet
        Source(rss.filterNot(_.contains("comment")).map(url =>
          Feed(url, newsPublisher, List.empty, Some(DateTime.now()))))
    }

  def queryTermSourceEs(): Source[QueryTerm, NotUsed] = {
    implicit object QueryTermsHitAs extends HitAs[QueryTerm] {
      override def as(hit: RichSearchHit): QueryTerm = {
        parse(hit.getSourceAsString).extract[QueryTerm]
      }
    }

    val publisher: ScrollPublisher = client.publisher(termsDocPath, keepAlive = "180m")
    Source.fromPublisher(publisher)
      .map(res => res.as[QueryTerm])
  }

  def extractArticles(hostname: String) = Flow[QueryTerm]
    .flatMapConcat(q => terms.generateUrls(q.terms, q.lang, hostname))
    .flatMapConcat(u => terms.getResultsAsArticles(u))
    .map(gander.mainContent)

}
