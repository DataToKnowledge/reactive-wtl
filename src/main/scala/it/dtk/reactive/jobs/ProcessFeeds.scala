package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, ThrottleMode}
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.softwaremill.react.kafka.{ProducerMessage, ProducerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticFeeds
import it.dtk.model.{Feed, SchedulerData}
import it.dtk.protobuf._
import it.dtk.reactive.jobs.helpers._
import it.dtk.reactive.util.InfluxDBWrapper
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.reactivestreams.Subscriber
import redis.clients.jedis.Jedis

import scala.concurrent.duration._
import scala.language.implicitConversions

/**
  * Created by fabiofumarola on 09/03/16.
  */
class ProcessFeeds(configFile: String, kafka: ReactiveKafka)(implicit
                                                             val system: ActorSystem,
                                                             implicit val mat: ActorMaterializer) {
  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val feedsDocPath = config.as[String]("elastic.docs.feeds")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")
  val parallel = config.as[Int]("elastic.feeds.parallel")

  val interval = config.as[FiniteDuration]("schedulers.feeds.each")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val consumerGroup = config.as[String]("kafka.groups.feed_group")
  val writeTopic = config.as[String]("kafka.topics.feed_items")

  val kafkaSink: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] =
    kafka.publish(ProducerProperties(
      bootstrapServers = kafkaBrokers,
      topic = writeTopic,
      valueSerializer = new ByteArraySerializer()
    ))

  val client = new ElasticFeeds(esHosts, feedsDocPath, clusterName).client

  val influxDB = new InfluxDBWrapper(config)

  val redisHost = config.as[String]("redis.host")
  val redisDB = config.as[Int]("redis.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  def run(): Unit = {

    case object Tick

    val feedArticles = Source.tick(10.second, interval, Tick)
      .flatMapConcat(_ => feedSource())
      .via(extractArticles())
      .map { fa =>
        println(s"processed feed ${fa._1.url} articles extracted ${fa._2.size}")
        influxDB.write(
          "ProcessFeeds",
          Map("url" -> fa._1.url, "articles" -> fa._2.size), Map()
        )
        fa
      }

    val feedsSink = elasticFeedSink(client, feedsDocPath, batchSize, parallel)

    val saveGraph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val unzip = b.add(Unzip[Feed, List[Article]]())
      val printFeed = Flow[Feed].map { f => println(f); f }

      feedArticles ~> unzip.in
      unzip.out0 ~> feedsSink
      unzip.out1 ~> processArticles() ~> Sink.fromSubscriber(kafkaSink)
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

  def processArticles(): Flow[List[Article], ProducerMessage[Array[Byte], Array[Byte]], NotUsed] = Flow[List[Article]]
    .mapConcat(identity)
    .filterNot(a => duplicatedUrl(a.uri))
    .throttle(1, 1.second, 200, ThrottleMode.Shaping)
    .map(gander.mainContent)
    .map(a => ProducerMessage(a.uri.getBytes, a.toByteArray()))

//  def feedSource(): Source[Feed, NotUsed] = {
//    val publisher = kafka.consume(ConsumerProperties(
//      bootstrapServers = kafkaBrokers,
//      topic = readTopic,
//      groupId = consumerGroup,
//      valueDeserializer = new StringDeserializer()
//    ))
//
//    Source.fromPublisher(publisher)
//      .map(rec => parse(rec.value()).extract[Feed])
//      .filter(_.schedulerData.time.isBeforeNow)
//  }

  def feedSource(): Source[Feed, NotUsed] = {
    implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

    val publisher: ScrollPublisher = client.publisher(feedsDocPath, keepAlive = "60m")

    Source.fromPublisher(publisher)
      .map(hit => parse(hit.getSourceAsString).extract[Feed])
  }

  def extractArticles(): Flow[Feed, (Feed, List[Article]), NotUsed] =
    Flow[Feed].map { f =>
      val feedItems = feedExtr.parse(f.url, f.publisher)
      val feedUrls = f.parsedUrls.toSet
      val articles = feedItems.filterNot(a => feedUrls.contains(a.uri)).toList

      val nextSchedule = SchedulerData.next(f.schedulerData, articles.size)
      val parsedUrls = articles.map(_.uri) ::: f.parsedUrls

      val fUpdated = f.copy(
        lastTime = Option(DateTime.now),
        parsedUrls = parsedUrls.take(200),
        count = f.count + articles.size,
        schedulerData = nextSchedule
      )
      (fUpdated, articles)
    }
}
