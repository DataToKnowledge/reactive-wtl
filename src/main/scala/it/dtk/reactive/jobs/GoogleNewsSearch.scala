package it.dtk.reactive.jobs

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.stream._
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import it.dtk.NewsUtils
import it.dtk.es.ESUtil
import it.dtk.model.GoogleNews
import it.dtk.protobuf.Article
import it.dtk.reactive.jobs.ElasticHelper._
import it.dtk.reactive.jobs.Utils._
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime
import redis.clients.jedis.Jedis

import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Created by fabiofumarola on 30/04/16.
  */
class GoogleNewsSearch(configFile: String)(implicit val system: ActorSystem, implicit val mat: ActorMaterializer) {
  val logName = this.getClass.getSimpleName

  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")
  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val feedsDocPath = config.as[String]("elastic.docs.feeds")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")
  val parallel = config.as[Int]("elastic.feeds.parallel")

  val googleNewsIndexPath = config.as[String]("elastic.docs.google_news")
  val client = ESUtil.elasticClient(esHosts, clusterName)

  //scheduler params
  val interval = config.as[FiniteDuration]("schedulers.google_news.each")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val consumerGroup = config.as[String]("kafka.groups.google_news_group")
  val writeTopic = config.as[String]("kafka.topics.feed_items")

  //redis params
  val redisHost = config.as[String]("redis.host")
  val redisDB = config.as[Int]("redis.googleNews.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  def run(): Unit = {

    val source = Source.tick(10.seconds, interval, 1)
      .log(logName, x => s"Starting extraction at ${DateTime.now()}")
//      .withAttributes(Attributes.logLevels(onElement = Logging.DebugLevel))
      .flatMapConcat(_ => ElasticHelper.googleNewsSource(client, googleNewsIndexPath))
      .throttle(1, 5.seconds, 1, ThrottleMode.shaping)


    val esFeedsSink = feedSink(client, feedsDocPath, batchSize, parallel)
    val kafkaSink = KafkaHelper.articleSink(kafkaBrokers, writeTopic)

    val graph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[Article](2))

      source ~> queryGoogleNews ~> filterUrl ~> bcast.in
      bcast.out(0) ~> mainContent ~> printArticle(logName) ~> articleToMessage ~> kafkaSink
      bcast.out(1) ~> extractFeeds ~> printFeed(logName) ~> esFeedsSink

      ClosedShape
    }

    RunnableGraph.fromGraph(graph).run()
  }

  def queryGoogleNews() = Flow[GoogleNews]
    .map(q => NewsUtils.extractFromGoogleNews(q.search, q.lang))
    .filter(_.isSuccess)
    .map(_.get.toList)
    .mapConcat(identity)
    .log(logName, (a: Article) => s"extracted news with url ${a.uri}")

  def filterUrl = Flow[Article]
    .filterNot(a => duplicatedUrl(a.uri))

  def duplicatedUrl(uri: String): Boolean = {
    val found = Option(jedis.get(uri))
    if (found.isEmpty) jedis.set(uri, "1")
    else jedis.incr(uri)

    found.isDefined
  }

  def mainContent() = Flow[Article].mapAsync(1)(NewsUtils.mainContent)

  def articleToMessage() = Flow[Article]
    .map(a => KafkaHelper.wrap(writeTopic, a))

  def extractFeeds() = Flow[Article]
    .mapAsync(1)(a => NewsUtils.extractRss(a.uri))
    .mapConcat(identity)
}
