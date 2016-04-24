package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, ThrottleMode}
import com.typesafe.config.ConfigFactory
import it.dtk.es._
import it.dtk.model.{Feed, SchedulerData}
import it.dtk.protobuf._
import it.dtk.reactive.jobs.helpers._
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime
import redis.clients.jedis.Jedis

import scala.concurrent.duration._
import scala.language.implicitConversions

/**
 * Created by fabiofumarola on 09/03/16.
 */
class ProcessFeeds(configFile: String)(implicit val system: ActorSystem,
                                       implicit val mat: ActorMaterializer) {
  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val feedsDocPath = config.as[String]("elastic.docs.feeds")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")
  val parallel = config.as[Int]("elastic.feeds.parallel")

  val client = elasticClient(esHosts, clusterName)

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val consumerGroup = config.as[String]("kafka.groups.feed_group")
  val writeTopic = config.as[String]("kafka.topics.feed_items")

  //redis Params
  val redisHost = config.as[String]("redis.host")
  val redisDB = config.as[Int]("redis.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  val interval = config.as[FiniteDuration]("schedulers.feeds.each")

  def run(): Unit = {
    val kafkaSink = kafka.articleSink(kafkaBrokers, writeTopic)

    val feedArticles = Source.tick(10.second, interval, 1)
      .flatMapConcat(_ => elastic.feedSource(client, feedsDocPath))
      .via(extractArticles())
      .map { fa =>
        println(s"processed feed ${fa._1.url} articles extracted ${fa._2.size}")
        fa
      }

    val feedsSink = elastic.feedSink(client, feedsDocPath, batchSize, parallel)

    val saveGraph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val unzip = b.add(Unzip[Feed, List[Article]]())
      val printFeed = Flow[Feed].map { f => println(f); f }

      feedArticles ~> unzip.in
      unzip.out0 ~> feedsSink
      unzip.out1 ~> processArticles() ~> kafkaSink
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

  def processArticles() =
    Flow[List[Article]]
      .mapConcat(identity)
      .filterNot(a => duplicatedUrl(a.uri))
      .throttle(1, 1.second, 200, ThrottleMode.Shaping)
      .map(gander.mainContent)
      .map(a => kafka.wrap(a))

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
        schedulerData = nextSchedule)
      (fUpdated, articles)
    }
}
