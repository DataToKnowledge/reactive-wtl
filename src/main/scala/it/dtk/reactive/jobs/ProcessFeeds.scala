package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl._
import akka.stream.{ Graph, ActorMaterializer, ClosedShape, ThrottleMode }
import com.typesafe.config.ConfigFactory
import it.dtk.NewsUtils
import it.dtk.es._
import it.dtk.model.{ Feed, SchedulerData }
import it.dtk.protobuf._
import it.dtk.reactive.jobs.ElasticHelper._
import it.dtk.reactive.jobs.KafkaHelper._
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime
import redis.clients.jedis.Jedis
import StreamUtils._

import scala.concurrent.duration._
import scala.language.implicitConversions

/**
 * Created by fabiofumarola on 09/03/16.
 */
class ProcessFeeds(configFile: String)(implicit
  val system: ActorSystem,
    implicit val mat: ActorMaterializer) {

  implicit val log = Logging(system, this.getClass)
  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val feedsDocPath = config.as[String]("elastic.docs.feeds")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")
  val parallel = config.as[Int]("elastic.feeds.parallel")

  val client = ESUtil.elasticClient(esHosts, clusterName)

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val consumerGroup = config.as[String]("kafka.groups.feed_group")
  val writeTopic = config.as[String]("kafka.topics.feed_items")

  //redis Params
  val redisHost = config.as[String]("redis.host")
  val redisDB = config.as[Int]("redis.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  def duplicatedUrl(uri: String): Boolean = {
    val found = Option(jedis.get(uri))
    if (found.isEmpty) jedis.set(uri, "1")
    else jedis.incr(uri)

    found.isDefined
  }

  val interval = config.as[FiniteDuration]("schedulers.feeds.each")

  def run(): Unit = {
    Source.tick(10.seconds, interval, 1)
      .runForeach { _ =>
        RunnableGraph.fromGraph(graph()).run()
      }
  }

  private def graph(): Graph[ClosedShape.type, NotUsed] = {
    val feedsSink = feedSink(client, feedsDocPath, batchSize, parallel)
    val articleSink = KafkaHelper.articleSink(kafkaBrokers, writeTopic)

    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val printFeedArticle = Flow[(Feed, List[Article])].map { fa =>
        println(s"extracted ${fa._2.size} articles extracted  from feed ${fa._1.url}")
        fa
      }
      val printArticle = Flow[Article].map { a => println(s"Saving article ${a.uri}"); a }

      val unzip = b.add(Unzip[Feed, List[Article]]())
      val toMessage = Flow[Article].map(a => wrap(writeTopic, a))

      feedSource(client, feedsDocPath) ~> parseFeed ~> printFeedArticle ~> unzip.in
      unzip.out0 ~> feedsSink
      unzip.out1 ~> processArticles() ~> printArticle ~> toMessage ~> articleSink

      ClosedShape
    }
  }

  def processArticles() =
    Flow[List[Article]]
      .mapConcat(identity)
      .filterNot(a => duplicatedUrl(a.uri))
      .throttle(1, 1.second, 200, ThrottleMode.Shaping)
      .mapAsync(1)(NewsUtils.mainContent)

  def parseFeed(): Flow[Feed, (Feed, List[Article]), NotUsed] =
    Flow[Feed].
      map { f =>
        val feedItems = NewsUtils.parseFeed(f.url, f.publisher).getOrElse(Seq.empty)
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
