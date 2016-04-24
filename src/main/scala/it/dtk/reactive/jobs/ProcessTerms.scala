package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source, _}
import akka.stream.{ActorMaterializer, ClosedShape}
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.sksamuel.elastic4s.{HitAs, RichSearchHit}
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticQueryTerms
import it.dtk.model._
import it.dtk.protobuf.Article
import it.dtk.reactive.jobs.helpers._
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis
import it.dtk.es._

import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Random

/**
 * Created by fabiofumarola on 08/03/16.
 */
class ProcessTerms(configFile: String)(implicit val system: ActorSystem, implicit val mat: ActorMaterializer) {
//  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")
  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val feedsIndexPath = config.as[String]("elastic.docs.feeds")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")

  val termsIndexPath = config.as[String]("elastic.docs.query_terms")

  val client = new ElasticQueryTerms(esHosts, termsIndexPath, clusterName)

  //scheduler params
  val interval = config.as[FiniteDuration]("schedulers.queryTerms.each")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val consumerGroup = config.as[String]("kafka.groups.query_terms_group")
  val writeTopic = config.as[String]("kafka.topics.feed_items")

  //redis params
  val redisHost = config.as[String]("redis.host")
  val redisDB = config.as[Int]("redis.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  def run(): Unit = {
    val feedItemsSink = kafka.feedItemsSink(kafkaBrokers, writeTopic)
    val feedsSink = elastic.feedSink(client.client, feedsIndexPath, batchSize, 2)
    val queryTermSink = elastic.queryTermSink(client.client, termsIndexPath, batchSize, 2)

    val termArticlesSource = Source.tick(10.second, interval, 1)
      .flatMapConcat(_ => elastic.queryTermSource(client.client, client.queryTermsSortedDesc()))
      .map { qt =>
        Thread.sleep(Random.nextInt(5000))
        val urls = Seq(terms.generateUrl(qt.terms, qt.lang, hostname))

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
        (qt, results)
      }.filter(_._2.nonEmpty)

    val processGraph = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[(QueryTerm, List[Article])](3))
      val printArticle = Flow[Article].map { a =>
        println(s"Processed article ${a.uri} from Query Terms")
        a
      }
      val printQueryTerm = Flow[QueryTerm].map { t => println(t); t }

      val filterAndProcess = Flow[(QueryTerm, List[Article])]
        .mapConcat(_._2)
        .filterNot(a => duplicatedUrl(a.uri))
        .map(gander.mainContent)

      val updateQueryTerms = Flow[(QueryTerm, List[Article])]
        .map(_._1.copy(timestamp = Option(DateTime.now())))

      val articleToMessage = Flow[Article].map(a => kafka.wrap(a))

      termArticlesSource ~> bcast.in
      bcast.out(0) ~> filterAndProcess ~> printArticle ~> articleToMessage ~> feedItemsSink
      bcast.out(1) ~> feedFlow() ~> feedsSink
      bcast.out(2) ~> updateQueryTerms ~> printQueryTerm ~> queryTermSink
      ClosedShape
    }

    RunnableGraph.fromGraph(processGraph).run()
  }

  def duplicatedUrl(uri: String): Boolean = {
    val found = Option(jedis.get(uri))
    if (found.isEmpty) jedis.set(uri, "1")
    else jedis.incr(uri)

    found.isDefined
  }

  def feedFlow(): Flow[(QueryTerm, List[Article]), Feed, NotUsed] = Flow[(QueryTerm, List[Article])]
    .mapConcat(_._2)
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

  def extractArticles(hostname: String) = Flow[QueryTerm]
    .flatMapConcat(q => terms.generateUrls(q.terms, q.lang, hostname))
    .flatMapConcat(u => terms.getResultsAsArticles(u))
    .map(gander.mainContent)

}
