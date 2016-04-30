package it.dtk.reactive.jobs

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Source, _ }
import akka.stream.{ ActorMaterializer, ClosedShape }
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticQueryTerms
import it.dtk.model._
import it.dtk.protobuf.Article
import it.dtk.{ NewsUtils, QueryTermsSearch }
import net.ceedubs.ficus.Ficus._
import org.joda.time.DateTime
import redis.clients.jedis.Jedis

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Random

/**
 * Created by fabiofumarola on 08/03/16.
 */
@deprecated
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
  val redisDB = config.as[Int]("redis.processTerms.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  def run(): Unit = {
    val kafkaSink = KafkaHelper.feedItemsSink(kafkaBrokers, writeTopic)
    val feedsSink = ElasticHelper.feedSink(client.client, feedsIndexPath, batchSize, 2)
    val queryTermSink = ElasticHelper.queryTermSink(client.client, termsIndexPath, batchSize, 2)

    val termArticlesSource = Source.tick(10.second, interval, 1)
      .flatMapConcat(_ => ElasticHelper.queryTermSource(client.client, client.queryTermsSortedDesc()))
      .take(50)
      .map { qt =>
        Thread.sleep(Random.nextInt(5000))
        val urls = Seq(QueryTermsSearch.generateUrl(qt.terms, qt.lang, hostname))

        var results = List.empty[Article]
        var extracted = true

        for (url <- urls; if extracted) {
          val data = Await.result(QueryTermsSearch.getResultsAsArticles(url), 10.seconds)
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
        .mapAsync(1)(NewsUtils.mainContent)

      val updateQueryTerms = Flow[(QueryTerm, List[Article])]
        .map(_._1.copy(timestamp = Option(DateTime.now())))

      val articleToMessage = Flow[Article]
        .map(a => KafkaHelper.wrap(writeTopic, a))

      termArticlesSource ~> bcast.in
      bcast.out(0) ~> filterAndProcess ~> printArticle ~> articleToMessage ~> kafkaSink
      bcast.out(1) ~> extractFeeds ~> feedsSink
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

  def extractFeeds() = Flow[(QueryTerm, List[Article])]
    .mapConcat(_._2)
    .mapAsync(1)(a => NewsUtils.extractRss(a.uri))
    .mapConcat(identity)

}
