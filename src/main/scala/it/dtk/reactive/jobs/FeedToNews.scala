package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Supervision }
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.softwaremill.react.kafka.ReactiveKafka
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticQueryTerms
import it.dtk.model.{ Feed, SchedulerData }
import it.dtk.protobuf._
import helpers._
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import net.ceedubs.ficus.Ficus._

import scala.language.implicitConversions

/**
 * Created by fabiofumarola on 09/03/16.
 */
class FeedToNews(configFile: String, kafka: ReactiveKafka)(implicit
  val system: ActorSystem,
    implicit val mat: ActorMaterializer) {
  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val feedsDocPath = config.as[String]("elastic.docs.feeds")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val topic = config.as[String]("kafka.topics.feeds")

  val client = new ElasticQueryTerms(esHosts, feedsDocPath, clusterName).client

  def run(): Unit = {
    val feedArticles = feedSource().
      via(extractArticles())

    val feeds = feedArticles.map(_._1)

    saveToElastic(feeds, client, feedsDocPath, batchSize, 1)

    val articles = feedArticles
      .flatMapConcat(_._2)
      .map(gander.mainContent)

    saveArticlesToKafkaProtobuf(articles, kafka, kafkaBrokers, topic)
  }

  def feedSource(): Source[Feed, NotUsed] = {

    implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all
    implicit object FeedHitAs extends HitAs[Feed] {
      override def as(hit: RichSearchHit): Feed = {
        parse(hit.getSourceAsString).extract[Feed]
      }
    }

    val publisher: ScrollPublisher = client.publisher(feedsDocPath, keepAlive = "60m")

    Source.fromPublisher(publisher)
      .map(_.as[Feed])
      .filter(_.schedulerData.time.isBeforeNow)
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
