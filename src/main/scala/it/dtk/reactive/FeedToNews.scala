package it.dtk.reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.softwaremill.react.kafka.ReactiveKafka
import it.dtk.es.ElasticQueryTerms
import it.dtk.model.{Article, Feed, SchedulerData}
import it.dtk.reactive.helpers._
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.language.implicitConversions

/**
  * Created by fabiofumarola on 09/03/16.
  */
object FeedToNews {

  implicit val actorSystem = ActorSystem("FeedToNews")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher
  val kafka = new ReactiveKafka()
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  def main(args: Array[String]) {
    val esHosts = "192.168.99.100:9300"
    val feedsIndexPath = "wtl/feeds"
    val clusterName = "wheretolive"

    val hostname = "wheretolive.it"
    val batchSize = 10

    //Kafka Params
    val kafkaBrokers = "192.168.99.100:9092"
    val topic = "feed_items"

    val client = new ElasticQueryTerms(esHosts, feedsIndexPath, clusterName).client

    val filteredFeeds = feedSource(client, feedsIndexPath)
    val feedArticles = extractArticles(filteredFeeds)

    val feeds = feedArticles.map(_._1)

    saveToElastic(feeds, client, feedsIndexPath, batchSize, 1)

    val articles = feedArticles
      .flatMapConcat(_._2)
      .map(gander.mainContent)

    saveArticlesToKafka(articles, kafka, kafkaBrokers, topic)
  }

  def feedSource(client: ElasticClient, indexPath: String): Source[Feed, NotUsed] = {
    implicit object FeedHitAs extends HitAs[Feed] {
      override def as(hit: RichSearchHit): Feed = {
        parse(hit.getSourceAsString).extract[Feed]
      }
    }

    val publisher: ScrollPublisher = client.publisher(indexPath, keepAlive = "60m")

    Source.fromPublisher(publisher)
      .map(_.as[Feed])
      .filter(_.schedulerData.time.isBeforeNow)
  }

  def extractArticles(feeds: Source[Feed, NotUsed]): Source[(Feed, List[Article]), NotUsed] = {
    feeds.map { f =>
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
}
