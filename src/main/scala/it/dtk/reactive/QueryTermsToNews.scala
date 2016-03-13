package it.dtk.reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.softwaremill.react.kafka.ReactiveKafka
import it.dtk.es.ElasticQueryTerms
import it.dtk.model._
import it.dtk.reactive.helpers._
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.language.implicitConversions


/**
  * Created by fabiofumarola on 08/03/16.
  */
object QueryTermsToNews {

  implicit val actorSystem = ActorSystem("QueryTermsToNews")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher
  val kafka = new ReactiveKafka()
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  def main(args: Array[String]) {
    //Elasticsearch Params
    val esHosts = "192.168.99.100:9300"
    val esIndexPath = "wtl/query_terms"
    val clusterName = "wheretolive"
    val hostname = "wheretolive.it"

    val feedsIndexPath = "wtl/feeds"
    val batchSize = 5

    //Kafka Params
    val kafkaBrokers = "192.168.99.100:9092"
    val topic = "feed_items"


    val client = new ElasticQueryTerms(esHosts, esIndexPath, clusterName).client

    val toCheckQueries = queryTermSource(client, esIndexPath)
    val articles = toCheckQueries.via(extractArticles(hostname))

    saveArticlesToKafka(articles, kafka, kafkaBrokers, topic)

    val feeds = articles
      .map(a => a.publisher -> html.host(a.uri))
      .filter(_._2.nonEmpty)
      .map(f => (f._1, f._2.get))
      .filterNot(_._2.contains("comment"))
      .flatMapConcat {
        case (newsPublisher, url) =>
          val rss = html.findRss(url)
          Source(rss.filterNot(_.contains("comment")).map(url =>
            Feed(url, newsPublisher, List.empty, Some(DateTime.now()))
          ))
      }

    saveToElastic(feeds, client, feedsIndexPath, batchSize, 2)
    feeds.runWith(Sink.foreach(println))

  }

  def queryTermSource(client: ElasticClient, indexPath: String): Source[QueryTerm, NotUsed] = {
    implicit object QueryTermsHitAs extends HitAs[QueryTerm] {
      override def as(hit: RichSearchHit): QueryTerm = {
        parse(hit.getSourceAsString).extract[QueryTerm]
      }
    }

    val publisher: ScrollPublisher = client.publisher(indexPath, keepAlive = "1m")

    val toCheckQueries = Source.fromPublisher(publisher)
      .map(res => res.as[QueryTerm])
      .filter(_.timestamp.getOrElse(DateTime.now.minusMinutes(10)).isBeforeNow)

    toCheckQueries
  }

  def extractArticles(hostname: String) = Flow[QueryTerm]
    .flatMapConcat(q => terms.generateUrls(q.terms, q.lang, hostname))
    .flatMapConcat(u => terms.getResultsAsArticles(u))
    .map(gander.mainContent)

}
