package it.dtk.reactive.jobs

import java.net.URL

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticFeeds
import it.dtk.model.Feed
import it.dtk.protobuf.Article
import it.dtk.reactive.jobs.helpers._
import it.dtk.reactive.util.InfluxDBWrapper
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.joda.time.DateTime

/**
  * Created by fabiofumarola on 04/04/16.
  */
class FeedsFromArticles(configFile: String, kafka: ReactiveKafka)(implicit
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

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val consumerGroup = config.as[String]("kafka.groups.feed_from_articles")
  val readTopic = config.as[String]("kafka.topics.feed_items")

  val influxDB = new InfluxDBWrapper(config)

  val client = new ElasticFeeds(esHosts, feedsDocPath, clusterName).client

  def run() {

    val feedsSink = elasticFeedSink(client, feedsDocPath, batchSize, parallel)

    feedItemsSource()
      .map(_.uri)
      .via(feedFlow())
      .map { f => println(s"extracted feed ${f.url}"); f }
      .runWith(feedsSink)

  }

  def feedFlow(): Flow[String, Feed, NotUsed] = Flow[String]
    .map(url => html.host(url))
    .filter(_.nonEmpty)
    .map(_.get)
    .filterNot(_.contains("comment"))
    .flatMapConcat { url =>
      val mapRssTitle = html.findMapRssTitle(url)
      val host = new URL(url).getHost
      Source(mapRssTitle.filterNot(_._1.contains("comment")).map {
        case (rss, title) =>
          val publisher = if (title.isEmpty) host else title
          Feed(rss, publisher, List.empty, Some(DateTime.now()))
      })
    }

  def feedItemsSource(): Source[Article, NotUsed] = {
    val publisher = kafka.consume(ConsumerProperties(
      bootstrapServers = kafkaBrokers,
      topic = readTopic,
      groupId = consumerGroup,
      valueDeserializer = new ByteArrayDeserializer()
    ))

    Source.fromPublisher(publisher)
      .map(rec => Article.parseFrom(rec.value()))
  }
}

