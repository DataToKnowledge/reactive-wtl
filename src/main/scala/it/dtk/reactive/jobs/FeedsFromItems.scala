package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import com.typesafe.config.ConfigFactory
import it.dtk.NewsUtils
import it.dtk.es._
import it.dtk.model.Feed
import it.dtk.reactive.jobs.ElasticHelper._
import it.dtk.reactive.jobs.KafkaHelper._
import net.ceedubs.ficus.Ficus._

/**
 * Created by fabiofumarola on 04/04/16.
 */
class FeedsFromItems(configFile: String)(implicit
  val system: ActorSystem,
    implicit val mat: ActorMaterializer) {
  val logName = this.getClass.getSimpleName
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
  val consumerGroup = config.as[String]("kafka.groups.feed_from_items")
  val readTopic = config.as[String]("kafka.topics.feed_items")

  val client = ESUtil.elasticClient(esHosts, clusterName)

  def run() {

    val feedsSink = feedSink(client, feedsDocPath, batchSize, parallel)
    val feedsSource = articleSource(kafkaBrokers, consumerGroup, consumerGroup, readTopic)

    feedsSource
      .map(_.value.uri)
      .via(feedFlow())
      .log(logName, f => s"extracted feed ${f.url}")
      .runWith(feedsSink)

  }

  def feedFlow(): Flow[String, Feed, NotUsed] = Flow[String]
    .map(url => NewsUtils.host(url))
    .filterNot(_.contains("comment"))
    .mapAsync(1)(url => NewsUtils.extractRss(url))
    .mapConcat(identity)
    .filterNot(f => f.url.contains("comment") || f.url.contains("calcio"))
}
