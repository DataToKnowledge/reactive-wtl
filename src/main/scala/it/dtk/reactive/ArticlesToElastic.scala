package it.dtk.reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Supervision }
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.sksamuel.elastic4s.{ BulkCompatibleDefinition, ElasticClient, ElasticDsl }
import com.softwaremill.react.kafka.{ ConsumerProperties, ReactiveKafka }
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticQueryTerms
import it.dtk.protobuf._
import it.dtk.reactive.helpers._
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

/**
 * Created by fabiofumarola on 10/03/16.
 */
object ArticlesToElastic {

  implicit val actorSystem = ActorSystem("FeedToNews")

  val decider: Supervision.Decider = {
    case ex =>
      //TODO add methods to log all this errors
      Supervision.Resume
  }

  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)
  )

  implicit val executor = actorSystem.dispatcher
  val kafka = new ReactiveKafka()
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  def main(args: Array[String]) {

    val config = ConfigFactory.load(selectConfig(args)).getConfig("reactive_wtl")

    //Elasticsearch Params
    val esHosts = config.as[String]("elastic.hosts")
    val feedsIndexPath = config.as[String]("elastic.docs.articles")
    val clusterName = config.as[String]("elastic.clusterName")

    val hostname = config.as[String]("hostname")
    val batchSize = config.as[Int]("elastic.bulk.batch_size")

    //Kafka Params
    val kafkaBrokers = config.as[String]("kafka.brokers")
    val readTopic = config.as[String]("kakfa.topics.articles")
    val groupId = config.as[String]("kafka.groups.articlesEs")

    val client = new ElasticQueryTerms(esHosts, feedsIndexPath, clusterName).client

    val articles = articleSource(kafkaBrokers, readTopic, groupId)
    //do transformation
    saveToElastic(articles, client, feedsIndexPath, batchSize, 2)

  }

  def articleSource(brokers: String, topic: String, groupId: String): Source[Article, NotUsed] = {

    val publisher = kafka.consume(ConsumerProperties(
      bootstrapServers = brokers,
      topic = topic,
      groupId = groupId,
      valueDeserializer = new ByteArrayDeserializer()
    ))

    Source.fromPublisher(publisher)
      .map(rec => Article.parseFrom(rec.value()))
  }

  def saveToElastic(articles: Source[Article, NotUsed], client: ElasticClient,
    indexPath: String, batchSize: Int, concurrentReqs: Int): Unit = {

    implicit val builder = new RequestBuilder[Article] {

      import ElasticDsl._

      override def request(a: Article): BulkCompatibleDefinition =
        index into indexPath id a.uri source write(a)
    }

    val elasticSink = client.subscriber[Article](
      batchSize = batchSize,
      concurrentRequests = concurrentReqs,
      completionFn = () => println("all done")
    )

    articles.runWith(Sink.fromSubscriber(elasticSink))
  }
}
