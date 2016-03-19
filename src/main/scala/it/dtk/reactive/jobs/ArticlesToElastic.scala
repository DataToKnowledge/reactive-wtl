package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.sksamuel.elastic4s.{ BulkCompatibleDefinition, ElasticClient, ElasticDsl }
import com.softwaremill.react.kafka.{ ConsumerProperties, ReactiveKafka }
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticQueryTerms
import it.dtk.model.News
import it.dtk.protobuf._
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import ElasticDsl._

/**
 * Created by fabiofumarola on 10/03/16.
 */
class ArticlesToElastic(configFile: String, kafka: ReactiveKafka)(implicit
  val system: ActorSystem,
    implicit val mat: ActorMaterializer) {

  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val feedsIndexPath = config.as[String]("elastic.docs.articles")
  val clusterName = config.as[String]("elastic.clusterName")

  val hostname = config.as[String]("hostname")
  val batchSize = config.as[Int]("elastic.bulk.batch_size")
  val parallel = config.as[Int]("elastic.bulk.parallel")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val readTopic = config.as[String]("kafka.topics.articles")
  val groupId = config.as[String]("kafka.groups.articles_es")

  val client = new ElasticQueryTerms(esHosts, feedsIndexPath, clusterName).client

  def run() {
    val articles = articleSource()

    var counter = 1

    val mappedArticles = articles
      .map { a =>
        News(
          uri = a.uri,
          title = a.title,
          description = a.description,
          categories = a.categories.filter(_.length > 2),
          keywords = a.keywords.filter(_.length > 2),
          imageUrl = a.imageUrl,
          publisher = a.publisher,
          date = new DateTime(a.date),
          lang = a.lang,
          text = a.cleanedText,
          annotations = a.annotations,
          focusLocation = a.focusLocation
        )
      }
      .map { n =>
        println(s" $counter savig news ${n.uri}")
        counter += 1
        n
      }
      .to(elasticSink()).run()
  }

  def articleSource(): Source[Article, NotUsed] = {

    val publisher = kafka.consume(ConsumerProperties(
      bootstrapServers = kafkaBrokers,
      topic = readTopic,
      groupId = groupId,
      valueDeserializer = new ByteArrayDeserializer()
    ))

    Source.fromPublisher(publisher)
      .map(rec => Article.parseFrom(rec.value()))
  }

  def elasticSink(): Sink[News, NotUsed] = {
    implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

    implicit val builder = new RequestBuilder[News] {
      override def request(n: News): BulkCompatibleDefinition =
        index into feedsIndexPath id n.uri source write(n)
    }

    val elasticSink = client.subscriber[News](
      batchSize = batchSize,
      concurrentRequests = parallel,
      completionFn = () => println("all done")
    )
    Sink.fromSubscriber(elasticSink)
  }

}
