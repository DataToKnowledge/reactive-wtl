package it.dtk.reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializerSettings, Supervision, ActorMaterializer}
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.sksamuel.elastic4s.{BulkCompatibleDefinition, ElasticClient, ElasticDsl}
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import it.dtk.es.ElasticQueryTerms
import it.dtk.protobuf._
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import com.sksamuel.elastic4s.streams.ReactiveElastic._

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

    //Kafka Params
    val kafkaBrokers = "192.168.99.100:9092"
    val topic = "articles"
    val groupId = "articleToElastic"

    val esHosts = "192.168.99.100:9300"
    val feedsIndexPath = "wtl/articles"
    val clusterName = "wheretolive"
    val batchSize = 100
    val client = new ElasticQueryTerms(esHosts, feedsIndexPath, clusterName).client

    val articles = articleSource(kafkaBrokers, topic, groupId)
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
