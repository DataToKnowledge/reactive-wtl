package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.{ Control, Message }
import akka.kafka.scaladsl.Producer
import akka.kafka.scaladsl.Producer.Result
import akka.stream.scaladsl._
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.{ RequestBuilder, ScrollPublisher }
import it.dtk.model._
import it.dtk.protobuf._
import it.dtk.reactive.util.KafkaUtils
import it.dtk.{ GanderHelper, _ }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.language.implicitConversions

/**
 * Created by fabiofumarola on 08/03/16.
 */
object helpers {
  val web = HttpDownloader
  val feedExtr = RomeFeedHelper
  val tika = TikaHelper
  val gander = GanderHelper
  val html = HtmlHelper
  val terms = QueryTermsSearch
  val kafka = KafkaHelper
  val elastic = ElasticHelper

  implicit def seqToSource[T](seq: Seq[T]): Source[T, NotUsed] =
    Source(seq.toList)

  object KafkaHelper {

    def wrap(topic: String, a: Article) = new ProducerRecord[String, Article](topic, a.uri, a)

    def articleSource(brokers: String, groupId: String, clientId: String, topic: String)(implicit system: ActorSystem): Source[Message[String, Article], Control] = {
      KafkaUtils.atMostOnceSource(brokers, groupId, clientId, topic, new StringDeserializer, new ArticleDes)
    }

    def articleSink(brokers: String, topic: String)(implicit system: ActorSystem): Sink[ProducerRecord[String, Article], NotUsed] = {
      KafkaUtils.plainProducer(brokers, topic, new StringSerializer, new ArticleSer)
    }

    def feedItemsSink(brokers: String, topic: String)(implicit system: ActorSystem): Sink[ProducerRecord[String, Article], NotUsed] =
      articleSink(brokers, topic)

    def articleFlow(brokers: String, topic: String)(implicit system: ActorSystem): Flow[Producer.Message[String, Article, Nothing], Result[String, Article, Nothing], NotUsed] = {
      KafkaUtils.flowProducer(brokers, topic, new StringSerializer, new ArticleSer)
    }

    def feedItemFlow(brokers: String, topic: String)(implicit system: ActorSystem): Flow[Producer.Message[String, Article, Nothing], Result[String, Article, Nothing], NotUsed] = {
      articleFlow(brokers, topic)
    }
  }

  object ElasticHelper {

    implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

    def feedSink(client: ElasticClient, indexPath: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {

      implicit val builder = new RequestBuilder[Feed] {
        def request(t: Feed): BulkCompatibleDefinition =
          index into indexPath id t.publisher source write(t)
      }
      Sink.fromSubscriber(client.subscriber[Feed](batchSize, concurrentRequests))
    }

    def queryTermSink(client: ElasticClient, indexPath: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {
      implicit val builder = new RequestBuilder[QueryTerm] {
        def request(t: QueryTerm): BulkCompatibleDefinition =
          index into indexPath id t.terms.mkString("_") source write(t)
      }

      Sink.fromSubscriber(client.subscriber[QueryTerm](batchSize, concurrentRequests))
    }

    def articleSink(client: ElasticClient, indexPath: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {

      implicit val builder = new RequestBuilder[FlattenedNews] {
        // the request returned doesn't have to be an index - it can be anything supported by the bulk api
        def request(t: FlattenedNews): BulkCompatibleDefinition =
          index into indexPath id t.publisher source write(t)
      }

      Sink.fromSubscriber(client.subscriber[FlattenedNews](batchSize, concurrentRequests))
    }

    def flattenedNewsSink(client: ElasticClient, indexPath: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {
      implicit val builder = new RequestBuilder[FlattenedNews] {
        override def request(n: FlattenedNews): BulkCompatibleDefinition =
          index into indexPath id n.uri source write(n)
      }

      Sink.fromSubscriber(client.subscriber[FlattenedNews](
        batchSize = batchSize,
        concurrentRequests = concurrentRequests,
        errorFn = (ex: Throwable) => println(ex.getLocalizedMessage)
      ))
    }

    def queryTermSource(client: ElasticClient, searchDef: SearchDefinition)(implicit system: ActorSystem): Source[QueryTerm, NotUsed] = {
      implicit object QueryTermsHitAs extends HitAs[QueryTerm] {
        override def as(hit: RichSearchHit): QueryTerm = {
          parse(hit.getSourceAsString).extract[QueryTerm]
        }
      }

      Source.fromPublisher(client.publisher(searchDef size 10 scroll "10m"))
        .map(res => res.as[QueryTerm])
    }

    def feedSource(client: ElasticClient, indexPath: String)(implicit system: ActorSystem): Source[Feed, NotUsed] = {
      val publisher: ScrollPublisher = client.publisher(indexPath, keepAlive = "10m")
      Source.fromPublisher(publisher)
        .map(hit => parse(hit.getSourceAsString).extract[Feed])
    }
  }

  class ArticleSer extends Serializer[Article] {
    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    override def serialize(topic: String, data: Article): Array[Byte] = {
      data.toByteArray()
    }

    override def close(): Unit = {}
  }

  class ArticleDes extends Deserializer[Article] {

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

    override def close(): Unit = {}

    override def deserialize(topic: String, data: Array[Byte]): Article = {
      Article.parseFrom(data)
    }
  }

}

