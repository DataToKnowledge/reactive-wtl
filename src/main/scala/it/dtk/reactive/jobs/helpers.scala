package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.{Control, Message}
import akka.kafka.scaladsl.Producer
import akka.kafka.scaladsl.Producer.Result
import akka.stream.scaladsl._
import it.dtk.model._
import it.dtk.protobuf._
import it.dtk.reactive.util.KafkaUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.bulk.BulkCompatibleDefinition
import com.sksamuel.elastic4s.searches.SearchDefinition
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.{RequestBuilder, ScrollPublisher}

import scala.language.implicitConversions

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

  def feedSink(client: TcpClient, indexType: String, docType: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {

    implicit val builder = new RequestBuilder[Feed] {
      def request(f: Feed): BulkCompatibleDefinition = {
        indexInto(indexType,docType) id f.publisher source write(f)
      }
    }
    Sink.fromSubscriber(client.subscriber[Feed](batchSize, concurrentRequests))
  }

  def queryTermSink(client: TcpClient, indexType: String, docType: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {
    implicit val builder = new RequestBuilder[QueryTerm] {
      def request(t: QueryTerm): BulkCompatibleDefinition =
        indexInto(indexType, docType) id t.terms.mkString("_") source write(t)
    }

    Sink.fromSubscriber(client.subscriber[QueryTerm](batchSize, concurrentRequests))
  }

  def articleSink(client: TcpClient, indexType: String, docType: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {

    implicit val builder = new RequestBuilder[FlattenedNews] {
      // the request returned doesn't have to be an index - it can be anything supported by the bulk api
      def request(t: FlattenedNews): BulkCompatibleDefinition =
        indexInto(indexType, docType) id t.publisher source write(t)
    }

    Sink.fromSubscriber(client.subscriber[FlattenedNews](batchSize, concurrentRequests))
  }

  def flattenedNewsSink(client: TcpClient, indexType: String, docType: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem) = {
    implicit val builder = new RequestBuilder[FlattenedNews] {
      override def request(n: FlattenedNews): BulkCompatibleDefinition =
        indexInto(indexType, docType) id n.uri source write(n)
    }

    Sink.fromSubscriber(client.subscriber[FlattenedNews](
      batchSize = batchSize,
      concurrentRequests = concurrentRequests,
      errorFn = (ex: Throwable) => println(ex.getLocalizedMessage)
    ))
  }

  def queryTermSource(client: TcpClient, searchDef: SearchDefinition)(implicit system: ActorSystem): Source[QueryTerm, NotUsed] = {
    Source.fromPublisher(client.publisher(searchDef size 10 scroll "10m"))
      .map(res => parse(res.sourceAsString).extract[QueryTerm])
  }

  def feedSource(client: TcpClient, indexPath: String)(implicit system: ActorSystem): Source[Feed, NotUsed] = {
    val publisher: ScrollPublisher = client.publisher(indexPath, keepAlive = "10m")
    Source.fromPublisher(publisher)
      .map(hit => parse(hit.sourceAsString).extract[Feed])
  }

  def googleNewsSource(client: TcpClient, indexPath: String)(implicit system: ActorSystem): Source[GoogleNews, NotUsed] = {
    val publisher: ScrollPublisher = client.publisher(indexPath, keepAlive = "10m")
    Source.fromPublisher(publisher)
      .map(hit => parse(hit.sourceAsString).extract[GoogleNews])
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

object Utils {

  //(implicit log: LoggingAdapter)

  def printArticle(name: String) = Flow[Article]
    .log(name, (a: Article) => s"Processed Article with uri ${a.uri}")

  //    .withAttributes(Attributes.logLevels(onElement = Logging.DebugLevel))

  def printFeed(name: String) = Flow[Feed]
    .log(name, (f: Feed) => s"Extracted Feed with uri ${f.url}")

  //    .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))

  def printFeedArticle(name: String) = Flow[(Feed, List[Article])]
    .log(name, fa => s"extracted ${fa._2.size} articles extracted  from feed ${fa._1.url}")
}
