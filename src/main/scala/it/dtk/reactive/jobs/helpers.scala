package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.sksamuel.elastic4s._
import com.softwaremill.react.kafka.{ ProducerMessage, ProducerProperties, ReactiveKafka }
import it.dtk.{ GanderHelper, _ }
import it.dtk.model._
import it.dtk.protobuf._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.reactivestreams.Subscriber
import ElasticDsl._

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
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  implicit def seqToSource[T](seq: Seq[T]): Source[T, NotUsed] =
    Source(seq.toList)

  def saveArticlesToKafka(articles: Source[Article, NotUsed], kafka: ReactiveKafka, kafkaBrokers: String, topic: String)(implicit system: ActorSystem, materializer: ActorMaterializer): RunnableGraph[NotUsed] = {

    val kafkaSink: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] =
      kafka.publish(ProducerProperties(
        bootstrapServers = kafkaBrokers,
        topic = topic,
        valueSerializer = new ByteArraySerializer()
      ))

    articles
      .map { a =>
        println(s"tagged article with uri ${a.uri}")
        a
      }
      .map { a =>
        ProducerMessage(a.uri.getBytes, a.toByteArray())
      }.to(Sink.fromSubscriber(kafkaSink))
  }

  def saveToElastic(feeds: Source[Feed, NotUsed], client: ElasticClient, indexPath: String,
    batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem): RunnableGraph[NotUsed] = {
    implicit val builder = new RequestBuilder[Feed] {
      // the request returned doesn't have to be an index - it can be anything supported by the bulk api
      def request(t: Feed): BulkCompatibleDefinition =
        index into indexPath id t.publisher source write(t)
    }

    implicit val materializer = ActorMaterializer()

    val elasticSink = client.subscriber[Feed](
      batchSize = batchSize,
      concurrentRequests = concurrentRequests,
      completionFn = () => println("all done")
    )

    feeds.to(Sink.fromSubscriber(elasticSink))
  }

  def elasticFeedSink(client: ElasticClient, indexPath: String, batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem): Sink[Feed, NotUsed] = {
    implicit val builder = new RequestBuilder[Feed] {
      // the request returned doesn't have to be an index - it can be anything supported by the bulk api
      def request(t: Feed): BulkCompatibleDefinition =
        index into indexPath id t.publisher source write(t)
    }

    val elasticSink = client.subscriber[Feed](
      batchSize = batchSize,
      concurrentRequests = concurrentRequests,
      completionFn = () => println("all done")
    )

    Sink.fromSubscriber(elasticSink)
  }

  //  def saveToElastic(terms: Source[QueryTerm, NotUsed], client: ElasticClient, indexPath: String,
  //                    batchSize: Int, concurrentRequests: Int)
  //                   (implicit system: ActorSystem, materializer: ActorMaterializer): RunnableGraph[NotUsed] = {
  //
  //    implicit val builder = new RequestBuilder[QueryTerm] {
  //      def request(t: QueryTerm): BulkCompatibleDefinition =
  //        index into indexPath id t.terms.mkString("_") source write(t)
  //    }
  //
  //    val elasticSink = client.subscriber[QueryTerm](
  //      batchSize = batchSize,
  //      concurrentRequests = concurrentRequests,
  //      completionFn = () => println("all done")
  //    )
  //
  //    terms.to(Sink.fromSubscriber(elasticSink))
  //  }
}
