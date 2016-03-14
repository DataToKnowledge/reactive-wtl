package it.dtk.reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.sksamuel.elastic4s.{BulkCompatibleDefinition, ElasticClient, ElasticDsl}
import com.softwaremill.react.kafka.{ProducerMessage, ProducerProperties, ReactiveKafka}
import it.dtk._
import it.dtk.model.Feed
import it.dtk.protobuf._
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._
import org.reactivestreams.Subscriber

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

  def saveArticlesToKafkaProtobuf(articles: Source[Article, NotUsed], kafka: ReactiveKafka, kafkaBrokers: String, topic: String)(implicit system: ActorSystem): Unit = {

    implicit val materializer = ActorMaterializer()

    val kafkaSink: Subscriber[ProducerMessage[Array[Byte], Array[Byte]]] =
      kafka.publish(ProducerProperties(
        bootstrapServers = kafkaBrokers,
        topic = topic,
        valueSerializer = new ByteArraySerializer()
      ))

    articles.map { a =>
      ProducerMessage(a.uri.getBytes, a.toByteArray())
    }.to(Sink.fromSubscriber(kafkaSink)).run()
  }

  def saveToElastic(feeds: Source[Feed, NotUsed], client: ElasticClient, indexPath: String,
                    batchSize: Int, concurrentRequests: Int)(implicit system: ActorSystem): Unit = {
    implicit val builder = new RequestBuilder[Feed] {

      import ElasticDsl._

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

    feeds.runWith(Sink.fromSubscriber(elasticSink))
  }

  def selectConfig(args: Array[String]): String = {

    if (args.isEmpty) {
      println("run: docker_prod|linux_dev|mac_dev")
      System.exit(1)
    }

    args(0) match {
      case "docker_prod" => "docker_prod.conf"
      case "linux_dev" => "linux_dev.conf"
      case "mac_dev" => "mac_dev.conf"
      case x =>
        println(s"value $x not valid")
        System.exit(1)
        x
    }

  }
}
