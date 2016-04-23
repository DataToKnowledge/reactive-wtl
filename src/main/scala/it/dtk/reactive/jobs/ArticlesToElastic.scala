package it.dtk.reactive.jobs

import java.net.URL

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.sksamuel.elastic4s.BulkCompatibleDefinition
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.RequestBuilder
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticQueryTerms
import it.dtk.model.{FlattenedNews, SemanticTag}
import it.dtk.protobuf._
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.joda.time.DateTime
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.Try

/**
  * Created by fabiofumarola on 10/03/16.
  */
class ArticlesToElastic(configFile: String, kafka: ReactiveKafka)(implicit val system: ActorSystem,
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

    articles
      .map { a =>
        val annotations = convertAnnotations(a.annotations)

        val n = FlattenedNews(
          uri = a.uri,
          title = a.title,
          description = a.description,
          categories = a.categories.filter(_.length > 2),
          keywords = a.keywords.filter(_.length > 2),
          imageUrl = a.imageUrl,
          publisher = cleanPublisher(a.publisher),
          date = new DateTime(a.date),
          lang = a.lang,
          text = a.cleanedText,
          cityName = a.focusLocation.map(_.cityName).getOrElse(""),
          provinceName = a.focusLocation.map(_.provinceName).getOrElse(""),
          regionName = a.focusLocation.map(_.regionName).getOrElse(""),
          annotations = annotations,
          crimes = filterCrimes(annotations),
          locations = filterLocations(annotations),
          persons = Seq(),
          semanticNames = annotations.map(_.name).distinct,
          semanticTags = annotations.
            flatMap(_.tags.map(_.replace("_", " "))).distinct,
          pin = a.focusLocation.map(_.pin))
        n
      }.map { n =>
      println(s" $counter saving news ${n.uri}")
      counter +=1
      n
    }.to(elasticSink()).run()
  }

  def cleanPublisher(publisher: String): String = {
    publisher match {
      case value if value.startsWith("http") =>
        Try(new URL(publisher).getHost).getOrElse(value)

      case value => value
    }
  }

  val tagRegex = "^Q\\d*".r

  /**
    *
    * @param annotations
    * @return all the annotations where
    *         1. tags with length > 2, and does not match with Q\d*
    *         2. name with length > 2
    */
  def convertAnnotations(annotations: Seq[Annotation]): Seq[SemanticTag] = {
    annotations.groupBy(_.surfaceForm.toLowerCase).map {
      case (name, list) =>

        val cleanedTags = list.
          flatMap(_.types).
          map(_.value).
          toSet.
          filter(_.length > 2).
          filter(t => tagRegex.findFirstIn(t).isEmpty)

        SemanticTag(
          name = name.capitalize,
          wikipediaUrl = list.head.wikipediaUrl,
          tags = cleanedTags,
          pin = list.head.pin,
          support = list.map(_.support).sum / list.length)
    }.filter(_.name.length > 2)
      .toSeq
  }

  def filterCrimes(list: Seq[SemanticTag]): Seq[String] = {
    list.filter(_.tags.contains("Crime"))
      .map(_.name)
  }

  def filterLocations(list: Seq[SemanticTag]): Seq[String] = {
    list.filter(_.tags.contains("PopulatedPlace"))
      .map(_.name)
  }

  def filterPerson(list: Seq[SemanticTag]): Seq[String] = {
    list.filter(_.tags.contains("NaturalPerson"))
      .map(_.name)
  }

  def articleSource(): Source[Article, NotUsed] = {

    val publisher = kafka.consume(ConsumerProperties(
      bootstrapServers = kafkaBrokers,
      topic = readTopic,
      groupId = groupId,
      valueDeserializer = new ByteArrayDeserializer()))

    Source.fromPublisher(publisher)
      .map(rec => Article.parseFrom(rec.value()))
  }

  def elasticSink(): Sink[FlattenedNews, NotUsed] = {
    implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

    implicit val builder = new RequestBuilder[FlattenedNews] {
      override def request(n: FlattenedNews): BulkCompatibleDefinition =
        index into feedsIndexPath id n.uri source write(n)
    }

    val elasticSink = client.subscriber[FlattenedNews](
      batchSize = batchSize,
      concurrentRequests = parallel,
      completionFn = () => println("all done"),
      errorFn = (ex: Throwable) => println(ex.getLocalizedMessage))
    Sink.fromSubscriber(elasticSink)
  }
}
