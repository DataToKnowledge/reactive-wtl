package it.dtk.reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import it.dtk.nlp.{DBpediaSpotLight, FocusLocation}
import it.dtk.protobuf.Annotation.DocumentSection
import it.dtk.protobuf._
import it.dtk.reactive.helpers._
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization

import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

/**
  * Created by fabiofumarola on 09/03/16.
  */
object TagArticles {

  val decider: Supervision.Decider = {
    case ex =>
      //TODO add methods to log all this errors
      Supervision.Resume
  }

  implicit val actorSystem = ActorSystem("TagArticles")
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
    val locationsIndexPath = config.as[String]("elastic.docs.locations")
    val clusterName = config.as[String]("elastic.clusterName")
    val hostname = config.as[String]("hostname")
    val batchSize = config.as[Int]("elastic.feeds.batch_size")

    //Kafka Params
    val kafkaBrokers = config.as[String]("kafka.brokers")
    val readTopic = config.as[String]("kafka.topics.feeds")
    val writeTopic = config.as[String]("kafka.topics.articles")
    val groupId = config.as[String]("kafka.groups.tagArticles")

    val dbPediaBaseUrl = config.as[String]("dbPedia.it.baseUrl")
    val lang = config.as[String]("dbPedia.it.lang")

    implicit val dbpedia = new DBpediaSpotLight(dbPediaBaseUrl, lang)
    val locExtractor = new FocusLocation(esHosts, locationsIndexPath, clusterName)

    val source = feedItemsSource(kafkaBrokers, readTopic, groupId)

    val taggedArticles = source
      .groupedWithin(50, 20 seconds)
      .flatMapConcat(s => Source(s.toSet))
      .map(annotateArticle)
      .map { a =>
        val enrichment = a.annotations.map(ann => dbpedia.enrichAnnotation(ann))
        a.copy(annotations = enrichment)
      }

    val focusLocationArticles = taggedArticles.map { a =>
      val location = locExtractor.findMainLocation(a)
      a.copy(focusLocation = location)
    }

    saveArticlesToKafkaProtobuf(focusLocationArticles, kafka, kafkaBrokers, writeTopic)

    actorSystem.registerOnTermination({
      dbpedia.close()
    })
  }

  def feedItemsSource(brokers: String, topic: String, groupId: String): Source[Article, NotUsed] = {

    val publisher = kafka.consume(ConsumerProperties(
      bootstrapServers = brokers,
      topic = topic,
      groupId = groupId,
      valueDeserializer = new ByteArrayDeserializer()
    ))

    Source.fromPublisher(publisher)
      .map(rec => Article.parseFrom(rec.value()))
  }

  def annotateArticle(a: Article)(implicit dbpedia: DBpediaSpotLight): Article = {

    val titleAn = if (a.title.nonEmpty)
      dbpedia.annotateText(a.title, DocumentSection.Title)
    else Seq.empty[Annotation]

    val descrAn = if (a.description.nonEmpty)
      dbpedia.annotateText(a.description, DocumentSection.Summary)
    else Seq.empty[Annotation]

    val textAn = if (a.cleanedText.nonEmpty)
      dbpedia.annotateText(a.cleanedText, DocumentSection.Corpus)
    else Seq.empty[Annotation]

    val mergedCatKey = (a.categories ++ a.keywords).mkString(" ")

    val keywordAn = if (mergedCatKey.nonEmpty)
      dbpedia.annotateText(mergedCatKey, DocumentSection.KeyWords)
    else Seq.empty[Annotation]

    val annotations = titleAn ++ descrAn ++ textAn ++ keywordAn
    a.copy(annotations = annotations.toList)
  }

}
