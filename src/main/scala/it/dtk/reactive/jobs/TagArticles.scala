package it.dtk.reactive.jobs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.softwaremill.react.kafka.{ ConsumerProperties, ReactiveKafka }
import com.typesafe.config.ConfigFactory
import it.dtk.nlp.{ DBpediaSpotLight, FocusLocation }
import it.dtk.protobuf.Annotation.DocumentSection
import it.dtk.protobuf._
import it.dtk.reactive.jobs.helpers._
import it.dtk.reactive.util.InfluxDBWrapper
import net.ceedubs.ficus.Ficus._
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.duration._
import scala.language.{ implicitConversions, postfixOps }

/**
 * Created by fabiofumarola on 09/03/16.
 */
class TagArticles(configFile: String, kafka: ReactiveKafka)(implicit
  val system: ActorSystem,
    implicit val mat: ActorMaterializer) {
  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val esHosts = config.as[String]("elastic.hosts")
  val locationsDocPath = config.as[String]("elastic.docs.locations")
  val clusterName = config.as[String]("elastic.clusterName")
  val hostname = config.as[String]("hostname")
  val batchSize = config.as[Int]("elastic.feeds.batch_size")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val consumerGroup = config.as[String]("kafka.groups.tag_articles")
  val readTopic = config.as[String]("kafka.topics.feed_items")
  val writeTopic = config.as[String]("kafka.topics.articles")

  val dbPediaBaseUrl = config.as[String]("dbPedia.it.baseUrl")
  val lang = config.as[String]("dbPedia.it.lang")

  implicit val dbpedia = new DBpediaSpotLight(dbPediaBaseUrl, lang)
  val locExtractor = new FocusLocation(esHosts, locationsDocPath, clusterName)

  val inlufxDB = new InfluxDBWrapper(config)

  def run() {
    val taggedArticles = feedItemsSource()
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
    }.map { a =>
      println(s"extracted annotations and focus location for article ${a.uri}")

      inlufxDB.write(
        "TagArticles",
        Map("url" -> a.uri, "annotations" -> a.annotations.size, "location" -> a.focusLocation.isDefined),
        Map("publisher" -> a.publisher)
      )
      a
    }

    saveArticlesToKafka(focusLocationArticles, kafka, kafkaBrokers, writeTopic).run()

    system.registerOnTermination({
      dbpedia.close()
    })
  }

  def feedItemsSource(): Source[Article, NotUsed] = {
    val publisher = kafka.consume(ConsumerProperties(
      bootstrapServers = kafkaBrokers,
      topic = readTopic,
      groupId = consumerGroup,
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
