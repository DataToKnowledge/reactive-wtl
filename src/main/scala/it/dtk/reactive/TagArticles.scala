package it.dtk.reactive

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import it.dtk.nlp.DBpediaSpotLight
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s.NoTypeHints
import org.json4s.ext.JodaTimeSerializers
import org.json4s.jackson.Serialization
import it.dtk.model._
import helpers._
import org.json4s.jackson.JsonMethods._
import org.json4s._
import scala.concurrent.duration._

import scala.language.implicitConversions

/**
  * Created by fabiofumarola on 09/03/16.
  */
object TagArticles {
  implicit val actorSystem = ActorSystem("QueryTermsToNews")
  implicit val materializer = ActorMaterializer()
  implicit val executor = actorSystem.dispatcher
  val kafka = new ReactiveKafka()
  implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all

  def main(args: Array[String]) {
    val esHosts = "192.168.99.100:9300"
    val locDocPath = "wtl/locations"
    val clusterName = "wheretolive"

    val hostname = "wheretolive.it"
    val batchSize = 10

    //Kafka Params
    val kafkaBrokers = "192.168.99.100:9092"
    val topic = "feed_items"
    val grouId = "articlesTagger"

    val dbPediaBaseUrl = "http://192.168.99.100:2230"
    val lang = "it"
    val dbpedia = new DBpediaSpotLight(dbPediaBaseUrl, lang)


    val source = feedItemsSource(kafkaBrokers, topic, grouId)

    val taggedArticles = source
      .groupedWithin(50, 20 seconds)
      .map(_.toSet)
      .map(a => annotateArticle)
      .map { a =>
        val enrichment = a.annotations.map(ann => dbpedia.enrichAnnotation(ann))
        a.copy(annotations = enrichment)
      }



    actorSystem.registerOnTermination({
      dbpedia.close()
    })
  }

  def feedItemsSource(brokers: String, topic: String, groupId: String): Source[Article, NotUsed] = {
    val publisher = kafka.consume(ConsumerProperties(
      bootstrapServers = brokers,
      topic = topic,
      groupId = groupId,
      valueDeserializer = new StringDeserializer()
    ))

    Source.fromPublisher(publisher)
      .map(rec => parse(rec.value()).extract[Article])
  }


  def annotateArticle(implicit dbpedia: DBpediaSpotLight, a: Article): Article = {

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
