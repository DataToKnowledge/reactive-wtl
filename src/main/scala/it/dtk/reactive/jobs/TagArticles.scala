package it.dtk.reactive.jobs

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import it.dtk.nlp.{ DBpediaSpotLight, FocusLocation }
import it.dtk.protobuf.Annotation.DocumentSection
import it.dtk.protobuf._
import net.ceedubs.ficus.Ficus._
import redis.clients.jedis.Jedis
import scala.language.{ implicitConversions, postfixOps }
import KafkaHelper._

/**
 * Created by fabiofumarola on 09/03/16.
 */
class TagArticles(configFile: String)(implicit
  val system: ActorSystem,
    implicit val mat: ActorMaterializer) {
  val logName = this.getClass.getSimpleName
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

  //redis params
  val redisHost = config.as[String]("redis.host")
  val redisDB = config.as[Int]("redis.tagArticles.db")
  val jedis = new Jedis(redisHost)
  jedis.select(redisDB)

  def run() {

    val result = dbpedia.annotateText("Arrestati presso i carabinieri del comune di ostuni", DocumentSection.Title)

    if (result.isEmpty) {
      println("Cannot get tags for text:Arrestati presso i carabinieri del comune di ostuni")
      println("stopping everything")
      System.exit(2)
    } else {
      println("successfully passed the base test with results")
      println(result)
    }

    val feedItemsSource = articleSource(kafkaBrokers, consumerGroup, consumerGroup, readTopic)
    val articlesSink = articleSink(kafkaBrokers, writeTopic)

    val taggedArticles = feedItemsSource
      .log(logName, m => s"Processing article with url ${m.key}")
      .map(_.value)
      .filterNot(a => duplicatedUrl(a.uri))
      .map(annotateArticle)
      .map { a =>
        val enrichment = a.annotations.map(ann => dbpedia.enrichAnnotation(ann))
        a.copy(annotations = enrichment)
      }

    taggedArticles.map { a =>
      val location = locExtractor.findMainLocation(a)
      a.copy(focusLocation = location)
    }.log(logName, a => s"extracted annotations and focus location for article ${a.uri}")
      .map(a => wrap(writeTopic, a))
      .log(logName, m => s"sending to Kafka processed article with url ${m.key()}")
      .runWith(articlesSink)

    system.registerOnTermination({
      dbpedia.close()
    })
  }

  def duplicatedUrl(uri: String): Boolean = {
    val found = Option(jedis.get(uri))

    if (found.isEmpty) jedis.set(uri, "1")
    else jedis.incr(uri)

    found.isDefined
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
