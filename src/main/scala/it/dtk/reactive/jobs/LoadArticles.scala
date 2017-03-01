package it.dtk.reactive.jobs

import java.net.URL

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import better.files.File
import com.typesafe.config.ConfigFactory
import it.dtk.es.ESUtil
import it.dtk.model.{ FlattenedNews, SemanticTag }
import it.dtk.protobuf.{ Annotation, Article }
import org.joda.time.DateTime
import net.ceedubs.ficus.Ficus._

import scala.util.Try

class LoadArticles(configFile: String)(implicit val system: ActorSystem, implicit val mat: ActorMaterializer) {
  val logName = this.getClass.getSimpleName

  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  val esHosts = config.as[String]("elastic.hosts")
  val indexType = config.as[String]("elastic.docs.news_index")
  val articleType = config.as[String]("elastic.docs.articles")
  val clusterName = config.as[String]("elastic.clusterName")
  val batchSize = config.as[Int]("elastic.bulk.batch_size")
  val client = ESUtil.elasticClient(esHosts, clusterName)

  def run(): Unit = {

    val articlesSink = ElasticHelper.flattenedNewsSink(client, indexType, articleType, batchSize, 3)

    val source = Source.fromIterator(() => new Iterator[Article] {
      println("started the iterator")
      val file = File(config.getString("load_articles_path"))
      val in = file.newInputStream
      var article = Article.parseDelimitedFrom(in)

      override def hasNext: Boolean = article.nonEmpty

      override def next(): Article = {
        val current = article.get
        article = Article.parseDelimitedFrom(in)
        current
      }

      override def finalize(): Unit = {
        in.close()
        super.finalize()
      }
    })

    var counter = 0

    val graph = source
      .map(convertTo)
      .log(logName, a => {
        counter += 1
        s"Sending to ElasticSearch Article $counter with url ${a.uri}"
      })
      //      .to(Sink.foreach(println))
      .to(articlesSink)

    graph.run()
  }

  def convertTo(a: Article): FlattenedNews = {
    val annotations = convertAnnotations(a.annotations)

    FlattenedNews(
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
      persons = filterPerson(annotations),
      semanticNames = annotations.map(_.name).distinct,
      semanticTags = Seq(),
      //        annotations.flatMap(_.tags.map(_.replace("_", " "))).distinct,
      pin = a.focusLocation.map(_.pin)
    )
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
          support = list.map(_.support).sum / list.length
        )
    }.filter(_.name.length > 2)
      .toSeq
  }

  def cleanPublisher(publisher: String): String = {
    publisher match {
      case value if value.startsWith("http") =>
        Try(new URL(publisher).getHost).getOrElse(value)

      case value => value
    }
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

}
