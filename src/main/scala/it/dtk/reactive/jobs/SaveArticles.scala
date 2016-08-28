package it.dtk.reactive.jobs

import java.io.FileOutputStream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._

import scala.concurrent.Future
import scala.util.{Failure, Success}
import better.files._
import java.io.{File => JFile}

import it.dtk.protobuf.Article

/**
  * Created by fabiofumarola on 29/05/16.
  */
class SaveArticles(configFile: String)(implicit val system: ActorSystem, implicit val mat: ActorMaterializer) {
  val logName = this.getClass.getSimpleName

  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Kafka Params
  val kafkaBrokers = config.as[String]("kafka.brokers")
  val readTopic = config.as[String]("kafka.topics.articles")
  val groupId = config.as[String]("kafka.groups.save_articles")

  var counter = 0

  def run() {
    val articlesSource = KafkaHelper.articleSource(kafkaBrokers, groupId, groupId, readTopic)
    val file = better.files.File.root/"opt"/"docker"/"backup"/"articles.log"

    val out = validFile().newOutputStream

    val future: Future[Done] = articlesSource
      .map(_.value)
      .log(logName, a => s"saving article with url ${a.uri}")
      .map(a => a.writeDelimitedTo(out))
      .runWith(Sink.ignore)
    //parseDelimitedFrom

    import scala.concurrent.ExecutionContext.Implicits.global
    future onComplete {
      case Success(value) => out.close()
      case Failure(ex) =>
        ex.printStackTrace()
        out.close()
    }
  }

  def validFile(): File = {
    def loadFile = better.files.File.root/"opt"/"docker"/"backup"/ s"articles_${counter}.log"
    var file = loadFile
    if (file.exists){
      counter +=1
      file = loadFile
    }
    file
  }
}
