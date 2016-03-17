package it.dtk.reactive

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer, Supervision }
import com.softwaremill.react.kafka.ReactiveKafka
import it.dtk.reactive.jobs._
import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val jobs = List("QueryTerms", "Feeds", "TagArticles", "ToElastic", "InitIndex")
  val envs = List("mac", "linux", "docker")

  val jobName = opt[String](descr = s"name of the job to run in $jobs", required = true)
  validate(jobName) { j =>
    if (jobs.contains(j)) Right(Unit)
    else Left(s"Not valid value it should be in $jobs")
  }

  val env = opt[String](descr = s"config env setting in $envs", required = true)
  validate(env) { e =>
    if (envs.contains(e)) Right(Unit)
    else Left(s"Not valid value it should be in $envs")
  }
  verify()
}

/**
 * Created by fabiofumarola on 15/03/16.
 */
object Boot {

  def main(args: Array[String]) {
    val conf = new Conf(args)

    val decider: Supervision.Decider = {
      case ex => Supervision.Resume
    }

    implicit val actorSystem = ActorSystem("ReactiveWtl")
    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)
    )
    implicit val executor = actorSystem.dispatcher
    val kafka = new ReactiveKafka()

    val configFile = selectConfigFile(conf.env.get.get)

    conf.jobName.get match {
      case Some("QueryTerms") => new QueryTermsToNews(configFile, kafka).run()
      case Some("Feeds") => new FeedToNews(configFile, kafka).run()
      case Some("TagArticles") => new TagArticles(configFile, kafka).run()
      case Some("ToElastic") => new ArticlesToElastic(configFile, kafka).run()
      case Some("InitIndex") => new InitIndex(configFile).run()
      case _ =>
        conf.printHelp()
        System.exit(1)
    }
  }

  def selectConfigFile(env: String) = env match {
    case "docker" => "docker_prod.conf"
    case "linux" => "linux_dev.conf"
    case "mac" => "mac_dev.conf"
  }

}
