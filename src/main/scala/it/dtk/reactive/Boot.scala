package it.dtk.reactive

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.config.ConfigFactory
import it.dtk.reactive.jobs._
import org.rogach.scallop._
import org.slf4j.LoggerFactory

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val jobs = List("ProcessFeeds", "GoogleNews", "TagArticles",
    "ToElastic", "InitIndex", "FeedsFromItems")
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
    val config = ConfigFactory.load("reactive_wtl")
    implicit val actorSystem = ActorSystem("ReactiveWtl", config)

    val log = Logging(actorSystem, this.getClass)

    val decider: Supervision.Decider = {
      case ex =>
        log.error(ex, "Error at decider")
        Supervision.Restart
    }

    implicit val materializer = ActorMaterializer(
      ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)
    )
    implicit val executor = actorSystem.dispatcher

    val configFile = selectConfigFile(conf.env.get.get)

    conf.jobName.get match {
      case Some("GoogleNews") => new GoogleNewsSearch(configFile).run()
      case Some("ProcessFeeds") => new ProcessFeeds(configFile).run()
      case Some("TagArticles") => new TagArticles(configFile).run()
      case Some("ToElastic") => new ArticlesToElastic(configFile).run()
      case Some("InitIndex") => new InitIndex(configFile).run()
      case Some("FeedsFromItems") => new FeedsFromItems(configFile).run()
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
