package it.dtk.reactive.crimes

import com.typesafe.config.ConfigFactory
import it.dtk.es.ElasticAdmin
import net.ceedubs.ficus.Ficus._

/**
 * Created by fabiofumarola on 17/03/16.
 */
class InitIndex(configFile: String) {

  val config = ConfigFactory.load(configFile).getConfig("reactive_wtl")

  //Elasticsearch Params
  val adminHost = config.as[String]("elastic.adminHost")
  val esHosts = config.as[String]("elastic.hosts")
  val locationsPath = config.as[String]("elastic.docs.locations")
  val clusterName = config.as[String]("elastic.clusterName")

  def run(): Unit = {
    val admin = new ElasticAdmin(adminHost, esHosts, locationsPath, clusterName)
    admin.initWhereToLive()
    System.exit(0)
  }
}
