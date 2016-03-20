package it.dtk.reactive.util

import java.util.concurrent.TimeUnit

import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.influxdb.InfluxDBFactory
import org.influxdb.dto._

/**
  * Created by fabiofumarola on 20/03/16.
  */
class InfluxDBWrapper(config: Config) {

  val host = config.as[String]("influxdb.host")
  val user = config.as[String]("influxdb.username")
  val pwd = config.as[String]("influxdb.password")
  val dbName = config.as[String]("influxdb.db")

  val dbConn = InfluxDBFactory.connect(host, user, pwd)
  dbConn.createDatabase(dbName)
  dbConn.enableBatch(2000, 100, TimeUnit.MILLISECONDS)

  val retention = "default"


  def write(point: Point): Unit = {
    dbConn.write(dbName, retention, point)
  }

  def write(measure: String, fields: Map[String, Any], tags: Map[String, String]): Unit = {

    val point = Point.measurement(measure)
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)

    fields.foreach(kv => point.field(kv._1, kv._2))
    tags.foreach(kv => point.tag(kv._1, kv._2))

    write(point.build())

  }
}
