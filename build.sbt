import com.typesafe.sbt.packager.docker.{ExecCmd, Cmd}

lazy val commons = Seq(
  organization := "it.datatoknowledge",
  name := "reactive-wtl",
  version := "1.3.5",
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq("-target:jvm-1.8", "-feature"),
  resolvers ++= Seq(
    "spray repo" at "http://repo.spray.io",
    Resolver.sonatypeRepo("public"),
    Resolver.typesafeRepo("releases")
  )
)

lazy val root = (project in file("."))
  .enablePlugins(SbtScalariform, DockerPlugin, JavaAppPackaging)
  .settings(commons: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.10.0",
      //      "org.slf4j" % "slf4j-nop" % "1.7.18",
      "com.sksamuel.elastic4s" %% "elastic4s-streams" % "2.2.0",
      "com.typesafe.akka" %% "akka-http-core" % "2.4.3",
      "org.rogach" %% "scallop" % "1.0.0",
      "org.influxdb" % "influxdb-java" % "2.1",
      "redis.clients" % "jedis" % "2.8.1"
    )
  ) dependsOn algocore


lazy val algocore = (project in file("./algocore"))
  .settings(commons: _*)
  .settings(name := "algocore")


maintainer in Docker := "info@datatotknowledge.it"
version in Docker := version.value
dockerBaseImage := "java:8-jre"


//dockerExposedPorts := Seq(9000)
dockerExposedVolumes := Seq("/opt/docker/logs")
dockerRepository := Option("data2knowledge")