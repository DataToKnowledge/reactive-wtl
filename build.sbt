import com.typesafe.sbt.packager.docker.{ExecCmd, Cmd}

lazy val commons = Seq(
  organization := "it.datatoknowledge",
  name := "reactive-wtl",
  version := "1.6.3",
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
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.11-M2",
      "com.typesafe.akka" %% "akka-stream" % "2.4.4",
      "com.sksamuel.elastic4s" %% "elastic4s-streams" % "2.2.1",
      "com.typesafe.akka" %% "akka-http-core" % "2.4.4",
      "org.rogach" %% "scallop" % "1.0.0",
      "redis.clients" % "jedis" % "2.8.1",
      "com.typesafe.akka" %% "akka-slf4j" % "2.4.4",
      "ch.qos.logback" % "logback-classic" % "1.1.3"
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
