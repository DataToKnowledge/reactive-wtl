
lazy val commons = Seq(
  organization := "it.datatoknowledge",
  name := "reactive-wtl",
  version := "0.1.0",
  scalaVersion := "2.11.7",
  scalacOptions ++= Seq("-target:jvm-1.8", "-feature"),
  resolvers ++= Seq(
    "spray repo" at "http://repo.spray.io",
    Resolver.sonatypeRepo("public"),
    Resolver.typesafeRepo("releases")
  )
)

lazy val root = (project in file("."))
  .enablePlugins(SbtScalariform)
  .settings(commons: _*)
  .settings(
    name := "spark-jobs",
    libraryDependencies ++= Seq(
      "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.10.0",
//      "org.slf4j" % "slf4j-nop" % "1.7.18",
      "com.sksamuel.elastic4s" %% "elastic4s-streams" % "2.2.0",
      "com.typesafe.akka" %% "akka-http-core" % "2.4.2"
    )
  ) dependsOn algocore
lazy val algocore = (project in file("./algocore"))
  .settings(commons: _*)
  .settings(name := "algocore")
