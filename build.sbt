ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Repli"
  )

libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "4.0.0",
  "com.typesafe.akka" %% "akka-stream" % "2.6.19",
  "com.typesafe" % "config" % "1.4.2",
  "ch.qos.logback" % "logback-classic" % "1.2.10",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.reactivemongo" %% "reactivemongo-akkastream" % "1.1.0-RC6"
)
