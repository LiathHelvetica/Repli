ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Repli"
  )

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % "3.0.1",
  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "4.0.0",
  "com.typesafe.akka" %% "akka-stream" % "2.6.19",
  "com.typesafe" % "config" % "1.4.2",
  "ch.qos.logback" % "logback-classic" % "1.2.10",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "com.typesafe.play" %% "play-json" % "2.9.3",
  "joda-time" % "joda-time" % "2.12.0",
  "org.typelevel" %% "cats-core" % "2.8.0"
  // "org.reactivemongo" %% "reactivemongo" % "1.0.10",
  // "org.reactivemongo" %% "reactivemongo-akkastream" % "1.0.10",
  // "org.reactivemongo" % "reactivemongo-shaded-native" % "1.0.10-linux-x86-64" % "runtime"
)
