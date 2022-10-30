package lthv

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.exporter.FromMongoToKafkaExporter
import lthv.utils.ConfigHelpers.getCharArrayProperty
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getStringProperty
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoClientSettings
import org.mongodb.scala.MongoCredential
import org.mongodb.scala.ServerAddress
import org.mongodb.scala.connection.ClusterSettings

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Success

object MainExport extends App {

  implicit val conf: Config = ConfigFactory.load("export")
  implicit val actorSystem: ActorSystem = ActorSystem()

  val credentials: MongoCredential = MongoCredential.createCredential(
    getStringProperty("repli.exporter.target.user"),
    getStringPropertyWithFallback("repli.exporter.target.authDb"),
    getCharArrayProperty("repli.exporter.target.password")
  )
  val server: ServerAddress = ServerAddress(
    getStringPropertyWithFallback("repli.exporter.target.host"),
    getIntPropertyWithFallback("repli.exporter.target.port")
  )

  val client = MongoClient(
    MongoClientSettings.builder()
      .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(Seq(server).asJava))
      .credential(credentials)
      .build()
  )

  val exporter = FromMongoToKafkaExporter(
    client,
    getStringProperty("repli.exporter.target.db"),
    getStringProperty("repli.exporter.target.collection")
  )

  Await.ready(
    exporter.graph.run,
    Duration.Inf
  ).value.get match {
    case Success(_) => println(s"Export done")
    case _ => println("failure")
  }

  client.close()
  actorSystem.terminate()
}
