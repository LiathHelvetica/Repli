package lthv

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.exporter.FromMongoToKafkaExporter
import lthv.utils.ConfigHelpers.getCharArrayProperty
import lthv.utils.ConfigHelpers.getKafkaTopicStrategyWithFallback
import lthv.utils.ConfigHelpers.getStringProperty
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.ConfigHelpers.getUrlSeqProperty
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoClientSettings
import org.mongodb.scala.MongoCredential
import org.mongodb.scala.ServerAddress
import org.mongodb.scala.connection.ClusterSettings

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object MainExport extends App {

  implicit val conf: Config = ConfigFactory.load("export")
  implicit val actorSystem: ActorSystem = ActorSystem()

  val credentialsTry: Try[MongoCredential] = Try {
    MongoCredential.createCredential(
      getStringProperty("repli.exporter.target.user"),
      getStringPropertyWithFallback("repli.exporter.target.authDb"),
      getCharArrayProperty("repli.exporter.target.password")
    )
  }

  val serversTry: Try[Seq[ServerAddress]] = getUrlSeqProperty(
    "repli.exporter.target.hosts",
    (s1, s2) => ServerAddress(s1, s2.toInt)
  )

  val clientTry: Try[MongoClient] = for {
    servers <- serversTry
    credentials <- credentialsTry
  } yield {
    MongoClient(
      MongoClientSettings.builder()
        .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(servers.asJava))
        .credential(credentials)
        .build()
    )
  }

  val exportersTry: Try[Seq[FromMongoToKafkaExporter]] = for {
    client <- clientTry
    kafkaTopicStrategy <- getKafkaTopicStrategyWithFallback("repli.exporter.destination.kafka.topic.strategy")
    collections <- getUrlSeqProperty(
      "repli.exporter.target.collections",
      (s1, s2) => (s1, s2)
    )
  } yield {
    collections.map(s => FromMongoToKafkaExporter(
      client,
      s._1,
      s._2,
      kafkaTopicStrategy
    ))
  }

  val outcome = exportersTry.flatMap(exporters => {
    Await.ready(
      Future.sequence(exporters.map(_.graph.run)),
      Duration.Inf
    ).value match {
      case Some(f) => f
      case None => Failure(new Exception("Future has not yet ended. If this happened - congratulations :^]"))
    }
  })

  outcome match {
    case Success(_) => println("Export done")
    case Failure(e) => println(s"Export failure\n$e")
  }

  clientTry.map(client => client.close)
  Await.result(
    actorSystem.terminate,
    Duration.Inf
  )
}
