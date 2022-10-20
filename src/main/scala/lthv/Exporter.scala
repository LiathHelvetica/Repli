package lthv

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.utils.ConfigHelpers.getCharArrayProperty
import lthv.utils.ConfigHelpers.getIntProperty
import lthv.utils.ConfigHelpers.getStringProperty
import org.mongodb.scala.Document
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoClientSettings
import org.mongodb.scala.MongoCredential
import org.mongodb.scala.ServerAddress
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.connection.ClusterSettings

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Success

object Exporter extends App {

  implicit val conf: Config = ConfigFactory.load("default")
  implicit val actorSystem: ActorSystem = ActorSystem()

  val credentials: MongoCredential = MongoCredential.createCredential(
    getStringProperty("repli.exporter.target.user"),
    getStringProperty("repli.exporter.target.authDb"),
    getCharArrayProperty("repli.exporter.target.password")
  )
  val server: ServerAddress = ServerAddress(
    getStringProperty("repli.exporter.target.host"),
    getIntProperty("repli.exporter.target.port")
  )

  val client = MongoClient(
    MongoClientSettings.builder()
      .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(Seq(server).asJava))
      .credential(credentials)
      .build()
  )
  val db = client.getDatabase(getStringProperty("repli.exporter.target.db"))
  val collection = db.getCollection(getStringProperty("repli.exporter.target.collection"))

  val source: Source[BsonDocument, NotUsed] = MongoSource(collection.find())

  val parsingFlow: Flow[BsonDocument, String, NotUsed] = Flow[BsonDocument]
    .mapAsyncUnordered(getIntProperty("repli.exporter.parallelism")) {
      document =>
        Future {
          document.toJson()
        }
    }
  // TODO: parallelism on sending to kafka
  val producerSink: Sink[String, Future[Int]] = Sink
    .fold[Int, String](0)((acc, json) => {
      println(json)
      acc + 1
    })

  val graph: RunnableGraph[Future[Int]] = source.via(parsingFlow).toMat(producerSink)(Keep.right)
  Await.ready(
    graph.run,
    Duration.Inf
  ).value.get match {
    case Success(v) => println(s"emitted $v documents")
    case _ => println("failure")
  }

  client.close()
  actorSystem.terminate()
}
