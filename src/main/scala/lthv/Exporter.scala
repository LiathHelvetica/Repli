package lthv

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.exportschema.DocumentSchema
import lthv.utils.ConfigHelpers.getCharArrayProperty
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getStringProperty
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.ConfigHelpers.getConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.mongodb.scala.Document
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoClientSettings
import org.mongodb.scala.MongoCredential
import org.mongodb.scala.ServerAddress
import org.mongodb.scala.connection.ClusterSettings
import play.api.libs.json.Json

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Success

object Exporter extends App {

  implicit val conf: Config = ConfigFactory.load("export")
  implicit val actorSystem: ActorSystem = ActorSystem()

  val kafkaSettings: ProducerSettings[Array[Byte], Array[Byte]] =
    ProducerSettings(getConfig("repli.exporter.kafka.producer"), new ByteArraySerializer, new ByteArraySerializer)

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
  val db = client.getDatabase(getStringProperty("repli.exporter.target.db"))
  val collection = db.getCollection(getStringProperty("repli.exporter.target.collection"))

  val source: Source[Document, NotUsed] = MongoSource(collection.find())

  val parsingFlow: Flow[Document, ProducerRecord[Array[Byte], Array[Byte]], NotUsed] = Flow[Document]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.exporter.parallelism")) {
      document =>
        Future {
          val bson = document.toBsonDocument()
          val id = DocumentSchema.getId(bson)
          val message = Json.toBytes(DocumentSchema.encode(bson))
          val record = new ProducerRecord(
            getStringProperty("repli.exporter.destination.kafka.topic"),
            id.id,
            message
          )
          record.headers().add(
            getStringPropertyWithFallback("repli.exporter.schema.idTypeKey"),
            id.tag.getBytes(getStringPropertyWithFallback("repli.exporter.schema.exportIdCharset"))
          )
          record
        }
    }

  val kafkaSink: Sink[ProducerRecord[Array[Byte], Array[Byte]], Future[Done]] = Producer.plainSink(kafkaSettings)

  val graph: RunnableGraph[Future[Done]] = source.via(parsingFlow).toMat(kafkaSink)(Keep.right)
  Await.ready(
    graph.run,
    Duration.Inf
  ).value.get match {
    case Success(v) => println(s"Export done")
    case _ => println("failure")
  }

  client.close()
  actorSystem.terminate()
}
