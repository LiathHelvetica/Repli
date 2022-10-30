package lthv.exporter

import akka.Done
import com.typesafe.config.Config
import lthv.exporter.ExportToKafka.getKafkaSettings
import org.apache.kafka.clients.producer.ProducerRecord
import org.mongodb.scala.Document
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.MongoDatabase

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

case class FromMongoToKafkaExport(
  collection: MongoCollection[Document]
)(implicit conf: Config, ex: ExecutionContext)
  extends Export[Document, ProducerRecord[Array[Byte], Array[Byte]], Future[Done]] {

  override val from: ExportFrom[Document] = ExportFromMongo(collection)
  override val middleman: ExportMiddleman[Document, ProducerRecord[Array[Byte], Array[Byte]]] = FromMongoToKafkaMiddleman()
  override val to: ExportTo[ProducerRecord[Array[Byte], Array[Byte]], Future[Done]] = ExportToKafka(getKafkaSettings)
}

object FromMongoToKafkaExport {

  def apply(
    client: MongoClient,
    requestedDatabase: String,
    requestedCollection: String
  )(implicit conf: Config, ex: ExecutionContext): FromMongoToKafkaExport = {

    val db: MongoDatabase = client.getDatabase(requestedDatabase)
    val collection: MongoCollection[Document] = db.getCollection(requestedCollection)

    new FromMongoToKafkaExport(collection)
  }
}
