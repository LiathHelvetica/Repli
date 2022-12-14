package lthv.exporter

import akka.Done
import akka.kafka.ProducerSettings
import com.typesafe.config.Config
import lthv.exporter.from.ExportFrom
import lthv.exporter.from.ExportFromMongo
import lthv.exporter.middleman.ExportMiddleman
import lthv.exporter.middleman.FromMongoToKafkaMiddleman
import lthv.exporter.to.ExportTo
import lthv.exporter.to.ExportToKafka
import lthv.kafka.KafkaTopicStrategy
import org.apache.kafka.clients.producer.ProducerRecord
import org.mongodb.scala.Document
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.MongoDatabase

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

case class FromMongoToKafkaExporter(
  collection: MongoCollection[Document],
  kafkaSettings: ProducerSettings[Array[Byte], Array[Byte]],
  topic: String
)(implicit conf: Config, ex: ExecutionContext)
  extends Exporter[Document, ProducerRecord[Array[Byte], Array[Byte]], Future[Done]] {

  override val from: ExportFrom[Document] = ExportFromMongo(collection)
  override val middleman: ExportMiddleman[Document, ProducerRecord[Array[Byte], Array[Byte]]] = FromMongoToKafkaMiddleman(topic)
  override val to: ExportTo[ProducerRecord[Array[Byte], Array[Byte]], Future[Done]] = ExportToKafka(kafkaSettings)
}

object FromMongoToKafkaExporter {

  def apply(
    client: MongoClient,
    requestedDatabase: String,
    requestedCollection: String,
    kafkaTopicStrategy: KafkaTopicStrategy,
    kafkaSettings: ProducerSettings[Array[Byte], Array[Byte]]
  )(implicit conf: Config, ex: ExecutionContext): FromMongoToKafkaExporter = {

    val db: MongoDatabase = client.getDatabase(requestedDatabase)
    val collection: MongoCollection[Document] = db.getCollection(requestedCollection)

    new FromMongoToKafkaExporter(
      collection,
      kafkaSettings,
      kafkaTopicStrategy.getTopicName(requestedDatabase, requestedCollection)
    )
  }
}
