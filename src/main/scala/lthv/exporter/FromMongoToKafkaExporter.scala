package lthv.exporter

import com.typesafe.config.Config
import lthv.exportschema.DocumentSchema
import lthv.exportschema.RootSchema
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.MongoDatabase

import scala.concurrent.ExecutionContext

case class FromMongoToKafkaExporter(
  client: MongoClient,
  requestedDatabase: String,
  requestedCollection: String
)(implicit val conf: Config, val ex: ExecutionContext) extends KafkaMiddleman with FromMongoExporter {

  override val middlemanHelper: RootSchema[IN] = DocumentSchema

  val db: MongoDatabase = client.getDatabase(requestedDatabase)
  val collection: MongoCollection[IN] = db.getCollection(requestedCollection)
}
