package lthv.exporter.middleman

import com.typesafe.config.Config
import lthv.schema.encode.DocumentSchema
import lthv.schema.encode.RepliSchemaWithId
import org.mongodb.scala.Document

import scala.concurrent.ExecutionContext

case class FromMongoToKafkaMiddleman(topic: String)(implicit val conf: Config, val ex: ExecutionContext) extends ToKafkaMiddleman[Document] {

  override val middlemanHelper: RepliSchemaWithId[Document] = DocumentSchema
}
