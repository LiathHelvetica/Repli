package lthv.exporter.middleman

import com.typesafe.config.Config
import lthv.schema.DocumentSchema
import lthv.schema.RootRepliSchema
import org.mongodb.scala.Document

import scala.concurrent.ExecutionContext

case class FromMongoToKafkaMiddleman()(implicit val conf: Config, val ex: ExecutionContext) extends ToKafkaMiddleman[Document] {

  override val middlemanHelper: RootRepliSchema[Document] = DocumentSchema
}
