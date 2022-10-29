package lthv.exporter

import akka.NotUsed
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.scaladsl.Source
import org.mongodb.scala.Document
import org.mongodb.scala.MongoCollection

trait FromMongoExporter extends Exporter {

  override type IN = Document
  val collection: MongoCollection[IN]

  val source: Source[IN, NotUsed] = MongoSource(collection.find())
}
