package lthv.exporter.from

import akka.NotUsed
import akka.stream.alpakka.mongodb.scaladsl.MongoSource
import akka.stream.scaladsl.Source
import org.mongodb.scala.Document
import org.mongodb.scala.MongoCollection

case class ExportFromMongo(collection: MongoCollection[Document]) extends ExportFrom[Document] {

  val source: Source[Document, NotUsed] = MongoSource(collection.find())
}
