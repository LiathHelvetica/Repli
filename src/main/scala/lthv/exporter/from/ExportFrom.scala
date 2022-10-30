package lthv.exporter.from

import akka.NotUsed
import akka.stream.scaladsl.Source

trait ExportFrom[IN] {

  val source: Source[IN, NotUsed]
}
