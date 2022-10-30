package lthv.exporter

import akka.NotUsed
import akka.stream.scaladsl.Flow

trait ExportMiddleman[IN, OUT] {

  val flow: Flow[IN, OUT, NotUsed]
}
