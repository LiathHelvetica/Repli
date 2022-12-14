package lthv.exporter.to

import akka.stream.scaladsl.Sink

trait ExportTo[OUT, MAT] {

  val sink: Sink[OUT, MAT]
}
