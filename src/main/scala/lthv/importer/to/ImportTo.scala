package lthv.importer.to

import akka.stream.scaladsl.Sink

trait ImportTo[OUT, MAT] {

  val sink: Sink[OUT, MAT]
}
