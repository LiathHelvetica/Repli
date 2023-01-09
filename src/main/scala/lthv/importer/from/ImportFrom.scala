package lthv.importer.from

import akka.stream.scaladsl.Source

trait ImportFrom[IN, MAT] {

  val source: Source[IN, MAT]
}
