package lthv.importer

import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RunnableGraph
import lthv.importer.from.ImportFrom
import lthv.importer.middleman.ImportMiddleman
import lthv.importer.to.ImportTo

trait Importer[IN, OUT, MAT_FROM, MAT_TO] {
  val from: ImportFrom[IN, MAT_FROM]
  val middleman: ImportMiddleman[IN, OUT]
  val to: ImportTo[OUT, MAT_TO]

  def graph: RunnableGraph[(MAT_FROM, MAT_TO)] = from.source.via(middleman.flow).toMat(to.sink)(Keep.both)
}
