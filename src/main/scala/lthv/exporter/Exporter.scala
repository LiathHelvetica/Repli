package lthv.exporter

import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RunnableGraph
import lthv.exporter.from.ExportFrom
import lthv.exporter.middleman.ExportMiddleman
import lthv.exporter.to.ExportTo

trait Exporter[IN, OUT, MAT] {

  val from: ExportFrom[IN]
  val middleman: ExportMiddleman[IN, OUT]
  val to: ExportTo[OUT, MAT]

  def graph: RunnableGraph[MAT] = from.source.via(middleman.flow).toMat(to.sink)(Keep.right)
}
