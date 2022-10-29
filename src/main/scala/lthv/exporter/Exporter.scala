package lthv.exporter

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.Config

trait Exporter {

  type IN
  type OUT
  type MAT

  implicit val conf: Config

  val source: Source[IN, NotUsed]
  val flow: Flow[IN, OUT, NotUsed]
  val sink: Sink[OUT, MAT]

  val graph: RunnableGraph[MAT] = source.via(flow).toMat(sink)(Keep.right)
}
