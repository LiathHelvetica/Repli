package lthv.importer.middleman

import akka.NotUsed
import akka.stream.scaladsl.Flow

trait ImportMiddleman[IN, OUT] {

  val flow: Flow[IN, OUT, NotUsed]
}
