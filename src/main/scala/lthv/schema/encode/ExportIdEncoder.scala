package lthv.schema.encode

import com.typesafe.config.Config

case class ExportId(id: Array[Byte], tag: String)

trait ExportIdEncoder[T] {
  def getIdFrom(t: T)(implicit conf: Config): ExportId
}