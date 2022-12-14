package lthv.schema.decode

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

trait ExportIdDecoder[T] {
  def decode(message: ConsumerRecord[Array[Byte], Array[Byte]])(implicit conf: Config): Try[T]
}
