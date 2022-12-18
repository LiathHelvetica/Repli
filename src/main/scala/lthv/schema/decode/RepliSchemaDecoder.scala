package lthv.schema.decode

import com.typesafe.config.Config
import lthv.utils.id.IdGenerationStrategy
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

trait RepliSchemaDecoder[T] {
  def decode(message: ConsumerRecord[Array[Byte], Array[Byte]])(implicit idGenerationStrategy: IdGenerationStrategy, conf: Config): Try[T]
}
