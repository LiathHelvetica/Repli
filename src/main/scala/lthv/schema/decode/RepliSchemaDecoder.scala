package lthv.schema.decode

import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerRecord

trait RepliSchemaDecoder[ID, T] {

  val exportIdDecoder: ExportIdDecoder[ID]

  def decode(message: ConsumerRecord[Array[Byte], Array[Byte]])(implicit conf: Config): T
}
