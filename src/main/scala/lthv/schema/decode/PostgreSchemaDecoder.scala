package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.SqlRow
import lthv.sql.model.value.SqlValue
import org.apache.kafka.clients.consumer.ConsumerRecord
import play.api.libs.json.Json

import scala.util.Try

object PostgreSchemaDecoder extends RepliSchemaDecoder[SqlValue, Seq[SqlRow]] {

  override val exportIdDecoder: ExportIdDecoder[SqlValue] = PostgreExportIdDecoder

  override def decode(message: ConsumerRecord[Array[Byte], Array[Byte]])(implicit conf: Config): Seq[SqlRow] = {
    val id = exportIdDecoder.decode(message)
    val json = Try { Json.parse(message.value) }
    val tableName = message.topic

    // strategy?
    ???
  }
}
