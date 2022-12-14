package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.value.SqlBoolean
import lthv.sql.model.value.SqlDateTime
import lthv.sql.model.value.SqlText
import lthv.sql.model.value.SqlValue
import lthv.sql.model.value.SqlValueCreator
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.Converters.OptionTryConvertible
import lthv.utils.exception.ImproperIdTypeException
import lthv.utils.exception.NoIdTypeHeaderException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.bson.BsonType
import org.joda.time.format.DateTimeFormat

import scala.util.Failure
import scala.util.Try

trait SqlExportIdDecoder extends ExportIdDecoder[SqlValue] {

  val sqlValueCreator: SqlValueCreator

  private val disallowedTypes: Set[BsonType] = Set(BsonType.ARRAY, BsonType.NULL, BsonType.UNDEFINED)

  private val allowedTypes: Array[String] = BsonType.values.flatMap {
    case v if disallowedTypes.contains(v) => None
    case v => Some(v.name)
  }

  override def decode(message: ConsumerRecord[Array[Byte], Array[Byte]])(implicit conf: Config): Try[SqlValue] = {
    val idTypeKey = getStringPropertyWithFallback("repli.schema.idTypeKey")
    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    val key = message.key

    Option(message.headers.lastHeader(idTypeKey))
      .toTry(NoIdTypeHeaderException(idTypeKey))
      .map(h => new String(h.value, charset))
      .flatMap(t => Try {
        BsonType.valueOf(t)
      }.recoverWith {
        case _ => Failure(ImproperIdTypeException(t, this.getClass.getSimpleName, allowedTypes))
      })
      .map {
        case BsonType.BOOLEAN => sqlValueCreator.createSqlBoolean(key)
        case BsonType.DATE_TIME => sqlValueCreator.createSqlDateTime(key)
        case BsonType.BINARY => sqlValueCreator.createSqlBinary(key)
        case BsonType.DOUBLE | BsonType.TIMESTAMP => sqlValueCreator.createSqlNumber(key)
        case BsonType.MAX_KEY => SqlText(getStringPropertyWithFallback("repli.schema.maxKeyValue"))
        case BsonType.MIN_KEY => SqlText(getStringPropertyWithFallback("repli.schema.minKeyValue"))
        case BsonType.DB_POINTER | BsonType.DOCUMENT | BsonType.JAVASCRIPT | BsonType.JAVASCRIPT_WITH_SCOPE |
             BsonType.OBJECT_ID | BsonType.REGULAR_EXPRESSION | BsonType.STRING | BsonType.SYMBOL
        => sqlValueCreator.createSqlText(key)
        case t => throw ImproperIdTypeException(t.name, this.getClass.getSimpleName, allowedTypes)
      }
  }
}
