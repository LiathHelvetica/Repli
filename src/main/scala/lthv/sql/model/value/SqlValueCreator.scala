package lthv.sql.model.value

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import org.joda.time.format.DateTimeFormat

trait SqlValueCreator extends ValueFromBytesCreator[SqlValue] {

  def createSqlBinary(bytes: Array[Byte])(implicit conf: Config): SqlValue = {
    val VaryingTypeData(nBytesLowerBound, nBytesUpperBound, nBytes) = (bytes, "varbinaryType")

    bytes.length match {
      case _ if nBytesLowerBound <= nBytes && nBytes <= nBytesUpperBound => SqlVarbinary(bytes)
      case _ => SqlBinary(bytes)
    }
  }

  def createSqlText(bytes: Array[Byte])(implicit conf: Config): SqlValue = {
    val VaryingTypeData(nBytesLowerBound, nBytesUpperBound, nBytes) = (bytes, "varcharType")

    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    val v = new String(bytes, charset)

    v.length match {
      case _ if nBytesLowerBound <= nBytes && nBytes <= nBytesUpperBound => SqlVarchar(v)
      case _ => SqlText(v)
    }
  }

  def createSqlBoolean(bytes: Array[Byte])(implicit conf: Config): SqlValue = {

    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    SqlBoolean(new String(bytes, charset).toBoolean)
  }

  def createSqlDateTime(bytes: Array[Byte])(implicit conf: Config): SqlValue = {

    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    SqlDateTime({
      val formatter = DateTimeFormat.forPattern(getStringPropertyWithFallback("repli.schema.dateTimeFormat"))
      formatter.parseDateTime(new String(bytes, charset))
    })
  }
}

object VaryingTypeData {
  def unapply(data: (Array[Byte], String))(implicit conf: Config): Option[(Int, Int, Int)] = {
    val (bytes, propertyBit) = data

    val nBytesLowerBound = getIntPropertyWithFallback(s"repli.sql.$propertyBit.breakpoint.lowerBound")
    val nBytesUpperBound = getIntPropertyWithFallback(s"repli.sql.$propertyBit.breakpoint.upperBound")
    val nBytes = bytes.length

    Some(nBytesLowerBound, nBytesUpperBound, nBytes)
  }
}