package lthv.sql.model.value

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.StaticConfig.varbinaryTypePropertyBit
import lthv.utils.StaticConfig.varcharTypePropertyBit
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

trait SqlValueCreator extends ValueCreator[SqlValue] {

  def createSqlBinary(bytes: Array[Byte])(implicit conf: Config): SqlValue = {
    val VaryingTypeData(nBytesLowerBound, nBytesUpperBound, nBytes) = (bytes, varbinaryTypePropertyBit)

    bytes.length match {
      case _ if nBytesLowerBound <= nBytes && nBytes <= nBytesUpperBound => SqlVarbinary(bytes)
      case _ => SqlBinary(bytes)
    }
  }

  def createSqlText(bytes: Array[Byte])(implicit conf: Config): SqlValue = {
    val VaryingTypeData(nBytesLowerBound, nBytesUpperBound, nBytes) = (bytes, varcharTypePropertyBit)

    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    val v = new String(bytes, charset)

    createSqlText(v, nBytesLowerBound, nBytesUpperBound, nBytes)
  }

  private def createSqlText(
    s: String,
    nBytesLowerBound: Int,
    nBytesUpperBound: Int,
    nBytes: Int
  ): SqlValue = if (nBytesLowerBound <= nBytes && nBytes <= nBytesUpperBound) {
    SqlVarchar(s)
  } else {
    SqlText(s)
  }

  def createSqlText(s: String)(implicit conf: Config): SqlValue = {
    val VaryingTypeData(nBytesLowerBound, nBytesUpperBound, nBytes) =
      (s.getBytes(getStringPropertyWithFallback("repli.schema.sql.play.bytes.encoding")), varcharTypePropertyBit)

    createSqlText(s, nBytesLowerBound, nBytesUpperBound, nBytes)
  }

  def createSqlBoolean(bytes: Array[Byte])(implicit conf: Config): SqlValue = {

    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    createSqlBoolean(new String(bytes, charset).toBoolean)
  }

  def createSqlBoolean(b: Boolean)(implicit conf: Config): SqlValue = SqlBoolean(b)

  def createSqlDateTime(bytes: Array[Byte])(implicit conf: Config): SqlValue = {

    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    createSqlDateTime({
      val formatter = DateTimeFormat.forPattern(getStringPropertyWithFallback("repli.schema.dateTimeFormat"))
      formatter.parseDateTime(new String(bytes, charset))
    })
  }

  def createSqlDateTime(dt : DateTime)(implicit conf: Config): SqlValue = SqlDateTime(dt)
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