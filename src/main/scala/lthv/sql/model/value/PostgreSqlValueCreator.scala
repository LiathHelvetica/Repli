package lthv.sql.model.value

import com.typesafe.config.Config
import lthv.sql.model.SqlBytesDefinedFloatPrecision
import lthv.sql.model.SqlBytesDefinedIntPrecision
import lthv.sql.model.SqlDigitDefinedFloatPrecision
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback

object PostgreSqlValueCreator extends SqlValueCreator {

  def createSqlNumber(bytes: Array[Byte])(implicit conf: Config): SqlValue = {

    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")
    createSqlNumber(BigDecimal(new String(bytes, charset)))
  }

  def createSqlNumber(v: BigDecimal)(implicit conf: Config): SqlValue = {
    v.toBigIntExact match {
      case Some(i) => SqlInt(i, SqlBytesDefinedIntPrecision(i.toByteArray.length))
      case None => toSqlFloat(v)
    }
  }

  private def toSqlFloat(v: BigDecimal): SqlValue = {
    if (v.isBinaryFloat) {
      SqlFloat(v, SqlBytesDefinedFloatPrecision(4)) // TODO: magic values?
    } else if (v.isBinaryDouble) {
      SqlFloat(v, SqlBytesDefinedFloatPrecision(8))
    } else {
      val f = v.bigDecimal.stripTrailingZeros
      val precision = f.precision
      val afterSeparatorPrecision = f.scale
      val beforeSeparatorPrecision = precision - afterSeparatorPrecision
      SqlFloat(v, SqlDigitDefinedFloatPrecision(beforeSeparatorPrecision, afterSeparatorPrecision))
    }
  }
}
