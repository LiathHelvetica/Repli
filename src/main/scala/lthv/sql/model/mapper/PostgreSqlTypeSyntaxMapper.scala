package lthv.sql.model.mapper

import com.typesafe.config.Config
import lthv.sql.model.SqlBinaryType
import lthv.sql.model.SqlBooleanType
import lthv.sql.model.SqlBytesDefinedFloatPrecision
import lthv.sql.model.SqlBytesDefinedIntPrecision
import lthv.sql.model.SqlDateTimeType
import lthv.sql.model.SqlDigitDefinedFloatPrecision
import lthv.sql.model.SqlDigitDefinedIntPrecision
import lthv.sql.model.SqlFloatType
import lthv.sql.model.SqlIntType
import lthv.sql.model.SqlTextType
import lthv.sql.model.SqlType
import lthv.sql.model.SqlVarbinaryType
import lthv.sql.model.SqlVarcharType
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.Converters.SqlRawString
import lthv.utils.exception.SqlTypeNotSupportedException
import scalikejdbc.interpolation.SQLSyntax
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

import scala.util.Success
import scala.util.Try

object PostgreSqlTypeSyntaxMapper extends SqlTypeSyntaxMapper {

  val mapperName: String = this.getClass.getSimpleName

  def typeToSyntax(t: SqlType)(implicit conf: Config): Try[SQLSyntax] = {

    Success(t).map {
      case SqlTextType => sqls"text"
      case SqlVarcharType(n) => sqls"varchar(${n.toString.rawSql})"
      case SqlBinaryType => sqls"bytea"
      case SqlVarbinaryType(_) => sqls"bytea"
      case SqlBooleanType => sqls"boolean"
      case SqlDateTimeType => sqls"timestamp"
      case SqlIntType(SqlBytesDefinedIntPrecision(n)) => n match {
        case n if n <= getIntPropertyWithFallback("postgre.smallint.digit.precision") => sqls"smallint"
        case n if n <= getIntPropertyWithFallback("postgre.integer.digit.precision") => sqls"integer"
        case n if n <= getIntPropertyWithFallback("postgre.bigint.digit.precision") => sqls"bigint"
        case n => throw SqlTypeNotSupportedException(s"Mapper $mapperName does not support int types with $n bytes precision")
      }
      case SqlIntType(SqlDigitDefinedIntPrecision(_)) => {
        throw SqlTypeNotSupportedException(s"Mapper $mapperName does not support int types with digit-defined precision")
      }
      case SqlFloatType(SqlBytesDefinedFloatPrecision(n)) => n match {
        case n if n <= getIntPropertyWithFallback("postgre.real.bytes.precision") => sqls"real"
        case n if n <= getIntPropertyWithFallback("postgre.double.bytes.precision") => sqls"double precision"
        case n => throw SqlTypeNotSupportedException(s"Mapper $mapperName does not support float types with $n bytes precision")
      }
      case SqlFloatType(SqlDigitDefinedFloatPrecision(b, a)) => (b, a) match {
        case (b, a) if b + a <= getIntPropertyWithFallback("postgre.real.digit.precision") => sqls"real"
        case (b, a) if b + a <= getIntPropertyWithFallback("postgre.double.digit.precision") => sqls"double precision"
        case (b, a) if
          b <= getIntPropertyWithFallback("postgre.numeric.digit.precision.beforeSeparator") &&
          a <= getIntPropertyWithFallback("postgre.numeric.digit.precision.afterSeparator")
        => sqls"numeric"
        case (b, a) => throw SqlTypeNotSupportedException(s"Mapper $mapperName does not support float types with $b digits before and $a digits after separator")
      }
      case _ => throw SqlTypeNotSupportedException(t, mapperName)
    }
  }
}
