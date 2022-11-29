package lthv.utils

import lthv.sql.model.SqlNull
import lthv.sql.model.SqlValue
import org.bson.internal.Base64
import scalikejdbc.SQLSyntax

import scala.language.implicitConversions

object Converters {

  implicit def toBase64(bytes: Array[Byte]): String = {
    Base64.encode(bytes)
  }

  implicit class OptionalSqlValue(val underlying: Option[SqlValue]) extends AnyVal {

    def getSqlValue: SqlValue = {
      underlying.getOrElse(SqlNull)
    }
  }

  implicit class SqlRawString(val underlying: String) extends AnyVal {

    def rawSql: SQLSyntax = {
      SQLSyntax.createUnsafely(underlying)
    }
  }
}
