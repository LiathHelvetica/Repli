package lthv.utils

import lthv.sql.model.value.SqlNull
import lthv.sql.model.value.SqlValue
import org.bson.internal.Base64
import scalikejdbc.SQLSyntax

import scala.language.implicitConversions
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Converters {

  implicit class Bytes(val underlying: Array[Byte]) extends AnyVal {
    def toBase64: String = {
      Base64.encode(underlying)
    }
  }

  implicit class Base64String(val underlying: String) extends AnyVal {
    def fromBase64: Array[Byte] = {
      Base64.decode(underlying)
    }
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

  implicit class OptionTryConvertible[T](val o: Option[T]) extends AnyVal {

    def toTry(otherwise: Throwable): Try[T] = o match {
      case Some(t) => Success(t)
      case None => Failure(otherwise)
    }
  }
}

