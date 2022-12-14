package lthv.sql.model

import com.typesafe.config.Config
import lthv.utils.exception.IncompatibleSqlTypesException

import scala.util.Failure
import scala.util.Success
import scala.util.Try

// TODO: Strategy for merging instead of merger per type

trait SqlType {
  def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType]
}

object SqlNullType extends SqlType {
  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    Success(other)
  }
}

// unlimited text
// TODO: Add n bytes for better merging
object SqlTextType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    other match {
      case SqlTextType => Success(this)
      case SqlVarcharType(_) => Success(SqlTextType)
      case SqlNullType => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// varchar max n chars
case class SqlVarcharType(n: Int) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    other match {
      case SqlTextType => Success(SqlTextType)
      case SqlVarcharType(otherN) if otherN > n => Success(other)
      case SqlVarcharType(_) => Success(this)
      case SqlNullType => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// unlimited binary / blob
object SqlBinaryType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    other match {
      case SqlBinaryType => Success(this)
      case SqlVarbinaryType(_) => Success(SqlBinaryType)
      case SqlNullType => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// binary string
case class SqlVarbinaryType(n: Int) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    other match {
      case SqlBinaryType => Success(SqlBinaryType)
      case SqlVarbinaryType(otherN) if otherN > n => Success(other)
      case SqlVarbinaryType(_) => Success(this)
      case SqlNullType => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

object SqlBooleanType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    other match {
      case SqlBooleanType => Success(this)
      case SqlNullType => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

object SqlDateTimeType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    other match {
      case SqlDateTimeType => Success(this)
      case SqlNullType => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// other date types will not be supported yet

case class SqlIntType(precision: SqlIntPrecision) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    (other, precision) match {
      case (otherType@SqlIntType(SqlDigitDefinedIntPrecision(otherN)), SqlDigitDefinedIntPrecision(n)) if otherN > n => Success(otherType)
      case (SqlIntType(SqlDigitDefinedIntPrecision(_)), SqlDigitDefinedIntPrecision(_)) => Success(this)
      case (otherType@SqlIntType(SqlBytesDefinedIntPrecision(otherN)), SqlBytesDefinedIntPrecision(n)) if otherN > n => Success(otherType)
      case (SqlIntType(SqlBytesDefinedIntPrecision(_)), SqlBytesDefinedIntPrecision(_)) => Success(this)
      case (SqlFloatType(SqlDigitDefinedFloatPrecision(before, after)), SqlDigitDefinedIntPrecision(n)) => Success(SqlFloatType(SqlDigitDefinedFloatPrecision(n.max(before), after)))
      case (SqlFloatType(SqlBytesDefinedFloatPrecision(otherN)), SqlBytesDefinedIntPrecision(n)) => Success(SqlFloatType(SqlBytesDefinedFloatPrecision(n.max(otherN))))
      case (SqlNullType, _) => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

case class SqlFloatType(precision: SqlFloatPrecision) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlType] = {
    (other, precision) match {
      case (SqlFloatType(SqlDigitDefinedFloatPrecision(otherBefore, otherAfter)), SqlDigitDefinedFloatPrecision(before, after)) if otherBefore <= before && otherAfter <= after => Success(this)
      case (SqlFloatType(SqlDigitDefinedFloatPrecision(otherBefore, otherAfter)), SqlDigitDefinedFloatPrecision(before, after)) => Success(SqlFloatType(SqlDigitDefinedFloatPrecision(otherBefore.max(before), otherAfter.max(after))))
      case (SqlFloatType(SqlBytesDefinedFloatPrecision(otherN)), SqlBytesDefinedFloatPrecision(n)) if otherN <= n => Success(this)
      case (otherType@SqlFloatType(SqlBytesDefinedFloatPrecision(_)), SqlBytesDefinedFloatPrecision(_)) => Success(otherType)
      case (SqlIntType(SqlDigitDefinedIntPrecision(otherN)), SqlDigitDefinedFloatPrecision(before, _)) if otherN <= before => Success(this)
      case (SqlIntType(SqlDigitDefinedIntPrecision(otherN)), SqlDigitDefinedFloatPrecision(_, after)) => Success(SqlFloatType(SqlDigitDefinedFloatPrecision(otherN, after)))
      case (SqlIntType(SqlBytesDefinedIntPrecision(otherN)), SqlBytesDefinedFloatPrecision(n)) if otherN <= n => Success(this)
      case (SqlIntType(SqlBytesDefinedIntPrecision(otherN)), SqlBytesDefinedFloatPrecision(_)) => Success(SqlFloatType(SqlBytesDefinedFloatPrecision(otherN)))
      case (SqlNullType, _) => Success(this)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}