package lthv.sql.model

import com.typesafe.config.Config
import lthv.utils.exception.IncompatibleSqlTypesException

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait SqlType {
  def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult]
}

object SqlNullType extends SqlType {
  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    other match {
      case SqlNullType => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// unlimited text
// TODO: Add n bytes for better merging
object SqlTextType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    other match {
      case SqlTextType => Success(NoUpdateNeeded)
      case SqlVarcharType(_) => Success(UpdateNeeded(SqlTextType))
      case SqlNullType => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// varchar max n chars
case class SqlVarcharType(n: Int) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    other match {
      case SqlTextType => Success(UpdateNeeded(SqlTextType))
      case SqlVarcharType(otherN) if otherN > n => Success(UpdateNeeded(other))
      case SqlVarcharType(_) => Success(NoUpdateNeeded)
      case SqlNullType => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// unlimited binary / blob
object SqlBinaryType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    other match {
      case SqlBinaryType => Success(NoUpdateNeeded)
      case SqlVarbinaryType(_) => Success(UpdateNeeded(SqlBinaryType))
      case SqlNullType => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// binary string
case class SqlVarbinaryType(n: Int) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    other match {
      case SqlBinaryType => Success(UpdateNeeded(SqlBinaryType))
      case SqlVarbinaryType(otherN) if otherN > n => Success(UpdateNeeded(other))
      case SqlVarbinaryType(_) => Success(NoUpdateNeeded)
      case SqlNullType => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

object SqlBooleanType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    other match {
      case SqlBooleanType => Success(NoUpdateNeeded)
      case SqlNullType => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

object SqlDateTimeType extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    other match {
      case SqlDateTimeType => Success(NoUpdateNeeded)
      case SqlNullType => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

// other date types will not be supported yet

case class SqlIntType(precision: SqlIntPrecision) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    (other, precision) match {
      case (otherType@SqlIntType(SqlDigitDefinedIntPrecision(otherN)), SqlDigitDefinedIntPrecision(n)) if otherN > n => Success(UpdateNeeded(otherType))
      case (SqlIntType(SqlDigitDefinedIntPrecision(_)), SqlDigitDefinedIntPrecision(_)) => Success(NoUpdateNeeded)
      case (otherType@SqlIntType(SqlBytesDefinedIntPrecision(otherN)), SqlBytesDefinedIntPrecision(n)) if otherN > n => Success(UpdateNeeded(otherType))
      case (SqlIntType(SqlBytesDefinedIntPrecision(_)), SqlBytesDefinedIntPrecision(_)) => Success(NoUpdateNeeded)
      case (SqlFloatType(SqlDigitDefinedFloatPrecision(before, after)), SqlDigitDefinedIntPrecision(n)) => Success(UpdateNeeded(SqlFloatType(SqlDigitDefinedFloatPrecision(n.max(before), after))))
      case (SqlFloatType(SqlBytesDefinedFloatPrecision(otherN)), SqlBytesDefinedIntPrecision(n)) => Success(UpdateNeeded(SqlFloatType(SqlBytesDefinedFloatPrecision(n.max(otherN)))))
      case (SqlNullType, _) => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}

case class SqlFloatType(precision: SqlFloatPrecision) extends SqlType {

  override def mergeTypes(other: SqlType)(implicit conf: Config): Try[SqlTypeMergeResult] = {
    (other, precision) match {
      case (SqlFloatType(SqlDigitDefinedFloatPrecision(otherBefore, otherAfter)), SqlDigitDefinedFloatPrecision(before, after)) if otherBefore <= before && otherAfter <= after => Success(NoUpdateNeeded)
      case (SqlFloatType(SqlDigitDefinedFloatPrecision(otherBefore, otherAfter)), SqlDigitDefinedFloatPrecision(before, after)) => Success(UpdateNeeded(SqlFloatType(SqlDigitDefinedFloatPrecision(otherBefore.max(before), otherAfter.max(after)))))
      case (SqlFloatType(SqlBytesDefinedFloatPrecision(otherN)), SqlBytesDefinedFloatPrecision(n)) if otherN <= n => Success(NoUpdateNeeded)
      case (otherType@SqlFloatType(SqlBytesDefinedFloatPrecision(_)), SqlBytesDefinedFloatPrecision(_)) => Success(UpdateNeeded(otherType))
      case (SqlIntType(SqlDigitDefinedIntPrecision(otherN)), SqlDigitDefinedFloatPrecision(before, _)) if otherN <= before => Success(NoUpdateNeeded)
      case (SqlIntType(SqlDigitDefinedIntPrecision(otherN)), SqlDigitDefinedFloatPrecision(_, after)) => Success(UpdateNeeded(SqlFloatType(SqlDigitDefinedFloatPrecision(otherN, after))))
      case (SqlIntType(SqlBytesDefinedIntPrecision(otherN)), SqlBytesDefinedFloatPrecision(n)) if otherN <= n => Success(NoUpdateNeeded)
      case (SqlIntType(SqlBytesDefinedIntPrecision(otherN)), SqlBytesDefinedFloatPrecision(_)) => Success(UpdateNeeded(SqlFloatType(SqlBytesDefinedFloatPrecision(otherN))))
      case (SqlNullType, _) => Success(NoUpdateNeeded)
      case _ => Failure(IncompatibleSqlTypesException(this, other))
    }
  }
}