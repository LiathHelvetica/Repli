package lthv.sql.model

trait SqlFloatPrecision

case class SqlDigitDefinedFloatPrecision(beforeSeparatorPrecision: Int, afterSeparatorPrecision: Int) extends SqlFloatPrecision

case class SqlBytesDefinedFloatPrecision(precision: Int) extends SqlFloatPrecision
