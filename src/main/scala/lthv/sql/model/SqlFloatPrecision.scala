package lthv.sql.model

trait SqlFloatPrecision

// before 3.14 after
case class SqlDigitDefinedFloatPrecision(beforeSeparatorPrecision: Int, afterSeparatorPrecision: Int) extends SqlFloatPrecision

case class SqlBytesDefinedFloatPrecision(precision: Int) extends SqlFloatPrecision
