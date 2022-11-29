package lthv.sql.model

trait SqlIntPrecision

case class SqlDigitDefinedIntPrecision(precision: Int) extends SqlIntPrecision

case class SqlBytesDefinedIntPrecision(precision: Int) extends SqlIntPrecision
