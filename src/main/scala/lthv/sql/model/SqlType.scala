package lthv.sql.model

trait SqlType {

}

object SqlNullType extends SqlType

// unlimited text
object SqlTextType extends SqlType

// varchar max n chars
case class SqlVarcharType(n: Int) extends SqlType

// unlimited binary / blob
object SqlBinaryType extends SqlType

// binary string
case class SqlVarbinaryType(n: Int) extends SqlType

object SqlBooleanType extends SqlType

object SqlDateTimeType extends SqlType

// other date types will not be supported yet

case class SqlIntType(precision: SqlIntPrecision) extends SqlType

case class SqlFloatType(precision: SqlFloatPrecision) extends SqlType