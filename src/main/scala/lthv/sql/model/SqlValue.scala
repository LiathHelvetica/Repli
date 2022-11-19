package lthv.sql.model

import org.joda.time.DateTime

trait SqlValue {
  val sqlType: SqlType
}

case class SqlText(underlying: String, sqlType: SqlType = SqlTextType) extends SqlValue

case class SqlVarchar(underlying: String, n: Int) extends SqlValue {

  assert(n > underlying.length, "Varchar length smaller than its data")

  override val sqlType: SqlType = SqlVarcharType(n)
}

case class SqlBinary(underlying: Array[Byte], sqlType: SqlType = SqlBinaryType) extends SqlValue

case class SqlVarbinary(underlying: Array[Byte], n: Int) extends SqlValue {

  assert(n > underlying.length, "Varbinary length smaller than its data")

  override val sqlType: SqlType = SqlVarbinaryType(n)
}

case class SqlBoolean(underlying: Boolean, sqlType: SqlType = SqlBooleanType) extends SqlValue

case class SqlDateTime(underlying: DateTime, sqlType: SqlType = SqlDateTimeType) extends SqlValue

// TODO: classes for bigger/smaller ints/floats

case class SqlInt(underlying: Long, precision: SqlIntPrecision) extends SqlValue {
  override val sqlType: SqlType = SqlIntType(precision)
}

case class SqlFloat(underlying: Double, precision: SqlFloatPrecision) extends SqlValue {
  override val sqlType: SqlType = SqlFloatType(precision)
}

