package lthv.sql.model

import org.joda.time.DateTime
import scalikejdbc.ParameterBinder
import scalikejdbc.ParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.stringParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.bytesParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.booleanParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.longParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.doubleParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.nullParameterBinderFactory
import scalikejdbc.jodatime.JodaParameterBinderFactory.jodaDateTimeParameterBinderFactory
import scalikejdbc.ParameterBinderWithValue

import java.io.ByteArrayInputStream
import java.sql.PreparedStatement

trait SqlValue {

  type T

  val underlying: T
  val sqlType: SqlType

  val parameterBinderFactory: ParameterBinderFactory[T]

  def toSql: ParameterBinderWithValue = {
    parameterBinderFactory(underlying)
  }
}

object SqlNull extends SqlValue {

  type T = Null

  val underlying: Null = null
  val sqlType: SqlType = SqlNullType

  val parameterBinderFactory: ParameterBinderFactory[Null] = nullParameterBinderFactory

  def apply: ParameterBinderWithValue = toSql
}

case class SqlText(
  underlying: String,
  sqlType: SqlType = SqlTextType,
  parameterBinderFactory: ParameterBinderFactory[String] = stringParameterBinderFactory
) extends SqlValue {

  type T = String
}

case class SqlVarchar(
  underlying: String,
  n: Int,
  parameterBinderFactory: ParameterBinderFactory[String] = stringParameterBinderFactory
) extends SqlValue {

  type T = String

  assert(n > underlying.length, "Varchar length smaller than its data")

  override val sqlType: SqlType = SqlVarcharType(n)
}

case class SqlBinary(
  underlying: Array[Byte],
  sqlType: SqlType = SqlBinaryType
) extends SqlValue {

  type T = Array[Byte]

  override val parameterBinderFactory: ParameterBinderFactory[Array[Byte]] = (value: Array[Byte]) => {

    val byteStream = new ByteArrayInputStream(value)

    ParameterBinder(
      value = byteStream,
      binder = (stmt: PreparedStatement, idx: Int) => stmt.setBinaryStream(idx, byteStream, value.length)
    )
  }
}

case class SqlVarbinary(
  underlying: Array[Byte],
  n: Int,
  parameterBinderFactory: ParameterBinderFactory[Array[Byte]] = bytesParameterBinderFactory
) extends SqlValue {

  type T = Array[Byte]

  assert(n > underlying.length, "Varbinary length smaller than its data")

  override val sqlType: SqlType = SqlVarbinaryType(n)
}

case class SqlBoolean(
  underlying: Boolean,
  sqlType: SqlType = SqlBooleanType,
  parameterBinderFactory: ParameterBinderFactory[Boolean] = booleanParameterBinderFactory
) extends SqlValue {

  type T = Boolean
}

case class SqlDateTime(
  underlying: DateTime,
  sqlType: SqlType = SqlDateTimeType,
  parameterBinderFactory: ParameterBinderFactory[DateTime] = jodaDateTimeParameterBinderFactory
) extends SqlValue {

  type T = DateTime
}

// TODO: classes for bigger/smaller ints/floats

case class SqlInt(
  underlying: Long,
  precision: SqlIntPrecision,
  parameterBinderFactory: ParameterBinderFactory[Long] = longParameterBinderFactory
) extends SqlValue {

  type T = Long

  override val sqlType: SqlType = SqlIntType(precision)
}

case class SqlFloat(
  underlying: Double,
  precision: SqlFloatPrecision,
  parameterBinderFactory: ParameterBinderFactory[Double] = doubleParameterBinderFactory
) extends SqlValue {

  type T = Double

  override val sqlType: SqlType = SqlFloatType(precision)
}

