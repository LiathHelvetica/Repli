package lthv.sql.model.value

import com.typesafe.config.Config
import lthv.sql.model.SqlBinaryType
import lthv.sql.model.SqlBooleanType
import lthv.sql.model.SqlDateTimeType
import lthv.sql.model.SqlFloatPrecision
import lthv.sql.model.SqlFloatType
import lthv.sql.model.SqlIntPrecision
import lthv.sql.model.SqlIntType
import lthv.sql.model.SqlNullType
import lthv.sql.model.SqlTextType
import lthv.sql.model.SqlType
import lthv.sql.model.SqlVarbinaryType
import lthv.sql.model.SqlVarcharType
import org.joda.time.DateTime
import scalikejdbc.ParameterBinder
import scalikejdbc.ParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.bigDecimalParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.bigIntParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.booleanParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.bytesParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.nullParameterBinderFactory
import scalikejdbc.ParameterBinderFactory.stringParameterBinderFactory
import scalikejdbc.ParameterBinderWithValue
import scalikejdbc.jodatime.JodaParameterBinderFactory.jodaDateTimeParameterBinderFactory

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
  parameterBinderFactory: ParameterBinderFactory[String] = stringParameterBinderFactory
) extends SqlValue {

  type T = String

  override val sqlType: SqlType = SqlVarcharType(underlying.length)
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
  parameterBinderFactory: ParameterBinderFactory[Array[Byte]] = bytesParameterBinderFactory
) extends SqlValue {

  type T = Array[Byte]

  override val sqlType: SqlType = SqlVarbinaryType(underlying.length)
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

// TODO: classes for bigger/smaller ints/floats (client side, db side is already done)

case class SqlInt(
  underlying: BigInt,
  precision: SqlIntPrecision,
  parameterBinderFactory: ParameterBinderFactory[BigInt] = bigIntParameterBinderFactory
) extends SqlValue {

  type T = BigInt

  override val sqlType: SqlType = SqlIntType(precision)
}

case class SqlFloat(
  underlying: BigDecimal,
  precision: SqlFloatPrecision,
  parameterBinderFactory: ParameterBinderFactory[BigDecimal] = bigDecimalParameterBinderFactory
) extends SqlValue {

  type T = BigDecimal

  override val sqlType: SqlType = SqlFloatType(precision)
}
