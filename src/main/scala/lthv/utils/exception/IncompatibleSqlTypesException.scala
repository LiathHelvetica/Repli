package lthv.utils.exception

import lthv.sql.model.SqlType

case class IncompatibleSqlTypesException(msg: String) extends Exception(msg)

object IncompatibleSqlTypesException {

  def apply(current: SqlType, other: SqlType): IncompatibleSqlTypesException = {
    new IncompatibleSqlTypesException(s"Attempted to merge incompatible SqlTypes: $current AND $other")
  }
}
