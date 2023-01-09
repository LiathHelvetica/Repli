package lthv.utils.exception

import lthv.sql.model.SqlTable
import lthv.sql.model.command.SqlConstraint

case class SqlConstraintNotSupportedException(msg: String) extends Exception(msg)

object SqlConstraintNotSupportedException {

  def apply(msg: String): SqlConstraintNotSupportedException = {
    new SqlConstraintNotSupportedException(msg)
  }

  def apply(table: SqlTable, constraint: SqlConstraint, actionProviderName: String): SqlConstraintNotSupportedException = {
    SqlConstraintNotSupportedException(
      s"Constraint ${constraint.getClass.getSimpleName} is not supported by ActionProvider $actionProviderName. " +
      s"Table: ${table.name}, Constraint: $constraint"
    )
  }
}
