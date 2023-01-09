package lthv.sql.model.command

trait SqlConstraint {
  val name: String
}

case class PrimaryKeyConstraint(name: String, columns: Set[String]) extends SqlConstraint

case class ForeignKeyConstraint(name: String, originColumn: String, referencedColumn: String, referencedTable: String) extends SqlConstraint
