package lthv.sql.model

trait SqlConstraint {

}

case class PrimaryKeyConstraint(columns: Set[String]) extends SqlConstraint

case class ForeignKeyConstraint(originColumn: String, referencedColumn: String, referencedTable: String) extends SqlConstraint
