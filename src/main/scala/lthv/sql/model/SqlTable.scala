package lthv.sql.model

import lthv.utils.Converters.SqlRawString
import scalikejdbc.interpolation.SQLSyntax

case class SqlTable(
  name: String,
  // they do not contain idColumn and parentIdColumn
  columns: Seq[SqlColumn],
  constraints: Seq[SqlConstraint],
  idColumn: SqlColumn,
  parentIdColumn: Option[SqlColumn]
) {

  def columnNames: Seq[String] = {
    Seq(idColumn.name) ++ parentIdColumn.toSeq.names ++ columns.names
  }

  def columnNamesAsSyntax: SQLSyntax = {
    columnNames.mkString(", ").rawSql
  }
}
