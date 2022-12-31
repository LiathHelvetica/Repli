package lthv.sql.model

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.Converters.SqlRawString
import scalikejdbc.interpolation.SQLSyntax

case class SqlTable(
  name: SqlTableName,
  // they do not contain idColumn and parentIdColumn
  columns: Seq[SqlColumn],
  constraints: Seq[SqlConstraint],
  idColumn: SqlColumn,
  parentIdColumn: Option[SqlColumn],
  rootIdColumn: Option[SqlColumn]
) {

  def columnNames: Seq[String] = {
    Seq(idColumn.name) ++ parentIdColumn.toSeq.names ++ rootIdColumn.toSeq.names ++ columns.names
  }

  def columnNamesAsSyntax: SQLSyntax = {
    columnNames.mkString(", ").rawSql
  }
}

object SqlTable {
  def fromRow(name: SqlTableName, row: SqlRow)(implicit conf: Config): SqlTable = SqlTable(
    name,
    row.values.toSeq.map(pair => SqlColumn(pair._1, pair._2.sqlType)), // TODO: column parser
    Seq.empty,
    SqlColumn(getStringPropertyWithFallback("repli.importer.destination.sql.columns.id.name"), row.id.sqlType),
    row.parentId.map(sqlV => SqlColumn(getStringPropertyWithFallback("repli.importer.destination.sql.columns.parentId.name"), sqlV.sqlType)),
    row.rootId.map(sqlV => SqlColumn(getStringPropertyWithFallback("repli.importer.destination.sql.columns.rootId.name"), sqlV.sqlType))
  )
}
