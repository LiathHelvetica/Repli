package lthv.sql.model

import com.typesafe.config.Config
import lthv.sql.model.command.SqlConstraint
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.Converters.SqlRawString
import scalikejdbc.interpolation.SQLSyntax

case class SqlTable(
  name: SqlTableName,
  // they do not contain idColumn and parentIdColumn
  columns: Map[String, SqlType],
  constraints: Seq[SqlConstraint],
  idColumn: (String, SqlType),
  parentIdColumn: Option[(String, SqlType)],
  rootIdColumn: Option[(String, SqlType)]
) {

  def columnNames: Seq[String] = {
    Seq(idColumn._1) ++ rootIdColumn.map(c => c._1) ++ parentIdColumn.map(c => c._1) ++  columns.keys
  }

  def columnNamesAsSyntax: SQLSyntax = {
    columnNames.mkString(", ").rawSql
  }
}

object SqlTable {
  def fromRow(name: SqlTableName, row: SqlRow)(implicit conf: Config): SqlTable = SqlTable(
    name,
    row.values.map(pair => pair._1 -> pair._2.sqlType), // TODO: column parser
    Seq.empty,
    getStringPropertyWithFallback("repli.importer.destination.sql.columns.id.name") -> row.id.sqlType,
    row.parentId.map(sqlV => getStringPropertyWithFallback("repli.importer.destination.sql.columns.parentId.name") -> sqlV.sqlType),
    row.rootId.map(sqlV => getStringPropertyWithFallback("repli.importer.destination.sql.columns.rootId.name") -> sqlV.sqlType)
  )
}
