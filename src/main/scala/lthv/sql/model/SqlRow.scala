package lthv.sql.model

import lthv.sql.model.value.SqlValue
import lthv.utils.Converters.OptionalSqlValue
import scalikejdbc.interpolation.SQLSyntax
import scalikejdbc.interpolation.SQLSyntax.csv
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

case class SqlRow(id: SqlValue, parentId: Option[SqlValue], rootId: Option[SqlValue], values: Map[String, SqlValue]) {

  def toInsertValue(table: SqlTable): SQLSyntax = {
    val valuesSyntax = getRowValues(table).map(v => sqls"${v.toSql}")
    sqls"(${csv(valuesSyntax: _*)})"
  }

  private def getRowValues(table: SqlTable): Seq[SqlValue] = {
    Seq(id) ++
      table.parentIdColumn.map(_ => parentId.getSqlValue) ++
      table.rootIdColumn.map(_ => rootId.getSqlValue) ++
      table.columns.keys.map(col => values.get(col).getSqlValue)
  }
}

object SqlRow {

  def apply(id: SqlValue, parentId: Option[SqlValue], rootId: Option[SqlValue], values: Map[String, SqlValue]): SqlRow = {
    new SqlRow(id, parentId, rootId, values)
  }

  // TODO: ListMap?
  def apply(id: SqlValue, parentId: Option[SqlValue], rootId: Option[SqlValue]): SqlRow = {
    SqlRow(id, parentId, rootId, Map.empty)
  }
}
