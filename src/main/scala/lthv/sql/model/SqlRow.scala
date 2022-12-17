package lthv.sql.model

import lthv.sql.model.value.SqlValue
import lthv.utils.Converters.OptionalSqlValue
import scalikejdbc.interpolation.SQLSyntax
import scalikejdbc.interpolation.SQLSyntax.csv
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

case class SqlRow(id: SqlValue, parentId: Option[SqlValue], rootId: Option[SqlValue], values: Map[String, SqlValue]) {

  def toInsertValue(table: SqlTable): SQLSyntax = {
    val valuesSyntax = getRowValues(table).values.map(v => sqls"${v.toSql}").toSeq
    sqls"(${csv(valuesSyntax: _*)})"
  }

  private def getRowValues(table: SqlTable): Map[String, SqlValue] = {
    Map(table.idColumn.name -> id) ++
      table.parentIdColumn.map(pIdCol => pIdCol.name -> parentId.getSqlValue) ++
      table.rootIdColumn.map(rIdCol => rIdCol.name -> rootId.getSqlValue) ++
      table.columns.names.map(col => col -> values.get(col).getSqlValue).toMap
  }
}

object SqlRow {

  def apply(id: SqlValue, parentId: Option[SqlValue], rootId: Option[SqlValue], values: Map[String, SqlValue]): SqlRow = {
    new SqlRow(id, parentId, rootId, values)
  }

  def apply(id: SqlValue, parentId: Option[SqlValue], rootId: Option[SqlValue]): SqlRow = {
    SqlRow(id, parentId, rootId, Map.empty)
  }
}
