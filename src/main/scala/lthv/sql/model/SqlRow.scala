package lthv.sql.model

import lthv.utils.Converters.OptionalSqlValue
import scalikejdbc.interpolation.SQLSyntax
import scalikejdbc.interpolation.SQLSyntax.csv
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

case class SqlRow(id: SqlValue, parentId: Option[SqlValue], values: Map[String, SqlValue]) {

  def toInsertValue(table: SqlTable): SQLSyntax = {
    val valuesSyntax = getRowValues(table).values.map(v => sqls"${v.toSql}").toSeq
    sqls"(${csv(valuesSyntax: _*)})"
  }

  private def getRowValues(table: SqlTable): Map[String, SqlValue] = {
    Map(table.idColumn.name -> id) ++
      table.parentIdColumn.map(pIdCol => pIdCol.name -> parentId.getSqlValue) ++
      table.columns.names.map(col => col -> values.get(col).getSqlValue).toMap
  }
}
