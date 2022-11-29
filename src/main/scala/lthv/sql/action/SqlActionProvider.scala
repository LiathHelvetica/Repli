package lthv.sql.action

import lthv.sql.model.SqlRow
import lthv.sql.model.SqlTable
import lthv.sql.model.mapper.SqlTypeSyntaxMapper
import lthv.utils.Converters.SqlRawString
import scalikejdbc.NoExtractor
import scalikejdbc.SQL
import scalikejdbc.interpolation.SQLSyntax.csv
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

import scala.util.Try

import cats.implicits._

object SqlActionProvider {

  def getBatchInsertCommand(table: SqlTable, values: Seq[SqlRow]): Option[SQL[Nothing, NoExtractor]] = {
    values.headOption.map(_ => {
      val parsedValues = values.map(row => row.toInsertValue(table))
      sql"""
        insert into ${table.name.rawSql} (${table.columnNamesAsSyntax}) values ${csv(parsedValues: _*)}
      """
    })
  }

  def getCreateTableCommand(table: SqlTable)(implicit typeMapper: SqlTypeSyntaxMapper): Try[SQL[Nothing, NoExtractor]] = {

    val columnSyntaxSeq = Seq(table.idColumn.toSql) ++ table.parentIdColumn.map(c => c.toSql) ++ table.columns.map(col => col.toSql)
    columnSyntaxSeq.sequence.map(columns =>
      sql"""
        create table ${table.name.rawSql} (${csv(columns: _*)})
      """)
  }
}
