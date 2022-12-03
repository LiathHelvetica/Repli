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
import lthv.sql.model.ForeignKeyConstraint
import lthv.sql.model.PrimaryKeyConstraint
import lthv.sql.model.SqlConstraint
import lthv.utils.exception.SqlConstraintNotSupportedException

import scala.util.Success

object SqlActionProvider {

  val actionProviderName: String = this.getClass.getSimpleName

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

  def getCreateConstraintsCommand(table: SqlTable, constraint: SqlConstraint): Try[SQL[Nothing, NoExtractor]] = {
    Success(constraint).map {
      case c: PrimaryKeyConstraint => getCreatePrivateKeyCommand(table, c)
      case c: ForeignKeyConstraint => getCreateForeignKeyCommand(table, c)
      case _ => throw SqlConstraintNotSupportedException(table, constraint, actionProviderName)
    }
  }

  private def getCreatePrivateKeyCommand(table: SqlTable, constraint: PrimaryKeyConstraint): SQL[Nothing, NoExtractor] = {
    val columns = constraint.columns.toSeq.map(c => c.rawSql)
    sql"alter table ${table.name.rawSql} add constraint ${constraint.name.rawSql} primary key (${csv(columns: _*)})"
  }

  private def getCreateForeignKeyCommand(table: SqlTable, constraint: ForeignKeyConstraint): SQL[Nothing, NoExtractor] = {
    sql"alter table ${table.name.rawSql} add constraint ${constraint.name.rawSql} foreign key (${constraint.originColumn.rawSql}) references ${constraint.referencedTable.rawSql}(${constraint.referencedColumn.rawSql})"
  }
}
