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
import com.typesafe.config.Config
import lthv.sql.model.SqlColumn.toSql
import lthv.sql.model.command.ForeignKeyConstraint
import lthv.sql.model.command.PrimaryKeyConstraint
import lthv.sql.model.command.SqlAlterTable
import lthv.sql.model.command.SqlConstraint
import lthv.utils.Converters.OptionalSqlValue
import lthv.utils.exception.SqlConstraintNotSupportedException

import scala.util.Success

trait SqlActionProvider {

  val actionProviderName: String
  implicit val typeMapper: SqlTypeSyntaxMapper
  def getAlterTableCommand(alterData: SqlAlterTable)(implicit conf: Config): Try[SQL[Nothing, NoExtractor]]

  def getBatchInsertCommand(table: SqlTable, values: Seq[SqlRow]): Option[SQL[Nothing, NoExtractor]] = {
    values.headOption.map(_ => {
      val tableColumns = table.columns.keys.toSeq
      val batch = values.map(row => {
        val parsedValues = Seq(sqls"${row.id.toSql}") ++
          table.rootIdColumn.flatMap(_ => row.rootId.map(v => sqls"${v.toSql}")) ++
          table.parentIdColumn.flatMap(_ => row.parentId.map(v => sqls"${v.toSql}")) ++
          tableColumns.map(columnName => {
            sqls"${row.values.get(columnName).getSqlValue.toSql}"
          })
        sqls"(${csv(parsedValues: _*)})"
      })
      sql"""
        insert into ${table.name.toTableName.rawSql} (${table.columnNamesAsSyntax}) values ${csv(batch: _*)}
      """
    })
  }

  def getCreateTableCommand(table: SqlTable)(implicit conf: Config): Try[SQL[Nothing, NoExtractor]] = {

    val columnSyntaxSeq = Seq(toSql(table.idColumn)) ++
      table.parentIdColumn.map(toSql(_)) ++
      table.rootIdColumn.map(toSql(_)) ++
      table.columns.map(toSql(_))

    columnSyntaxSeq.sequence.map(columns =>
      sql"""
        create table ${table.name.toTableName.rawSql} (${csv(columns: _*)})
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
    sql"alter table ${table.name.toTableName.rawSql} add constraint ${constraint.name.rawSql} primary key (${csv(columns: _*)})"
  }

  private def getCreateForeignKeyCommand(table: SqlTable, constraint: ForeignKeyConstraint): SQL[Nothing, NoExtractor] = {
    sql"alter table ${table.name.toTableName.rawSql} add constraint ${constraint.name.rawSql} foreign key (${constraint.originColumn.rawSql}) references ${constraint.referencedTable.rawSql}(${constraint.referencedColumn.rawSql})"
  }
}
