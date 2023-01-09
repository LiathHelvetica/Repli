package lthv.sql.action

import com.typesafe.config.Config
import lthv.sql.model.command.SqlAddColumnAlter
import lthv.sql.model.command.SqlAlterCommand
import lthv.sql.model.command.SqlAlterTable
import lthv.sql.model.command.SqlColumnTypeAlter
import lthv.sql.model.mapper.PostgreSqlTypeSyntaxMapper
import lthv.sql.model.mapper.SqlTypeSyntaxMapper
import lthv.utils.exception.SqlAlterCommandNotSupported
import lthv.utils.Converters.SqlRawString
import scalikejdbc.NoExtractor
import scalikejdbc.SQL
import scalikejdbc.interpolation.SQLSyntax
import scalikejdbc.interpolation.SQLSyntax.csv
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

import scala.util.Failure
import scala.util.Try

import cats._
import cats.implicits._

object PostgreActionProvider extends SqlActionProvider {

  override val typeMapper: SqlTypeSyntaxMapper = PostgreSqlTypeSyntaxMapper
  override val actionProviderName: String = this.getClass.getSimpleName

  override def getAlterTableCommand(alterData: SqlAlterTable)(implicit conf: Config): Try[SQL[Nothing, NoExtractor]] = {
    toAlterSyntax(alterData.commands).map(commands =>
      sql"""
         alter table ${alterData.tableName.toTableName.rawSql} ${csv(commands: _*)}
       """)
  }

  private def toAlterSyntax(commands: Seq[SqlAlterCommand])(implicit conf: Config): Try[Seq[SQLSyntax]] = {
    commands.map {
      case SqlAddColumnAlter(columnName, columnType) => typeMapper.typeToSyntax(columnType).map(columnSyntax => sqls"add column ${columnName.rawSql} $columnSyntax")
      case SqlColumnTypeAlter(columnName, columnType) => typeMapper.typeToSyntax(columnType).map(columnSyntax => sqls"alter column ${columnName.rawSql} type $columnSyntax")
      case command => Failure(SqlAlterCommandNotSupported(actionProviderName, command))
    }.sequence
  }
}
