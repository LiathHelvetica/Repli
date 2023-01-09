package lthv.sql.model.command

import lthv.sql.model.SqlTableName
import lthv.sql.model.SqlType

case class SqlAlterTable(tableName: SqlTableName, commands: Seq[SqlAlterCommand])

trait SqlAlterCommand

case class SqlAddColumnAlter(columnName: String, columnType: SqlType) extends SqlAlterCommand

case class SqlColumnTypeAlter(columnName: String, columnType: SqlType) extends SqlAlterCommand


