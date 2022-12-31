package lthv.utils.exception

import lthv.sql.model.SqlTableName

case class TableWithNoRowsException(msg: String) extends Exception(msg)

object TableWithNoRowsException {

  def apply(tableName: SqlTableName): TableWithNoRowsException = {
    new TableWithNoRowsException(s"Attempted to create table by providing 0 rows. Table name: ${tableName.name}")
  }
}
