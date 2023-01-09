package lthv.utils.exception

import lthv.sql.model.command.SqlAlterCommand

case class SqlAlterCommandNotSupported(msg: String) extends Exception(msg)

object SqlAlterCommandNotSupported {
  def apply(actionProviderName: String, alterCommand: SqlAlterCommand): SqlAlterCommandNotSupported = {
    new SqlAlterCommandNotSupported(s"ActionProvider $actionProviderName was provided with unsupported command: $alterCommand")
  }
}
