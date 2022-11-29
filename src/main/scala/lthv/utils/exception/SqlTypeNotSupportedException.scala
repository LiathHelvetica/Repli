package lthv.utils.exception

import lthv.sql.model.SqlType

case class SqlTypeNotSupportedException(msg: String) extends Exception(msg)

object SqlTypeNotSupportedException {

  def apply(msg: String): SqlTypeNotSupportedException = {
    new SqlTypeNotSupportedException(msg)
  }

  def apply(t: SqlType, mapperName: String): SqlTypeNotSupportedException = {
    SqlTypeNotSupportedException(s"Type $t is not supported by $mapperName")
  }
}