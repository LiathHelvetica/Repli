package lthv.sql.model.value

import com.typesafe.config.Config

trait ValueFromBytesCreator[T] {

  def createSqlNumber(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlBinary(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlText(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlBoolean(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlDateTime(bytes: Array[Byte])(implicit conf: Config): T
}
