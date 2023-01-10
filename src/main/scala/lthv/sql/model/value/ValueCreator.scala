package lthv.sql.model.value

import com.typesafe.config.Config
import org.joda.time.DateTime

trait ValueCreator[T] {

  def createSqlNumber(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlBinary(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlText(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlBoolean(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlDateTime(bytes: Array[Byte])(implicit conf: Config): T

  def createSqlNumber(v: BigDecimal)(implicit conf: Config): T

  def createSqlText(s: String)(implicit conf: Config): T

  def createSqlBoolean(b: Boolean)(implicit conf: Config): T

  def createSqlDateTime(dt: DateTime)(implicit conf: Config): T
}
