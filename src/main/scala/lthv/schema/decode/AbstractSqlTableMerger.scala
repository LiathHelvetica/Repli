package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.SqlTable
import lthv.sql.model.SqlTableData

import scala.util.Try

trait AbstractSqlTableMerger {
  def rowsToTableMetadata(data: SqlDecoderResult)(implicit conf: Config): Try[SqlTableData]
  def mergeTables(t1: SqlTable, t2: SqlTable)(implicit conf: Config): Try[SqlTable]
}
