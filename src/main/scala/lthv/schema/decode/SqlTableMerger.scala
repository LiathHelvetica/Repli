package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.SqlTableMetadata

import scala.util.Try

trait SqlTableMerger {
  // TODO: Probably state - valuemerger

  def rowsToTable(data: SqlDecoderResult)(implicit conf: Config): Try[SqlTableMetadata]
  // TODO: merge two metadatas
}
