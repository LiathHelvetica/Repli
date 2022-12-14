package lthv.schema.decode

import lthv.sql.model.value.PostgreSqlValueCreator
import lthv.sql.model.value.SqlValueCreator

object PostgreExportIdDecoder extends SqlExportIdDecoder {

  override val sqlValueCreator: SqlValueCreator = PostgreSqlValueCreator
}
