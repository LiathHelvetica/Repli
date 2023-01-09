package lthv.importer.middleman

import com.typesafe.config.Config
import lthv.schema.decode.AbstractSqlTableMerger
import lthv.schema.decode.PostgreSchemaDecoder
import lthv.schema.decode.SqlTableMerger
import lthv.sql.action.PostgreActionProvider
import lthv.utils.id.IdGenerationStrategy

import scala.concurrent.ExecutionContext

class FromKafkaToPostgreMiddleman(
  idGenerationStrategy: IdGenerationStrategy,
  sqlTableMerger: AbstractSqlTableMerger
)(
  implicit conf: Config,
  ec: ExecutionContext
) extends FromKafkaToSqlMiddleman(PostgreSchemaDecoder, idGenerationStrategy, sqlTableMerger, PostgreActionProvider)

object FromKafkaToPostgreMiddleman {

  def apply(idGenerationStrategy: IdGenerationStrategy)(implicit conf: Config, ec: ExecutionContext): FromKafkaToPostgreMiddleman = {
    new FromKafkaToPostgreMiddleman(idGenerationStrategy, SqlTableMerger)
  }
}