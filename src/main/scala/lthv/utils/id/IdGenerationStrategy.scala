package lthv.utils.id

import com.typesafe.config.Config
import lthv.sql.model.value.SqlValue
import lthv.sql.model.value.SqlValueCreator

trait IdGenerationStrategy {
  def getId(implicit sqlValueCreator: SqlValueCreator, conf: Config): SqlValue
}

case class RandomIdStrategy(randomIdGenerator: IdGenerator) extends IdGenerationStrategy {

  override def getId(implicit sqlValueCreator: SqlValueCreator, conf: Config): SqlValue = sqlValueCreator.createSqlText(randomIdGenerator.generateId)
}

object RandomIdStrategy {

  def apply(implicit conf: Config): RandomIdStrategy = {
    new RandomIdStrategy(IdGeneratorImpl(conf))
  }
}
