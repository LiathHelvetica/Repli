package lthv.utils.id

import com.typesafe.config.Config
import lthv.sql.model.value.SqlVarchar

trait IdGenerationStrategy {
  def getId(implicit conf: Config): SqlVarchar
}

case class RandomIdStrategy(randomIdGenerator: IdGenerator) extends IdGenerationStrategy {

  override def getId(implicit conf: Config): SqlVarchar = SqlVarchar(randomIdGenerator.generateId)
}

object RandomIdStrategy {

  def apply(implicit conf: Config): RandomIdStrategy = {
    new RandomIdStrategy(IdGeneratorImpl(conf))
  }
}
