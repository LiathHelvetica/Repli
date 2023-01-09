package lthv.sql.model

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback

// TODO: filters for strings? i.e. mappers to snake case and others

trait SqlTableNamingStrategy {
  def getTableName(nameStack: Seq[String])(implicit conf: Config): SqlTableName
}

object LastNameStrategy extends SqlTableNamingStrategy {

  override def getTableName(nameStack: Seq[String])(implicit conf: Config): SqlTableName = SqlTableName(nameStack.last)
}

object FullPathStrategy extends SqlTableNamingStrategy {

  override def getTableName(nameStack: Seq[String])(implicit conf: Config): SqlTableName = {
    val sep = getStringPropertyWithFallback("repli.importer.destination.sql.tableName.separator")
    SqlTableName(nameStack.mkString(sep)/*TODO: name middleware*/.replace(".", sep))
  }
}