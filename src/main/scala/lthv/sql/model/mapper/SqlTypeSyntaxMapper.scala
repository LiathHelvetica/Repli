package lthv.sql.model.mapper

import com.typesafe.config.Config
import lthv.sql.model.SqlType
import scalikejdbc.interpolation.SQLSyntax

import scala.util.Try

trait SqlTypeSyntaxMapper {
  def typeToSyntax(t: SqlType)(implicit conf: Config): Try[SQLSyntax]
}
