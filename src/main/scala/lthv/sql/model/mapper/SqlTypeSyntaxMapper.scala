package lthv.sql.model.mapper

import lthv.sql.model.SqlType
import scalikejdbc.interpolation.SQLSyntax

import scala.util.Try

trait SqlTypeSyntaxMapper {
  def typeToSyntax(t: SqlType): Try[SQLSyntax]
}
