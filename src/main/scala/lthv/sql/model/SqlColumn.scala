package lthv.sql.model

import lthv.sql.model.mapper.SqlTypeSyntaxMapper
import lthv.utils.Converters.SqlRawString
import scalikejdbc.SQLSyntax
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

import scala.util.Try

case class SqlColumn(name: String, sqlType: SqlType) {

  def toSql(implicit typeMapper: SqlTypeSyntaxMapper): Try[SQLSyntax] = {

    typeMapper.typeToSyntax(sqlType).map(typeSyntax =>
      sqls"${name.rawSql} $typeSyntax"
    )
  }
}

object SqlColumn {

  implicit class TableColumns(val underlying: Seq[SqlColumn]) extends AnyVal {

    def names: Seq[String] = {
      underlying.map(c => c.name)
    }
  }
}
