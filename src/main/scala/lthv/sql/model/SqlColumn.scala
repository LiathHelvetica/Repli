package lthv.sql.model

import com.typesafe.config.Config
import lthv.sql.model.mapper.SqlTypeSyntaxMapper
import lthv.utils.Converters.SqlRawString
import scalikejdbc.SQLSyntax
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef

import scala.util.Try

case class SqlColumn(name: String, sqlType: SqlType) {

  def toSql(implicit typeMapper: SqlTypeSyntaxMapper, conf: Config): Try[SQLSyntax] = {

    typeMapper.typeToSyntax(sqlType).map(typeSyntax =>
      sqls"${name.rawSql} $typeSyntax"
    )
  }
}

object SqlColumn {

  def toSql(column: (String, SqlType))(implicit typeMapper: SqlTypeSyntaxMapper, conf: Config): Try[SQLSyntax] = {
    toSql(column._1, column._2)
  }

  def toSql(columnName: String, columnType: SqlType)(implicit typeMapper: SqlTypeSyntaxMapper, conf: Config): Try[SQLSyntax] = {
    typeMapper.typeToSyntax(columnType).map(typeSyntax =>
      sqls"${columnName.rawSql} $typeSyntax"
    )
  }
}
