package lthv.sql.model

case class SqlTable(name: String, columns: Map[String, SqlType], constraints: Set[SqlConstraint])
