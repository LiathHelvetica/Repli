package lthv.sql.model

case class SqlRow(id: SqlValue, parentId: Option[SqlValue], fields: Map[String, SqlValue])
