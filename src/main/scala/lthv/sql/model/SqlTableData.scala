package lthv.sql.model

case class SqlTableData(data: Map[SqlTableName, (SqlTable, Seq[SqlRow])])
