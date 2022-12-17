package lthv.sql.model

case class SqlTableName(name: String) {
  def toTableName: String = name
}
