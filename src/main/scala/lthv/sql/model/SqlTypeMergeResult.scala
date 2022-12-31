package lthv.sql.model

trait SqlTypeMergeResult

object NoUpdateNeeded extends SqlTypeMergeResult

case class UpdateNeeded(newType: SqlType) extends SqlTypeMergeResult
