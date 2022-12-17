package lthv.schema.decode

import lthv.sql.model.SqlRow
import lthv.sql.model.SqlTableName

import scala.collection.immutable.Seq

case class SqlDecoderResult(results: Map[SqlTableName, Seq[SqlRow]])

object SqlDecoderResult {

  def apply(results: Map[SqlTableName, Seq[SqlRow]]): SqlDecoderResult = {
    new SqlDecoderResult(results)
  }

  def apply(results: Seq[(SqlTableName, SqlRow)]): SqlDecoderResult = {
    SqlDecoderResult(
      results.foldLeft(Map.empty[SqlTableName, Seq[SqlRow]])((acc, r) => {
        val (key, row) = r
        acc.get(key) match {
          case Some(_) => acc.updatedWith(key)(oldOpt => oldOpt.map(rows => rows :+ row)) // TODO: verify fastness - better collection
          case None => acc.updated(key, List(row))
        }
      })
    )
  }
}

trait SqlSchemaDecoder extends RepliSchemaDecoder[SqlDecoderResult]
