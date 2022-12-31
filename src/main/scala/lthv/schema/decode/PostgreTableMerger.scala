package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.SqlTable
import lthv.sql.model.SqlTableMetadata
import lthv.utils.exception.TableWithNoRowsException

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import cats._
import cats.implicits._

object PostgreTableMerger extends SqlTableMerger {

  override def rowsToTable(data: SqlDecoderResult)(implicit conf: Config): Try[SqlTableMetadata] = {

    val outSeqTry = data.results.map(pair => {
      val (tableName, rows) = pair
      rows.headOption match {
        case Some(row) => {
          val baseTable = SqlTable.fromRow(tableName, row)
          val tableTry = rows.foldLeft[Try[SqlTable]](Success(baseTable))((acc, r) => {
            acc.map(sqlTable => {
              ???
            })
          })
          tableTry.map(tableName -> _)
        }
        case None => Failure(TableWithNoRowsException(tableName))
      }
    })

    outSeqTry.toSeq.sequence.map(data => SqlTableMetadata(data.toMap))
  }
}
