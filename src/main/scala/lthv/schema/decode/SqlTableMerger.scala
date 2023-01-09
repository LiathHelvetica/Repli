package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.SqlRow
import lthv.sql.model.SqlTable
import lthv.sql.model.SqlTableData
import lthv.sql.model.SqlType
import lthv.sql.model.value.SqlValue
import lthv.utils.exception.TableWithNoRowsException
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.exception.TypeMergeMismatchException

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import cats._
import cats.implicits._

object SqlTableMerger {

  // verified - works
  def rowsToTableMetadata(data: SqlDecoderResult)(implicit conf: Config): Try[SqlTableData] = {

    val outSeqTry = data.results.map(pair => {
      val (tableName, rows) = pair
      rows.headOption match {
        case Some(row) => {
          val baseTable = SqlTable.fromRow(tableName, row)
          val tableTry = updateTableByRows(baseTable, rows)
          tableTry.map(tableName -> (_, rows))
        }
        case None => Failure(TableWithNoRowsException(tableName))
      }
    })

    outSeqTry.toSeq.sequence.map(data => SqlTableData(data.toMap))
  }

  private def updateTableByRows(table: SqlTable, rows: Seq[SqlRow])(implicit conf: Config): Try[SqlTable] = {
    rows.foldLeft[Try[SqlTable]](Success(table))((acc, r) => {
      acc.flatMap(sqlTable => {
        val idColumnMergeResultTry = getIdColumn(table.idColumn, r.id)

        val parentIdColumnMergeResultTry = getOptionalIdColumn(sqlTable.parentIdColumn, r.parentId, "repli.importer.destination.sql.columns.parentId.name")

        val rootIdColumnMergeResultTry = getOptionalIdColumn(sqlTable.rootIdColumn, r.rootId, "repli.importer.destination.sql.columns.rootId.name")

        val allValuesTry: Try[Seq[(String, SqlType)]] = zipRowAndColumnTypes(table.columns, r.values)
          .map {
            // (present, added row)
            case (key, (Some(presentType), Some(rowType))) => key -> presentType.mergeTypes(rowType)
            case (key, (Some(presentType), None)) => key -> Success(presentType)
            case (key, (None, Some(rowType))) => key -> Success(rowType)
            case (key, (None, None)) => key -> Failure(TypeMergeMismatchException.apply)
          }.toSeq
          .map {
            case (key, Success(sqlType)) => Success(key -> sqlType)
            case (_, Failure(t)) => Failure(t)
          }.sequence

        // TODO: a way to alleviate copying
        for {
          idColumn <- idColumnMergeResultTry
          parentIdColumn <- parentIdColumnMergeResultTry
          rootIdColumn <- rootIdColumnMergeResultTry
          values <- allValuesTry
        } yield sqlTable.copy(
          idColumn = idColumn,
          parentIdColumn = parentIdColumn,
          rootIdColumn = rootIdColumn,
          columns = values.toMap
        )
      })
    })
  }

  private def getIdColumn(currentColumn: (String, SqlType), rowValue: SqlValue)(implicit conf: Config): Try[(String, SqlType)] = {
    currentColumn._2.mergeTypes(rowValue.sqlType).map(currentColumn._1 -> _)
  }

  private def getOptionalIdColumn(currentColumn: Option[(String, SqlType)], rowValue: Option[SqlValue], idNamePropertyPath: String)(implicit conf: Config): Try[Option[(String, SqlType)]] = {
    (currentColumn, rowValue) match {
      case (Some(currentParentIdColumn), Some(rowParentIdValue)) => getIdColumn(currentParentIdColumn, rowParentIdValue).map(Some(_))
      case (None, Some(rowParentIdValue)) => Success(Some(getStringPropertyWithFallback(idNamePropertyPath) -> rowParentIdValue.sqlType))
      case (Some(currentParentIdColumn), None) => Success(Some(currentParentIdColumn)) // should this ever happen?
      case (None, None) => Success(None)
    }
  }

  private def zipRowAndColumnTypes(columns: Map[String, SqlType], rowValues: Map[String, SqlValue]): Map[String, (Option[SqlType], Option[SqlType])] = {
    val presentTypes: Map[String, (Option[SqlType], Option[SqlType])] = columns.map(col => col._1 -> (Some(col._2), None))
    rowValues.foldLeft(presentTypes)((acc, pair) => {
      val (columnName, rowValue) = pair
      acc.get(columnName) match {
        case Some((t1, _)) => acc.updated(columnName, (t1, Some(rowValue.sqlType)))
        case None => acc.updated(columnName, (None, Some(rowValue.sqlType)))
      }
    })
  }

  def mergeTables(t1: SqlTable, t2: SqlTable)(implicit conf: Config): Try[SqlTable] = {

    val idColTry = mergeColumns(t1.idColumn, t2.idColumn)

    val rootIdColTry = mergeOptionalColumns(t1.rootIdColumn, t2.rootIdColumn)

    val parentIdColTry = mergeOptionalColumns(t1.parentIdColumn, t2.parentIdColumn)

    val allColumnsTry = zipColumns(t1.columns, t2.columns)
      .toSeq
      .map {
        case (key, (Some(col1), Some(col2))) => col1.mergeTypes(col2).map(key -> _)
        case (key, (None, Some(col2))) => Success(key -> col2)
        case (key, (Some(col1), None)) => Success(key -> col1)
        case (_, (None, None)) => Failure(TypeMergeMismatchException.apply)
      }
      .sequence

    for {
      idColumn <- idColTry
      rootIdColumn <- rootIdColTry
      parentIdColumn <- parentIdColTry
      columns <- allColumnsTry
    } yield t1.copy(idColumn = idColumn, rootIdColumn = rootIdColumn, parentIdColumn = parentIdColumn, columns = columns.toMap)
  }

  private def mergeColumns(col1: (String, SqlType), col2: (String, SqlType))(implicit conf: Config): Try[(String, SqlType)] = col1._2.mergeTypes(col2._2).map(col1._1 -> _)

  private def mergeOptionalColumns(c1Opt: Option[(String, SqlType)], c2Opt: Option[(String, SqlType)])(implicit conf: Config): Try[Option[(String, SqlType)]] = {
    (c1Opt, c2Opt) match {
      case (Some(col1), Some(col2)) => mergeColumns(col1, col2).map(Some(_))
      case (None, Some(col2)) => Success(Some(col2)) // should this be possible
      case (Some(col1), None) => Success(Some(col1))
      case (None, None) => Success(None)
    }
  }

  private def zipColumns(cols1: Map[String, SqlType], cols2: Map[String, SqlType]): Map[String, (Option[SqlType], Option[SqlType])] = {
    val c1: Map[String, (Option[SqlType], Option[SqlType])] = cols1.map(tuple => {
      val (colName, col) = tuple
      colName -> (Some(col), None)
    })
    cols2.foldLeft(c1)((acc, tuple) => {
      val (colName2, col2) = tuple
      acc.get(colName2) match {
        case Some((col1, _)) => acc.updated(colName2, (col1, Some(col2)))
        case None => acc.updated(colName2, (None, Some(col2)))
      }
    })
  }
}
