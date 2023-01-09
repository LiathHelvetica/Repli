package lthv.importer.middleman

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import lthv.schema.decode.AbstractSqlTableMerger
import lthv.schema.decode.SqlDecoderResult
import lthv.schema.decode.SqlSchemaDecoder
import lthv.sql.action.SqlActionProvider
import lthv.sql.model.SqlRow
import lthv.sql.model.SqlTable
import lthv.sql.model.SqlTableData
import lthv.sql.model.SqlTableName
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.id.IdGenerationStrategy
import scalikejdbc.NoExtractor
import scalikejdbc.SQL

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class FromKafkaToSqlMiddleman(
  sqlSchemaDecoder: SqlSchemaDecoder,
  idGenerationStrategy: IdGenerationStrategy,
  sqlTableMerger: AbstractSqlTableMerger,
  sqlActionProvider: SqlActionProvider
)(
  implicit conf: Config,
  ec: ExecutionContext
) extends ImportMiddleman[CommittableMessage[Array[Byte], Array[Byte]], (SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor])] {

  val decodeMessagesFlow: Flow[CommittableMessage[Array[Byte], Array[Byte]], SqlDecoderResult, NotUsed] = Flow[CommittableMessage[Array[Byte], Array[Byte]]]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.messageDecoding"))(inRecord => Future {
      val rows = sqlSchemaDecoder.decode(inRecord.record)(idGenerationStrategy, conf)
      rows.get
    })

  val toSqlDataFlow: Flow[SqlDecoderResult, SqlTableData, NotUsed] = Flow[SqlDecoderResult]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.sqlData"))(decoderResult => Future {
      val tableData = sqlTableMerger.rowsToTableMetadata(decoderResult)
      tableData.get
    })

  // TODO: foldAsync?
  val foldForTableDataFlow: Flow[SqlTableData, Map[SqlTableName, (Seq[SqlTable], Seq[SqlRow])], NotUsed] = Flow[SqlTableData]
    .fold(Map.empty[SqlTableName, (Seq[SqlTable], Seq[SqlRow])])((acc, tableData) => {
      tableData.data.foldLeft(acc)((acc, tuple) => {
        val (tableName, (table, rows)) = tuple
        acc.get(tableName) match {
          case Some((tables, oldRows)) => acc.updated(tableName, (tables.prepended(table), oldRows ++ rows))
          case None => acc.updated(tableName, (Seq(table), rows))
        }
      })
    })

  val flattenTableDataFlow: Flow[Map[SqlTableName, (Seq[SqlTable], Seq[SqlRow])], (Seq[SqlTable], Seq[SqlRow]), NotUsed] = Flow[Map[SqlTableName, (Seq[SqlTable], Seq[SqlRow])]]
    .flatMapConcat(tableDataMap => {
      Source(tableDataMap.values.toSeq)
    })

  val mergeTablesFlow: Flow[(Seq[SqlTable], Seq[SqlRow]), (SqlTable, Seq[SqlRow]), NotUsed] = Flow[(Seq[SqlTable], Seq[SqlRow])]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.tableMerge"))(tuple => Future {
      val (tables, rows) = tuple
      val table = tables.tail.foldLeft(tables.head)((acc, t) => sqlTableMerger.mergeTables(acc, t).get)
      (table, rows)
    })

  val tablesToCommandsFlow: Flow[(SqlTable, Seq[SqlRow]), (SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), NotUsed] = Flow[(SqlTable, Seq[SqlRow])]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.commandEncoding"))(tuple => Future {

      val (table, rows) = tuple

      val actionsOpt = for {
        createTableCommand <- sqlActionProvider.getCreateTableCommand(table).toOption
        batchInsertCommand <- sqlActionProvider.getBatchInsertCommand(table, rows)
      } yield (createTableCommand, batchInsertCommand)

      actionsOpt.get
    })

  override val flow: Flow[CommittableMessage[Array[Byte], Array[Byte]], (SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), NotUsed] = Flow[CommittableMessage[Array[Byte], Array[Byte]]]
    .via(decodeMessagesFlow)
    .via(toSqlDataFlow)
    .via(foldForTableDataFlow)
    .via(flattenTableDataFlow)
    .via(mergeTablesFlow)
    .via(tablesToCommandsFlow)
}
