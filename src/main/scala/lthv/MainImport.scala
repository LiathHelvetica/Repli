package lthv

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.SourceShape
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.GraphDSL.Implicits.SourceShapeArrow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergePreferred
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.schema.decode.PostgreExportIdDecoder
import lthv.schema.decode.PostgreSchemaDecoder
import lthv.schema.decode.SqlDecoderResult
import lthv.schema.decode.SqlTableMerger
import lthv.sql.action.PostgreActionProvider
import lthv.sql.executor.SqlCommandExecutor
import lthv.sql.model.SqlRow
import lthv.sql.model.SqlTable
import lthv.sql.model.SqlTableData
import lthv.sql.model.SqlTableName
import lthv.utils.ConfigHelpers.getBooleanPropertyWithFallback
import lthv.utils.ConfigHelpers.getConfig
import lthv.utils.ConfigHelpers.getFiniteDurationProperty
import lthv.utils.ConfigHelpers.getFiniteDurationPropertyWithFallback
import lthv.utils.ConfigHelpers.getIdGenerationStrategyWithFallback
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getStringProperty
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.ConfigHelpers.getTopicName
import lthv.utils.id.IdGenerationStrategy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import play.api.libs.json.Json
import scalikejdbc.ConnectionPool
import scalikejdbc.ConnectionPoolSettings
import scalikejdbc.GlobalSettings
import scalikejdbc.LoggingSQLAndTimeSettings
import scalikejdbc.NoExtractor
import scalikejdbc.SQL

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object MainImport extends App {

  implicit val conf: Config = ConfigFactory.load("import")
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val ex = ExecutionContext.global

  val kafkaSettings = ConsumerSettings(
    getConfig("repli.importer.kafka.consumer"),
    new ByteArrayDeserializer,
    new ByteArrayDeserializer
  )

  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
    printUnprocessedStackTrace = getBooleanPropertyWithFallback("repli.importer.target.scalike.logging.printUnprocessedStackTrace"),
    warningEnabled = getBooleanPropertyWithFallback("repli.importer.target.scalike.logging.warningEnabled"),
    singleLineMode = getBooleanPropertyWithFallback("repli.importer.target.scalike.logging.singleLineMode")
  )

  val connectionPoolName = getStringPropertyWithFallback("repli.importer.target.poolName")

  val dbSettings = ConnectionPoolSettings(
    validationQuery = getStringPropertyWithFallback("repli.importer.target.validationQuery")
  )
  ConnectionPool.add(
    connectionPoolName,
    getStringProperty("repli.importer.target.url"),
    getStringProperty("repli.importer.target.user"),
    getStringProperty("repli.importer.target.password"),
    dbSettings
  )

  // can subscribe to multiple topics but in case of single Import unit should be one
  val topic = Subscriptions.topics(getTopicName)

  val tickSource = Source.tick(
    getFiniteDurationPropertyWithFallback("repli.importer.source.idle.timeout.delay"),
    getFiniteDurationPropertyWithFallback("repli.importer.source.idle.timeout.after"),
    Option.empty[CommittableMessage[Array[Byte], Array[Byte]]]
  )

  val kafkaSource: Source[Option[CommittableMessage[Array[Byte], Array[Byte]]], Control] = Consumer
    .committableSource(kafkaSettings, topic)
    .map(Some(_))

  val timeoutableKafkaGraph = GraphDSL.createGraph(kafkaSource, tickSource)(Keep.left) { implicit b ⇒
    (r1, r2) ⇒
      val merge = b.add(MergePreferred[Option[CommittableMessage[Array[Byte], Array[Byte]]]](1, false))
      r2 ~> merge.in(0)
      r1 ~> merge.preferred
      SourceShape(merge.out)
  }

  val source = Source
    .fromGraph(timeoutableKafkaGraph)
    .takeWhile(e => {
      e.isDefined
    })
    .map(_.get)


  /*val debugSink: Sink[CommittableMessage[Array[Byte], Array[Byte]], Future[Done]] = Sink.foreach(e => {
    println("-=-=-=-=-=-=-=-=-=-=-=-= I GOT SOMETHING -=-=-=-=-=-=-=-=-=-=-=-=")
    println(e)
  })

  val f = source.toMat(debugSink)(Keep.right)

  val o = Await.ready(
    f.run(),
    Duration.Inf
  ).value match {
    case Some(_) => Success("SUCCESS")
    case None => Failure(new Exception("Future has not yet ended. If this happened - congratulations :^]"))
  }

  println(o)*/

  val decodeMessagesFlowTry: Try[Flow[CommittableMessage[Array[Byte], Array[Byte]], SqlDecoderResult, NotUsed]] = for {
    idGenerationStrategy <- getIdGenerationStrategyWithFallback("repli.importer.destination.sql.id.generation.strategy")
  } yield {

    implicit val idGenerationStrategyAsImplicit: IdGenerationStrategy = idGenerationStrategy

    Flow[CommittableMessage[Array[Byte], Array[Byte]]]
      .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.messageDecoding"))(inRecord => Future {
        println("-=-=-=-=-=-=-=-=-=-=-=-= DECODING -=-=-=-=-=-=-=-=-=-=-=-=")
        val rows = PostgreSchemaDecoder.decode(inRecord.record)
        rows.get
      })
  }

  val toSqlDataFlow: Flow[SqlDecoderResult, SqlTableData, NotUsed] = Flow[SqlDecoderResult]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.sqlData"))(decoderResult => Future {
      println("-=-=-=-=-=-=-=-=-=-=-=-= TO TABLE DATA -=-=-=-=-=-=-=-=-=-=-=-=")
      val tableData = SqlTableMerger.rowsToTableMetadata(decoderResult)
      tableData.get
    })

  // TODO: foldAsync?
  val foldForTableDataFlow: Flow[SqlTableData, Map[SqlTableName, (Seq[SqlTable], Seq[SqlRow])], NotUsed] = Flow[SqlTableData]
    .fold(Map.empty[SqlTableName, (Seq[SqlTable], Seq[SqlRow])])((acc, tableData) => {
      println("-=-=-=-=-=-=-=-=-=-=-=-= FOLDING -=-=-=-=-=-=-=-=-=-=-=-=")
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
      println("-=-=-=-=-=-=-=-=-=-=-=-= TO SOURCE AGAIN -=-=-=-=-=-=-=-=-=-=-=-=")
      // Source.fromIterator(() => tableDataMap.values.iterator)
      Source(tableDataMap.values.toSeq)
    })

  val mergeTablesFlow: Flow[(Seq[SqlTable], Seq[SqlRow]), (SqlTable, Seq[SqlRow]), NotUsed] = Flow[(Seq[SqlTable], Seq[SqlRow])]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.tableMerge"))(tuple => Future {
      println("-=-=-=-=-=-=-=-=-=-=-=-= MERGING TABLES -=-=-=-=-=-=-=-=-=-=-=-=")
      val (tables, rows) = tuple
      val table = tables.tail.foldLeft(tables.head)((acc, t) => SqlTableMerger.mergeTables(acc, t).get)
      (table, rows)
    })

  val tablesToCommandsFlow: Flow[(SqlTable, Seq[SqlRow]), (SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), NotUsed] = Flow[(SqlTable, Seq[SqlRow])]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism.commandEncoding"))(tuple => Future {

      println("-=-=-=-=-=-=-=-=-=-=-=-= TO SQL COMMANDS -=-=-=-=-=-=-=-=-=-=-=-=")

      val (table, rows) = tuple

      val actionsOpt = for {
        createTableCommand <- PostgreActionProvider.getCreateTableCommand(table).toOption
        batchInsertCommand <- PostgreActionProvider.getBatchInsertCommand(table, rows)
      } yield (createTableCommand, batchInsertCommand)

      actionsOpt.get
    })



  val sink: Sink[(SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), Future[Done]] = Sink.foreach(commands => {
    println("-=-=-=-=-=-=-=-=-=-=-=-= EXECUTING COMMANDS -=-=-=-=-=-=-=-=-=-=-=-=")
    val c = Seq(commands._1, commands._2)
    SqlCommandExecutor.executeCommands(connectionPoolName, c)
  })

  val a = for {
    decodeMessagesFlow <- decodeMessagesFlowTry
  } yield source
    .via(decodeMessagesFlow)
    .via(toSqlDataFlow)
    .via(foldForTableDataFlow)
    .via(flattenTableDataFlow)
    .via(mergeTablesFlow)
    .via(tablesToCommandsFlow)
    .toMat(sink)(Keep.both)

  /*val preMergeGraphTry = for {
    decodeMessagesFlow <- decodeMessagesFlowTry
  } yield source
    .via(decodeMessagesFlow)
    .via(toSqlDataFlow)
    .via(foldForTableDataFlow)

  val postMergeGraphTry = for {
    preMergeGraph <- preMergeGraphTry
  } yield Source.fromGraph(preMergeGraph)
    .via(flattenTableDataFlow)
    .via(mergeTablesFlow)
    .via(tablesToCommandsFlow)
    .toMat(sink)(Keep.both)*/

  val outcome = a.flatMap(importer => {

    val (control, execution) = importer.run()

    Await.ready(
      execution,
      Duration.Inf
    ).value match {
      case Some(_) => Success(control)
      case None => Failure(new Exception("Future has not yet ended. If this happened - congratulations :^]"))
    }
  })

  println(outcome)
}
