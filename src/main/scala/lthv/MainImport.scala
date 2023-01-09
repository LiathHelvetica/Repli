package lthv

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ConsumerSettings
import akka.kafka.Subscription
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
import lthv.importer.FromKafkaToPostgreImporter
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

  val kafkaSettingsTry = Try {
    ConsumerSettings(
      getConfig("repli.importer.kafka.consumer"),
      new ByteArrayDeserializer,
      new ByteArrayDeserializer
    )
  }

  val connectionPoolNameTry: Try[String] = Try {
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

    connectionPoolName
  }

  // can subscribe to multiple topics but in case of single Import unit should be one
  val subscriptionTry: Try[Subscription] = Try { Subscriptions.topics(getTopicName) }

  val idGenerationStrategyTry: Try[IdGenerationStrategy] = getIdGenerationStrategyWithFallback("repli.importer.destination.sql.id.generation.strategy")

  val importer = for {
    kafkaSettings <- kafkaSettingsTry
    kafkaSubscription <- subscriptionTry
    idGenerationStrategy <- idGenerationStrategyTry
    connectionPoolName <- connectionPoolNameTry
  } yield FromKafkaToPostgreImporter(kafkaSettings, kafkaSubscription, idGenerationStrategy, connectionPoolName)

  val out = importer.flatMap(importer => {
    val (control, execution) = importer.graph.run()

    Await.ready(
      execution,
      Duration.Inf
    ).value match {
      case Some(_) => Success(control)
      case None => Failure(new Exception("Future has not yet ended. If this happened - congratulations :^]"))
    }
  })
}
