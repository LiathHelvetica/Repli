package lthv.importer

import akka.Done
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ConsumerSettings
import akka.kafka.Subscription
import akka.kafka.scaladsl.Consumer.Control
import com.typesafe.config.Config
import lthv.importer.from.ImportFrom
import lthv.importer.from.ImportFromKafka
import lthv.importer.middleman.FromKafkaToPostgreMiddleman
import lthv.importer.middleman.ImportMiddleman
import lthv.importer.to.ImportTo
import lthv.importer.to.ImportToSql
import lthv.utils.id.IdGenerationStrategy
import scalikejdbc.NoExtractor
import scalikejdbc.SQL

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class FromKafkaToPostgreImporter(
  kafkaSettings: ConsumerSettings[Array[Byte], Array[Byte]],
  kafkaSubscription: Subscription,
  idGenerationStrategy: IdGenerationStrategy,
  connectionPoolName: String
)(
  implicit conf: Config,
  ec: ExecutionContext
) extends Importer[CommittableMessage[Array[Byte], Array[Byte]], (SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), Control, Future[Done]] {

  override val from: ImportFrom[CommittableMessage[Array[Byte], Array[Byte]], Control] = ImportFromKafka(kafkaSettings, kafkaSubscription)
  override val middleman: ImportMiddleman[CommittableMessage[Array[Byte], Array[Byte]], (SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor])] = FromKafkaToPostgreMiddleman(idGenerationStrategy)
  override val to: ImportTo[(SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), Future[Done]] = ImportToSql(connectionPoolName)
}

object FromKafkaToPostgreImporter {
  def apply(
    kafkaSettings: ConsumerSettings[Array[Byte], Array[Byte]],
    kafkaSubscription: Subscription,
    idGenerationStrategy: IdGenerationStrategy,
    connectionPoolName: String
  )(
    implicit conf: Config,
    ec: ExecutionContext
  ): FromKafkaToPostgreImporter = new FromKafkaToPostgreImporter(kafkaSettings, kafkaSubscription, idGenerationStrategy, connectionPoolName)
}
