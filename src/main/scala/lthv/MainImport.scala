package lthv

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.utils.ConfigHelpers.getConfig
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getTopicName
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

object MainImport extends App {

  implicit val conf: Config = ConfigFactory.load("import")
  implicit val actorSystem: ActorSystem = ActorSystem()

  implicit val ex = ExecutionContext.global

  val kafkaSettings = ConsumerSettings(
    getConfig("repli.importer.kafka.consumer"),
    new ByteArrayDeserializer,
    new ByteArrayDeserializer
  )

  // can subscribe to multiple topics but in case of single Import unit should be one
  val topic = Subscriptions.topics(getTopicName)

  val source: Source[CommittableMessage[Array[Byte], Array[Byte]], Control] = Consumer.committableSource(kafkaSettings, topic)

  val flow: Flow[ConsumerRecord[Array[Byte], Array[Byte]], String /* Row */, NotUsed] = Flow[ConsumerRecord[Array[Byte], Array[Byte]]]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.importer.parallelism"))(inRecord => Future {
      val key = inRecord.key
      val value = Json.parse(inRecord.value)

      "asd"
    })

  val sink: Sink[String, Future[Int]] = Flow[String]
    .grouped(getIntPropertyWithFallback("repli.importer.rowGrouping"))
    .toMat(Sink.fold[Int, Seq[String]](0)((acc, rows) => {
      // process
      acc + rows.size
    }))(Keep.right)

}
