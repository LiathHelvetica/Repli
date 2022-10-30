package lthv.exporter

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.typesafe.config.Config
import lthv.schema.RootSchema
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getStringProperty
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait ToKafkaMiddleman[IN] extends ExportMiddleman[IN, ProducerRecord[Array[Byte], Array[Byte]]] {

  val middlemanHelper: RootSchema[IN]
  implicit val ex: ExecutionContext
  implicit val conf: Config

  val flow: Flow[IN, ProducerRecord[Array[Byte], Array[Byte]], NotUsed] = Flow[IN]
    .mapAsyncUnordered(getIntPropertyWithFallback("repli.exporter.parallelism")) {
      inRecord =>
        Future {
          val id = middlemanHelper.getId(inRecord)
          val message = Json.toBytes(middlemanHelper.encode(inRecord))
          val record = new ProducerRecord(
            getStringProperty("repli.exporter.destination.kafka.topic"),
            id.id,
            message
          )
          record.headers().add(
            getStringPropertyWithFallback("repli.exporter.schema.idTypeKey"),
            id.tag.getBytes(getStringPropertyWithFallback("repli.exporter.schema.exportIdCharset"))
          )
          record
        }
    }
}