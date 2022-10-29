package lthv.exporter

import akka.NotUsed
import akka.stream.scaladsl.Flow
import lthv.exportschema.RootSchema
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import lthv.utils.ConfigHelpers.getStringProperty
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait KafkaMiddleman extends ToKafkaExporter {

  val middlemanHelper: RootSchema[IN]
  implicit val ex: ExecutionContext

  val flow: Flow[IN, OUT, NotUsed] = Flow[IN]
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
