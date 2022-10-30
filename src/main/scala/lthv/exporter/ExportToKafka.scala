package lthv.exporter

import akka.Done
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Sink
import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.Future

case class ExportToKafka(
  kafkaSettings: ProducerSettings[Array[Byte], Array[Byte]]
) extends ExportTo[ProducerRecord[Array[Byte], Array[Byte]], Future[Done]] {

  val sink: Sink[ProducerRecord[Array[Byte], Array[Byte]], Future[Done]] = Producer.plainSink(kafkaSettings)
}

object ExportToKafka {

  def getKafkaSettings(implicit conf: Config): ProducerSettings[Array[Byte], Array[Byte]] = {
    ProducerSettings(getConfig("repli.exporter.kafka.producer"), new ByteArraySerializer, new ByteArraySerializer)
  }
}
