package lthv.exporter

import akka.Done
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Sink
import lthv.utils.ConfigHelpers.getConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.concurrent.Future

trait ToKafkaExporter extends Exporter {

  override type OUT = ProducerRecord[Array[Byte], Array[Byte]]
  override type MAT = Future[Done]

  val kafkaSettings: ProducerSettings[Array[Byte], Array[Byte]] =
    ProducerSettings(getConfig("repli.exporter.kafka.producer"), new ByteArraySerializer, new ByteArraySerializer)

  val sink: Sink[OUT, MAT] = Producer.plainSink(kafkaSettings)

}
