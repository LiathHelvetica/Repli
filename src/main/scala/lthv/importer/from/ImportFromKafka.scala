package lthv.importer.from

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ConsumerSettings
import akka.kafka.Subscription
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.SourceShape
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.GraphDSL.Implicits.SourceShapeArrow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergePreferred
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getFiniteDurationPropertyWithFallback

case class ImportFromKafka(
  kafkaSettings: ConsumerSettings[Array[Byte], Array[Byte]],
  kafkaSubscription: Subscription
)(implicit val conf: Config) extends ImportFrom[CommittableMessage[Array[Byte], Array[Byte]], Control] {

  val tickSource = Source.tick(
    getFiniteDurationPropertyWithFallback("repli.importer.source.idle.timeout.delay"),
    getFiniteDurationPropertyWithFallback("repli.importer.source.idle.timeout.after"),
    Option.empty[CommittableMessage[Array[Byte], Array[Byte]]]
  )

  val kafkaSource: Source[Option[CommittableMessage[Array[Byte], Array[Byte]]], Control] = Consumer
    .committableSource(kafkaSettings, kafkaSubscription)
    .map(Some(_))

  val timeoutableKafkaGraph = GraphDSL.createGraph(kafkaSource, tickSource)(Keep.left) { implicit b ⇒
    (r1, r2) ⇒
      val merge = b.add(MergePreferred[Option[CommittableMessage[Array[Byte], Array[Byte]]]](1, false))
      r2 ~> merge.in(0)
      r1 ~> merge.preferred
      SourceShape(merge.out)
  }

  override val source: Source[CommittableMessage[Array[Byte], Array[Byte]], Control] = Source
    .fromGraph(timeoutableKafkaGraph)
    .takeWhile(e => {
      e.isDefined
    })
    .map(_.get)
}
