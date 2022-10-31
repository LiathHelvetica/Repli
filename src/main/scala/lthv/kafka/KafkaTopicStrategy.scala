package lthv.kafka

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback

trait KafkaTopicStrategy {
  def getTopicName(db: String, collection: String)(implicit conf: Config): String
}

object FromCollectionName extends KafkaTopicStrategy {
  def getTopicName(db: String, collection: String)(implicit conf: Config): String = {
    s"${getStringPropertyWithFallback("repli.exporter.destination.kafka.topic.prefix")}$collection"
  }
}

object FromDbAndCollectionName extends KafkaTopicStrategy {
  def getTopicName(db: String, collection: String)(implicit conf: Config): String = {
    s"${getStringPropertyWithFallback("repli.exporter.destination.kafka.topic.prefix")}$db.$collection"
  }
}




