package lthv.utils.enums

object EnumKafkaTopicStrategy extends Enumeration {
  type KafkaTopicStrategy = Value

  val FromCollectionName, FromDbAndCollectionName = Value
}
