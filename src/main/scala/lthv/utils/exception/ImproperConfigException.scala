package lthv.utils.exception

case class ImproperConfigException(propertyPath: String, property: String)
  extends Exception(s"Improperly formatted property $property in path $propertyPath")
