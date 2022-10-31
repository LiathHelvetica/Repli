package lthv.utils.exception

case class EnumPropertyException(path: String, enumName: String, possibleValues: Set[String])
  extends Exception(s"Illegal value $enumName for path $path. Possible values: $possibleValues")

