package lthv.utils.exception

case class ImproperIdTypeException(idType: String, decoderName: String, allowedTypes: Array[String])
  extends Exception(s"Decoder $decoderName does not support type $idType. Allowed types: ${allowedTypes.mkString("Array(", ", ", ")")}")
