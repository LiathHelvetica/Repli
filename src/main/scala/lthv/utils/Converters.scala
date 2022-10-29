package lthv.utils

import org.bson.internal.Base64

import scala.language.implicitConversions

object Converters {

  implicit def toBase64(bytes: Array[Byte]): String = {
    Base64.encode(bytes)
  }
}
