package lthv.utils.exception

case class NoIdTypeHeaderException(idTypeKey: String) extends Exception(s"Could not find header $idTypeKey. It should specify message id type.")
