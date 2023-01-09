package lthv.utils.exception

case class TypeMergeMismatchException(msg: String) extends Exception(msg)

object TypeMergeMismatchException {
  def apply: TypeMergeMismatchException = {
    new TypeMergeMismatchException("Two missing values while joining for column merge. This should never have happened")
  }
}