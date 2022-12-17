package lthv.utils.exception

import lthv.sql.model.value.SqlValue
import play.api.libs.json.JsValue

case class ImproperJsValueException(msg: String) extends Exception(msg)

object ImproperJsValueException {

  def apply(
    json: JsValue,
    id: SqlValue,
    parentId: Option[SqlValue],
    rootId: Option[SqlValue],
    nameStack: Seq[String]
  ): ImproperJsValueException = {
    new ImproperJsValueException(
      "Decoder was provided with improper JsValue. It should be a JsObject\n" +
        getMessageForDecoderError(json, id, parentId, rootId, nameStack)
    )
  }

  def apply(
    key: String,
    jsVal: JsValue,
    json: JsValue,
    id: SqlValue,
    parentId: Option[SqlValue],
    rootId: Option[SqlValue],
    nameStack: Seq[String]
  ): ImproperJsValueException = {
    new ImproperJsValueException(
      "Decoder was provided with improper JsValue. It should be a JsObject or JsArray\n" +
        s"Improper JsValue: $key: $jsVal\n" +
        getMessageForDecoderError(json, id, parentId, rootId, nameStack)
    )
  }

  private def getMessageForDecoderError(
    json: JsValue,
    id: SqlValue,
    parentId: Option[SqlValue],
    rootId: Option[SqlValue],
    nameStack: Seq[String]
  ): String = s"Provided: $json\n" +
      s"Id: $id\n" +
      s"Parent Id: $parentId\n" +
      s"Root Id: $rootId\n" +
      s"Key stack: ${nameStack.mkString(" -> ")}"
}