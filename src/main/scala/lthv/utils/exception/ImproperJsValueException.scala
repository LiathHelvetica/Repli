package lthv.utils.exception

import lthv.sql.model.value.SqlValue
import play.api.libs.json.JsObject
import play.api.libs.json.JsValue

case class ImproperJsValueException(msg: String) extends Exception(msg)

object ImproperJsValueException {

  def apply(
    key: String,
    jsObj: JsObject,
    valueType: String
  ): ImproperJsValueException = {
    new ImproperJsValueException(
      s"Illegal leaf value of type $valueType\n" +
        s"Provided value $key: $jsObj"
    )
  }

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

  def apply(
    typeKey: String,
    jsV: JsValue,
    json: JsObject,
    id: SqlValue,
    parentId: Option[SqlValue],
    rootId: SqlValue,
    nameStack: Seq[String]
  ): ImproperJsValueException = {
    new ImproperJsValueException(
      "Improper type definition in RepliSchema - types should be passed as Strings\n" +
        s"Improper JsValue: $typeKey: $jsV\n" +
        getMessageForDecoderError(json, id, parentId, Some(rootId), nameStack)
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