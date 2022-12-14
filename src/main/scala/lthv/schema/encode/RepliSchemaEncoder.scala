package lthv.schema.encode

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue

trait RepliSchemaEncoder[B] {
  val tag: Option[String]

  protected def toTaggedSchema(v: JsValue, detailFields: JsObject = JsObject.empty)(implicit conf: Config): JsObject = {
    JsObject(Seq(
      getStringPropertyWithFallback("repli.schema.valueKey") -> v
    ) ++ tag.map(s => getStringPropertyWithFallback("repli.schema.typeKey") -> JsString(s))
    ) ++ detailFields
  }

  def encode(b: B)(implicit conf: Config): JsValue
}

trait RepliSchemaWithIdEncoder[B] extends RepliSchemaEncoder[B] {
  def getId(b: B)(implicit conf: Config): ExportId
}

trait RootRepliSchemaEncoder[B] extends RepliSchemaWithIdEncoder[B] {
  val exportIdProvider: ExportIdEncoder[B]

  override def getId(b: B)(implicit conf: Config): ExportId = {
    exportIdProvider.encodeId(b)
  }
}
