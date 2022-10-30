package lthv.schema

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue

trait Schema[B] {
  val tag: Option[String]

  protected def toTaggedSchema(v: JsValue, detailFields: JsObject = JsObject.empty)(implicit conf: Config): JsObject = {
    JsObject(Seq(
      getStringPropertyWithFallback("repli.exporter.schema.valueKey") -> v
    ) ++ tag.map(s => getStringPropertyWithFallback("repli.exporter.schema.typeKey") -> JsString(s))
    ) ++ detailFields
  }

  def encode(b: B)(implicit conf: Config): JsValue
}

trait RootSchema[B] extends Schema[B] {
  def getId(b: B)(implicit conf: Config): ExportId
}
