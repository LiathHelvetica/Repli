package lthv.schema.encode

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue

trait RepliSchema[B] {
  val tag: Option[String]

  protected def toTaggedSchema(v: JsValue, detailFields: JsObject = JsObject.empty)(implicit conf: Config): JsObject = {
    JsObject(Seq(
      getStringPropertyWithFallback("repli.schema.valueKey") -> v
    ) ++ tag.map(s => getStringPropertyWithFallback("repli.schema.typeKey") -> JsString(s))
    ) ++ detailFields
  }

  def encode(b: B)(implicit conf: Config): JsValue
}

trait RepliSchemaWithId[B] extends RepliSchema[B] {
  def getId(b: B)(implicit conf: Config): ExportId
}

trait RootRepliSchema[B] extends RepliSchemaWithId[B] {
  val exportIdProvider: ExportIdProvider[B]

  override def getId(b: B)(implicit conf: Config): ExportId = {
    exportIdProvider.getIdFrom(b)
  }
}
