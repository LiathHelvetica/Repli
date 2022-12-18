package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.value.SqlValue
import lthv.sql.model.value.SqlValueCreator
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import play.api.libs.json.JsObject

import scala.util.Try

trait MinMaxValueDecoderStrategy {
  def toSqlMinValue(jsObj: JsObject, sqlValueCreator: SqlValueCreator)(implicit conf: Config): Try[SqlValue]
  def toSqlMaxValue(jsObj: JsObject, sqlValueCreator: SqlValueCreator)(implicit conf: Config): Try[SqlValue]
}

object MinMaxAsStringDecoderStrategy extends MinMaxValueDecoderStrategy {

  override def toSqlMinValue(jsObj: JsObject, sqlValueCreator: SqlValueCreator)(implicit conf: Config): Try[SqlValue] = {
    toSqlText(jsObj, sqlValueCreator)
  }

  override def toSqlMaxValue(jsObj: JsObject, sqlValueCreator: SqlValueCreator)(implicit conf: Config): Try[SqlValue] = {
    toSqlText(jsObj, sqlValueCreator)
  }

  private def toSqlText(jsObj: JsObject, sqlValueCreator: SqlValueCreator)(implicit conf: Config): Try[SqlValue] = {
    for {
      s <- Try { (jsObj \ getStringPropertyWithFallback("repli.schema.typeKey")).as[String] }
    } yield sqlValueCreator.createSqlText(s)
  }
}
