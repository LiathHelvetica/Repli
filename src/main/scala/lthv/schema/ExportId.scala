package lthv.schema

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.StaticConfig.mongoIdKey
import lthv.utils.StaticConfig.nullString
import org.bson.BsonDbPointer
import org.mongodb.scala.bson.BsonBinary
import org.mongodb.scala.bson.BsonBoolean
import org.mongodb.scala.bson.BsonDateTime
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.BsonJavaScript
import org.mongodb.scala.bson.BsonJavaScriptWithScope
import org.mongodb.scala.bson.BsonMaxKey
import org.mongodb.scala.bson.BsonMinKey
import org.mongodb.scala.bson.BsonNull
import org.mongodb.scala.bson.BsonNumber
import org.mongodb.scala.bson.BsonObjectId
import org.mongodb.scala.bson.BsonRegularExpression
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.bson.BsonSymbol
import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.bson.BsonUndefined
import play.api.libs.json.JsObject
import play.api.libs.json.JsString

case class ExportId(id: Array[Byte], tag: String)

object ExportId {

  def apply(bson: BsonDocument)(implicit conf: Config): ExportId = {

    val idValue = bson.get(mongoIdKey)
    val idTag = idValue.getBsonType.name
    val charset = getStringPropertyWithFallback("repli.schema.exportIdCharset")

    new ExportId(
      idValue match {
        case b: BsonBinary => b.getData
        case b: BsonBoolean => b.getValue.toString.getBytes(charset)
        case d: BsonDateTime => d.toString.getBytes(charset)
        case d: BsonDbPointer => d.getId.toString.getBytes(charset)
        case d: BsonDocument => d.toString.getBytes(charset)
        case j: BsonJavaScript => j.getCode.getBytes(charset)
        case j: BsonJavaScriptWithScope => j.getCode.getBytes(charset)
        case _: BsonMaxKey => getStringPropertyWithFallback("repli.schema.maxKeyValue").getBytes(charset)
        case _: BsonMinKey => getStringPropertyWithFallback("repli.schema.minKeyValue").getBytes(charset)
        case _: BsonNull => nullString.getBytes(charset)
        case n: BsonNumber => n.doubleValue.toString.getBytes(charset)
        case i: BsonObjectId => i.getValue.toString.getBytes(charset)
        case r: BsonRegularExpression => JsObject(Seq(
          getStringPropertyWithFallback("repli.schema.regExPatternKey") -> JsString(r.getPattern),
          getStringPropertyWithFallback("repli.schema.regExOptionsKey") -> JsString(r.getOptions)
        )).toString.getBytes(charset)
        case s: BsonString => s.getValue.getBytes(charset)
        case s: BsonSymbol => s.getSymbol.getBytes(charset)
        case t: BsonTimestamp => t.getValue.toString.getBytes(charset)
        case _: BsonUndefined => nullString.getBytes(charset)
        case _ => sys.error(s"Id of type $idTag is not supported")
      },
      idTag
    )
  }
}