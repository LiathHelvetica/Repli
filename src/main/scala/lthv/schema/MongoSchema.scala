package lthv.schema

import com.typesafe.config.Config
import lthv.utils.Converters.toBase64
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import org.bson.BsonDbPointer
import org.bson.BsonType
import org.joda.time.format.DateTimeFormat.forPattern
import org.mongodb.scala.bson.BsonArray
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
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.Document
import play.api.libs.json.JsArray
import play.api.libs.json.JsBoolean
import play.api.libs.json.JsNull
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue

import java.util.Map.Entry
import scala.jdk.CollectionConverters._

object ArraySchema extends Schema[BsonArray] {
  val tag: Option[String] = Some(BsonType.ARRAY.name)

  def encode(b: BsonArray)(implicit conf: Config): JsValue = {
    JsArray(b.getValues.asScala.map(b => MongoSchema.encode(b)))
  }
}

object BinarySchema extends Schema[BsonBinary] {
  val tag: Option[String] = Some(BsonType.BINARY.name)

  def encode(b: BsonBinary)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(b.getData),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.exporter.schema.subtypeKey") -> JsNumber(b.getType.toInt)
      ))
    )
  }
}

object BooleanSchema extends Schema[BsonBoolean] {
  val tag: Option[String] = Some(BsonType.BOOLEAN.name)

  def encode(b: BsonBoolean)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsBoolean(b.getValue)
    )
  }
}

object DateTimeSchema extends Schema[BsonDateTime] {
  val tag: Option[String] = Some(BsonType.DATE_TIME.name)

  def encode(d: BsonDateTime)(implicit conf: Config): JsValue = {

    val format = getStringPropertyWithFallback("repli.exporter.schema.dateTimeFormat")

    toTaggedSchema(
      JsString(forPattern(format).print(d.getValue)),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.exporter.schema.subtypeKey") -> JsString(format)
      ))
    )
  }
}

object DbPointerSchema extends Schema[BsonDbPointer] {
  val tag: Option[String] = Some(BsonType.DB_POINTER.name)

  def encode(ptr: BsonDbPointer)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(ptr.getId.toString),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.exporter.schema.dbPointerNamespaceKey") -> JsString(ptr.getNamespace)
      ))
    )
  }
}

object BsonDocumentSchema extends RootSchema[BsonDocument] {
  val tag: Option[String] = Some(BsonType.DOCUMENT.name)

  def encode(document: BsonDocument)(implicit conf: Config): JsValue = {
    document.entrySet().asScala.foldLeft(JsObject.empty)((json, entry) => json ++ MongoSchema.encode(entry))
  }

  override def getId(b: BsonDocument)(implicit conf: Config): ExportId = {
    ExportId(b)
  }
}

object DocumentSchema extends RootSchema[Document] {
  val tag: Option[String] = Some(BsonType.DOCUMENT.name)

  def encode(document: Document)(implicit conf: Config): JsValue = {
    BsonDocumentSchema.encode(document.toBsonDocument)
  }

  override def getId(d: Document)(implicit conf: Config): ExportId = {
    BsonDocumentSchema.getId(d.toBsonDocument)
  }
}

object JavaScriptSchema extends Schema[BsonJavaScript] {
  val tag: Option[String] = Some(BsonType.JAVASCRIPT.name)

  def encode(js: BsonJavaScript)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(js.getCode),
    )
  }
}

object ScopedJavaScriptSchema extends Schema[BsonJavaScriptWithScope] {
  val tag: Option[String] = Some(BsonType.JAVASCRIPT_WITH_SCOPE.name)

  def encode(js: BsonJavaScriptWithScope)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(js.getCode)
    )
  }
}

object MaxKeySchema extends Schema[BsonMaxKey] {
  val tag: Option[String] = Some(BsonType.MAX_KEY.name)

  def encode(mk: BsonMaxKey)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(getStringPropertyWithFallback("repli.exporter.schema.maxKeyValue"))
    )
  }
}

object MinKeySchema extends Schema[BsonMinKey] {
  val tag: Option[String] = Some(BsonType.MIN_KEY.name)

  def encode(mk: BsonMinKey)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(getStringPropertyWithFallback("repli.exporter.schema.minKeyValue"))
    )
  }
}

object NullSchema extends Schema[BsonNull] {
  val tag: Option[String] = Some(BsonType.NULL.name)

  def encode(n: BsonNull)(implicit conf: Config): JsValue = {
    toTaggedSchema(JsNull)
  }
}

object NumberSchema extends Schema[BsonNumber] {
  val tag: Option[String] = Some(BsonType.DOUBLE.name)

  def encode(n: BsonNumber)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsNumber(n.doubleValue())
    )
  }
}

object IdSchema extends Schema[BsonObjectId] {
  val tag: Option[String] = Some(BsonType.OBJECT_ID.name)

  def encode(id: BsonObjectId)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(id.getValue.toString)
    )
  }
}

object RegExSchema extends Schema[BsonRegularExpression] {
  val tag: Option[String] = Some(BsonType.REGULAR_EXPRESSION.name)

  def encode(reg: BsonRegularExpression)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(reg.getPattern),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.exporter.schema.regExOptionsKey") -> JsString(reg.getOptions)
      ))
    )
  }
}

object StringSchema extends Schema[BsonString] {
  val tag: Option[String] = Some(BsonType.STRING.name)

  def encode(s: BsonString)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(s.getValue)
    )
  }
}

object SymbolSchema extends Schema[BsonSymbol] {
  val tag: Option[String] = Some(BsonType.SYMBOL.name)

  def encode(s: BsonSymbol)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(s.getSymbol)
    )
  }
}

object TimestampSchema extends Schema[BsonTimestamp] {
  val tag: Option[String] = Some(BsonType.TIMESTAMP.name)

  def encode(t: BsonTimestamp)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsNumber(t.getValue)
    )
  }
}

object UndefinedSchema extends Schema[BsonUndefined] {
  val tag: Option[String] = Some(BsonType.UNDEFINED.name)

  def encode(u: BsonUndefined)(implicit conf: Config): JsValue = {
    toTaggedSchema(JsNull)
  }
}

object MongoSchema extends Schema[BsonValue] {

  val tag: Option[String] = None

  def encode(entry: Entry[String, BsonValue])(implicit conf: Config): JsObject = {
    JsObject(Seq(
      entry.getKey -> encode(entry.getValue)
    ))
  }

  override def encode(b: BsonValue)(implicit conf: Config): JsValue = {
    b match {
      case a: BsonArray => ArraySchema.encode(a)
      case b: BsonBinary => BinarySchema.encode(b)
      case b: BsonBoolean => BooleanSchema.encode(b)
      case d: BsonDateTime => DateTimeSchema.encode(d)
      case d: BsonDbPointer => DbPointerSchema.encode(d)
      case d: BsonDocument => BsonDocumentSchema.encode(d)
      case j: BsonJavaScript => JavaScriptSchema.encode(j)
      case j: BsonJavaScriptWithScope => ScopedJavaScriptSchema.encode(j)
      case m: BsonMaxKey => MaxKeySchema.encode(m)
      case m: BsonMinKey => MinKeySchema.encode(m)
      case n: BsonNull => NullSchema.encode(n)
      case n: BsonNumber => NumberSchema.encode(n)
      case i: BsonObjectId => IdSchema.encode(i)
      case r: BsonRegularExpression => RegExSchema.encode(r)
      case s: BsonString => StringSchema.encode(s)
      case s: BsonSymbol => SymbolSchema.encode(s)
      case t: BsonTimestamp => TimestampSchema.encode(t)
      case u: BsonUndefined => UndefinedSchema.encode(u)
    }
  }
}