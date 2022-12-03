package lthv.schema.encode

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.Converters.toBase64
import lthv.utils.StaticConfig.mongoIdKey
import org.bson.BsonDbPointer
import org.bson.BsonType
import org.joda.time.format.DateTimeFormat.forPattern
import org.mongodb.scala.Document
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
import play.api.libs.json.JsArray
import play.api.libs.json.JsBoolean
import play.api.libs.json.JsNull
import play.api.libs.json.JsNumber
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue

import java.util.Map.Entry
import scala.jdk.CollectionConverters._

object ArraySchemaEncoder extends RepliSchemaEncoder[BsonArray] {
  val tag: Option[String] = Some(BsonType.ARRAY.name)

  def encode(b: BsonArray)(implicit conf: Config): JsValue = {
    JsArray(b.getValues.asScala.map(b => MongoSchemaEncoder.encode(b)))
  }
}

object BinarySchemaEncoder extends RepliSchemaEncoder[BsonBinary] {
  val tag: Option[String] = Some(BsonType.BINARY.name)

  def encode(b: BsonBinary)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(b.getData),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.schema.subtypeKey") -> JsNumber(b.getType.toInt)
      ))
    )
  }
}

object BooleanSchemaEncoder extends RepliSchemaEncoder[BsonBoolean] {
  val tag: Option[String] = Some(BsonType.BOOLEAN.name)

  def encode(b: BsonBoolean)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsBoolean(b.getValue)
    )
  }
}

object DateTimeSchemaEncoder extends RepliSchemaEncoder[BsonDateTime] {
  val tag: Option[String] = Some(BsonType.DATE_TIME.name)

  def encode(d: BsonDateTime)(implicit conf: Config): JsValue = {

    val format = getStringPropertyWithFallback("repli.schema.dateTimeFormat")

    toTaggedSchema(
      JsString(forPattern(format).print(d.getValue)),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.schema.subtypeKey") -> JsString(format)
      ))
    )
  }
}

object DbPointerSchemaEncoder extends RepliSchemaEncoder[BsonDbPointer] {
  val tag: Option[String] = Some(BsonType.DB_POINTER.name)

  def encode(ptr: BsonDbPointer)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(ptr.getId.toString),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.schema.dbPointerNamespaceKey") -> JsString(ptr.getNamespace)
      ))
    )
  }
}

object BsonDocumentSchemaEncoder extends RootRepliSchemaEncoder[BsonDocument] {
  val tag: Option[String] = Some(BsonType.DOCUMENT.name)
  val exportIdProvider: ExportIdProvider[BsonDocument] = MongoExportIdProvider

  def encode(document: BsonDocument)(implicit conf: Config): JsValue = {
    document.entrySet().asScala.foldLeft(JsObject.empty)((json, entry) => json ++ MongoSchemaEncoder.encode(entry))
  }
}

object DocumentSchemaEncoder extends RepliSchemaWithIdEncoder[Document] {
  val tag: Option[String] = Some(BsonType.DOCUMENT.name)

  def encode(document: Document)(implicit conf: Config): JsValue = {

    val d = document.toBsonDocument
    d.remove(mongoIdKey)

    BsonDocumentSchemaEncoder.encode(d)
  }

  override def getId(d: Document)(implicit conf: Config): ExportId = {
    BsonDocumentSchemaEncoder.getId(d.toBsonDocument)
  }
}

object JavaScriptSchemaEncoder extends RepliSchemaEncoder[BsonJavaScript] {
  val tag: Option[String] = Some(BsonType.JAVASCRIPT.name)

  def encode(js: BsonJavaScript)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(js.getCode),
    )
  }
}

object ScopedJavaScriptSchemaEncoder extends RepliSchemaEncoder[BsonJavaScriptWithScope] {
  val tag: Option[String] = Some(BsonType.JAVASCRIPT_WITH_SCOPE.name)

  def encode(js: BsonJavaScriptWithScope)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(js.getCode)
    )
  }
}

object MaxKeySchemaEncoder extends RepliSchemaEncoder[BsonMaxKey] {
  val tag: Option[String] = Some(BsonType.MAX_KEY.name)

  def encode(mk: BsonMaxKey)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(getStringPropertyWithFallback("repli.schema.maxKeyValue"))
    )
  }
}

object MinKeySchemaEncoder extends RepliSchemaEncoder[BsonMinKey] {
  val tag: Option[String] = Some(BsonType.MIN_KEY.name)

  def encode(mk: BsonMinKey)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(getStringPropertyWithFallback("repli.schema.minKeyValue"))
    )
  }
}

object NullSchemaEncoder extends RepliSchemaEncoder[BsonNull] {
  val tag: Option[String] = Some(BsonType.NULL.name)

  def encode(n: BsonNull)(implicit conf: Config): JsValue = {
    toTaggedSchema(JsNull)
  }
}

object NumberSchemaEncoder extends RepliSchemaEncoder[BsonNumber] {
  val tag: Option[String] = Some(BsonType.DOUBLE.name)

  def encode(n: BsonNumber)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsNumber(n.doubleValue())
    )
  }
}

object IdSchemaEncoder extends RepliSchemaEncoder[BsonObjectId] {
  val tag: Option[String] = Some(BsonType.OBJECT_ID.name)

  def encode(id: BsonObjectId)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(id.getValue.toString)
    )
  }
}

object RegExSchemaEncoder extends RepliSchemaEncoder[BsonRegularExpression] {
  val tag: Option[String] = Some(BsonType.REGULAR_EXPRESSION.name)

  def encode(reg: BsonRegularExpression)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(reg.getPattern),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.schema.regExOptionsKey") -> JsString(reg.getOptions)
      ))
    )
  }
}

object StringSchemaEncoder extends RepliSchemaEncoder[BsonString] {
  val tag: Option[String] = Some(BsonType.STRING.name)

  def encode(s: BsonString)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(s.getValue)
    )
  }
}

object SymbolSchemaEncoder extends RepliSchemaEncoder[BsonSymbol] {
  val tag: Option[String] = Some(BsonType.SYMBOL.name)

  def encode(s: BsonSymbol)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsString(s.getSymbol)
    )
  }
}

object TimestampSchemaEncoder extends RepliSchemaEncoder[BsonTimestamp] {
  val tag: Option[String] = Some(BsonType.TIMESTAMP.name)

  def encode(t: BsonTimestamp)(implicit conf: Config): JsValue = {
    toTaggedSchema(
      JsNumber(t.getTime),
      JsObject(Seq(
        getStringPropertyWithFallback("repli.schema.timestampValueKey") -> JsNumber(t.getValue)
      ))
    )
  }
}

object UndefinedSchemaEncoder extends RepliSchemaEncoder[BsonUndefined] {
  val tag: Option[String] = Some(BsonType.UNDEFINED.name)

  def encode(u: BsonUndefined)(implicit conf: Config): JsValue = {
    toTaggedSchema(JsNull)
  }
}

object MongoSchemaEncoder extends RepliSchemaEncoder[BsonValue] {

  val tag: Option[String] = None

  def encode(entry: Entry[String, BsonValue])(implicit conf: Config): JsObject = {
    JsObject(Seq(
      entry.getKey -> encode(entry.getValue)
    ))
  }

  override def encode(b: BsonValue)(implicit conf: Config): JsValue = {
    b match {
      case a: BsonArray => ArraySchemaEncoder.encode(a)
      case b: BsonBinary => BinarySchemaEncoder.encode(b)
      case b: BsonBoolean => BooleanSchemaEncoder.encode(b)
      case d: BsonDateTime => DateTimeSchemaEncoder.encode(d)
      case d: BsonDbPointer => DbPointerSchemaEncoder.encode(d)
      case d: BsonDocument => BsonDocumentSchemaEncoder.encode(d)
      case j: BsonJavaScript => JavaScriptSchemaEncoder.encode(j)
      case j: BsonJavaScriptWithScope => ScopedJavaScriptSchemaEncoder.encode(j)
      case m: BsonMaxKey => MaxKeySchemaEncoder.encode(m)
      case m: BsonMinKey => MinKeySchemaEncoder.encode(m)
      case n: BsonNull => NullSchemaEncoder.encode(n)
      case n: BsonNumber => NumberSchemaEncoder.encode(n)
      case i: BsonObjectId => IdSchemaEncoder.encode(i)
      case r: BsonRegularExpression => RegExSchemaEncoder.encode(r)
      case s: BsonString => StringSchemaEncoder.encode(s)
      case s: BsonSymbol => SymbolSchemaEncoder.encode(s)
      case t: BsonTimestamp => TimestampSchemaEncoder.encode(t)
      case u: BsonUndefined => UndefinedSchemaEncoder.encode(u)
    }
  }
}
