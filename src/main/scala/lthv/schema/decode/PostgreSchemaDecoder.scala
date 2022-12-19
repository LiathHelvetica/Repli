package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.SqlRow
import lthv.sql.model.SqlTableName
import lthv.sql.model.SqlTableNamingStrategy
import lthv.sql.model.value.PostgreSqlValueCreator
import lthv.sql.model.value.SqlNull
import lthv.sql.model.value.SqlValue
import lthv.sql.model.value.SqlValueCreator
import lthv.utils.ConfigHelpers.getSqlTableNamingStrategyWithFallback
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.Converters.Base64String
import lthv.utils.Converters.OptionTryConvertible
import lthv.utils.exception.ImproperJsValueException
import lthv.utils.id.IdGenerationStrategy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.bson.BsonType
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.JsArray
import play.api.libs.json.JsObject
import play.api.libs.json.JsString
import play.api.libs.json.JsValue
import play.api.libs.json.Json

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object PostgreSchemaDecoder extends SqlSchemaDecoder {

  private val exportIdDecoder: ExportIdDecoder[SqlValue] = PostgreExportIdDecoder
  private implicit val sqlValueCreator: SqlValueCreator = PostgreSqlValueCreator

  override def decode(
    message: ConsumerRecord[Array[Byte], Array[Byte]]
  )(implicit idGenerationStrategy: IdGenerationStrategy, conf: Config): Try[SqlDecoderResult] = {
    val idTry = exportIdDecoder.decode(message)
    val jsonTry = Try { Json.parse(message.value) }
    val nameStack = List(message.topic)
    val tableNamingStrategyTry = getSqlTableNamingStrategyWithFallback("repli.importer.destination.sql.tableName.strategy")

    for {
      id <- idTry
      json <- jsonTry
      tableNamingStrategy <- tableNamingStrategyTry
      outcome <- decode(json, id, None, None, nameStack)(tableNamingStrategy, idGenerationStrategy, conf)
    } yield {
      val results = Seq(outcome._1) ++ outcome._2
      SqlDecoderResult(results)
    } // wrap in more general exception + caused by

  }

  private def decode(
    json: JsValue,
    id: SqlValue,
    parentId: Option[SqlValue],
    rootId: Option[SqlValue],
    nameStack: Seq[String]
  )(implicit
    tableNamingStrategy: SqlTableNamingStrategy,
    idGenerationStrategy: IdGenerationStrategy,
    conf: Config
  ): Try[((SqlTableName, SqlRow), Seq[(SqlTableName, SqlRow)])] = {

    val tableName = tableNamingStrategy.getTableName(nameStack)

    json match {
      case obj: JsObject => {
        obj.fields.foldLeft[Try[((SqlTableName, SqlRow), Seq[(SqlTableName, SqlRow)])]](Success((
          (tableName, SqlRow(id, parentId, rootId)),
          Seq.empty
        )))((acc, jsField) => (acc, jsField) match {
          case (Success(acc), (key: String, jsObj: JsObject)) => decode(acc, key, jsObj, id, parentId, rootId.getOrElse(id), nameStack)

          // TODO: to separate method
          case (Success(acc), (key: String, jsArr: JsArray)) => {

            val typeKey = getStringPropertyWithFallback("repli.schema.typeKey")
            val nameStackForArr = nameStack :+ key
            val tableName = tableNamingStrategy.getTableName(nameStackForArr)

            val childrenTry = jsArr.value.foldLeft[Try[Seq[(SqlTableName, SqlRow)]]](
              Success(Seq.empty)
            )((childrenAccTry, jsV) => {
              (childrenAccTry, jsV) match {
                case (Success(childrenAcc), jsObj: JsObject) if jsObj.value.contains(typeKey) => for {
                  valueType <- jsObj.value.get(typeKey).toTry(new Exception("err")) // TODO: better exception - refactor this
                  values <- decodeAppendedSqlValue(typeKey, key, jsObj, valueType, id, parentId, rootId.getOrElse(id), nameStackForArr)
                } yield childrenAcc :+ (tableName, SqlRow(idGenerationStrategy.getId, Some(id), rootId.orElse(Some(id)), values))
                case (Success(childrenAcc), jsObj: JsObject) => for {
                  children <- decodeSubRows(key, jsObj, id, rootId.getOrElse(id), nameStack)
                } yield childrenAcc ++ children
                case (_, jsV: JsValue) => Failure(ImproperJsValueException(jsV, key, jsArr, json, id, parentId, rootId, nameStackForArr))
                case (accTry@Failure(_), _) => accTry
              }
            })
            val (currentRow, currentChildren) = acc
            childrenTry.map(children => (currentRow, currentChildren ++ children))
          }

          case (Success(_), (key: String, jsVal: JsValue)) => {
            Failure(ImproperJsValueException(key, jsVal, json, id, parentId, rootId, nameStack))
          }
          case (accTry@Failure(_), _) => accTry
        })
      }
      case _ => Failure(ImproperJsValueException(json, id, parentId, rootId, nameStack))
    }
  }

  // any js object - append to acc
  private def decode(
    acc: ((SqlTableName, SqlRow), Seq[(SqlTableName, SqlRow)]),
    key: String,
    jsObj: JsObject,
    currentId: SqlValue,
    parentId: Option[SqlValue],
    rootId: SqlValue,
    nameStack: Seq[String]
  )(implicit
    tableNamingStrategy: SqlTableNamingStrategy,
    idGenerationStrategy: IdGenerationStrategy,
    conf: Config
  ): Try[((SqlTableName, SqlRow), Seq[(SqlTableName, SqlRow)])] = {

    val (parentRow, childRows) = acc
    val typeKey = getStringPropertyWithFallback("repli.schema.typeKey")

    jsObj.value.get(typeKey) match {
      case Some(valueType: JsValue) => for {
        valuesToAppend <- decodeAppendedSqlValue(typeKey, key, jsObj, valueType, currentId, parentId, rootId, nameStack)
      } yield {
        val (tableName, row) = parentRow
        ((tableName, row.copy(values = row.values ++ valuesToAppend)), childRows)
      }
      case None => for { // one could insert child id to parent row but its redundant
        subRows <- decodeSubRows(key, jsObj, currentId, rootId, nameStack)
      } yield (parentRow, childRows ++ subRows)
      // case Some(jsV) => Failure(ImproperJsValueException(typeKey, jsV, jsObj, currentId, parentId, rootId, nameStack))
    }
  }

  // any non-leaf object
  private def decodeSubRows(
    key: String,
    jsObj: JsObject,
    currentId: SqlValue,
    rootId: SqlValue,
    nameStack: Seq[String]
  )(implicit
    tableNamingStrategy: SqlTableNamingStrategy,
    idGenerationStrategy: IdGenerationStrategy,
    conf: Config
  ): Try[Seq[(SqlTableName, SqlRow)]] = {
    val subObjDecodedTry = decode(jsObj, idGenerationStrategy.getId, Some(currentId), Some(rootId), nameStack :+ key)
    for {
      subObjDecoded <- subObjDecodedTry
    } yield List(subObjDecoded._1) ++ subObjDecoded._2
  }

  // leaf object
  private def decodeAppendedSqlValue(
    typeKey: String,
    key: String,
    jsObj: JsObject,
    valueType: JsValue,
    currentId: SqlValue,
    parentId: Option[SqlValue],
    rootId: SqlValue,
    nameStack: Seq[String]
  )(implicit conf: Config): Try[Map[String, SqlValue]] = {
    valueType match {
      case vType: JsString => decodeAppendedSqlValue(key, jsObj, vType.value)
      case _ => Failure(ImproperJsValueException(typeKey, valueType, jsObj, currentId, parentId, rootId, nameStack))
    }
  }

  // leaf object
  private def decodeAppendedSqlValue(
    key: String,
    jsObj: JsObject,
    valueType: String
  )(implicit conf: Config): Try[Map[String, SqlValue]] = {

    val valueKey = getStringPropertyWithFallback("repli.schema.valueKey")
    val subtypeKey = getStringPropertyWithFallback("repli.schema.subtypeKey")
    val sqlSeparator = getStringPropertyWithFallback("repli.importer.destination.sql.columns.separator")

    Try { BsonType.valueOf(valueType) } match {
      case Success(BsonType.BINARY) => for {
        v <- Try { (jsObj \ valueKey).as[String] }.map(_.fromBase64)
        t <- Try { (jsObj \ subtypeKey).as[Int] }
      } yield {
        val subtypeKey = key + sqlSeparator + getStringPropertyWithFallback("repli.schema.sql.binary.type.suffix")
        Map(key -> sqlValueCreator.createSqlBinary(v), subtypeKey -> sqlValueCreator.createSqlNumber(t))
      }
      case Success(BsonType.BOOLEAN) => for {
        b <- Try { (jsObj \ valueKey).as[Boolean] }
      } yield Map(key -> sqlValueCreator.createSqlBoolean(b))
      case Success(BsonType.DATE_TIME) => for {
        dtStr <- Try { (jsObj \ valueKey).as[String] }
        format <- Try { (jsObj \ subtypeKey).as[String] }
        formatter <- Try { DateTimeFormat.forPattern(format) }
        dt <- Try { formatter.parseDateTime(dtStr) }
      } yield Map(key -> sqlValueCreator.createSqlDateTime(dt))
      case Success(BsonType.DB_POINTER) => for {
        ptr <- Try { (jsObj \ valueKey).as[String] }
        namespace <- Try { (jsObj \ getStringPropertyWithFallback("repli.schema.dbPointerNamespaceKey")).as[String] }
      } yield {
        val namespaceKey = key + sqlSeparator + getStringPropertyWithFallback("repli.schema.sql.dbPointer.namespace.suffix")
        Map(key -> sqlValueCreator.createSqlText(ptr), namespaceKey -> sqlValueCreator.createSqlText(namespace))
      }
      case Success(BsonType.JAVASCRIPT) | Success(BsonType.JAVASCRIPT_WITH_SCOPE) | Success(BsonType.OBJECT_ID) |
           Success(BsonType.STRING) | Success(BsonType.SYMBOL) | Success(BsonType.MIN_KEY) | Success(BsonType.MAX_KEY) => for {
        s <- Try { (jsObj \ valueKey).as[String] }
      } yield Map(key -> sqlValueCreator.createSqlText(s))
      case Success(BsonType.NULL) | Success(BsonType.UNDEFINED) => Success(Map(key -> SqlNull))
      case Success(BsonType.DOUBLE) => for {
        d <- Try { (jsObj \ valueKey).as[BigDecimal] }
      } yield Map(key -> sqlValueCreator.createSqlNumber(d))
      case Success(BsonType.REGULAR_EXPRESSION) => for {
        s <- Try { (jsObj \ valueKey).as[String] }
        opt <- Try { (jsObj \ getStringPropertyWithFallback("repli.schema.regExOptionsKey")).as[String] }
      } yield {
        val optionsKey = key + sqlSeparator + getStringPropertyWithFallback("repli.schema.sql.regEx.options.suffix")
        Map(key -> sqlValueCreator.createSqlText(s), optionsKey -> sqlValueCreator.createSqlText(opt))
      }
      case Success(BsonType.TIMESTAMP) => for {
        v <- Try { (jsObj \ valueKey).as[Int] }
      } yield Map(key -> sqlValueCreator.createSqlDateTime(new DateTime(v)))
      case _ => Failure(ImproperJsValueException(key, jsObj, valueType))
    }
  }
}
