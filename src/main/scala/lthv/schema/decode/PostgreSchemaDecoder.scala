package lthv.schema.decode

import com.typesafe.config.Config
import lthv.sql.model.SqlRow
import lthv.sql.model.SqlTableName
import lthv.sql.model.SqlTableNamingStrategy
import lthv.sql.model.value.SqlValue
import lthv.utils.ConfigHelpers.getIdGenerationStrategyWithFallback
import lthv.utils.ConfigHelpers.getSqlTableNamingStrategyWithFallback
import lthv.utils.ConfigHelpers.getStringPropertyWithFallback
import lthv.utils.exception.ImproperJsValueException
import lthv.utils.id.IdGenerationStrategy
import org.apache.kafka.clients.consumer.ConsumerRecord
import play.api.libs.json.JsArray
import play.api.libs.json.JsObject
import play.api.libs.json.JsValue
import play.api.libs.json.Json

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object PostgreSchemaDecoder extends SqlSchemaDecoder {

  private val exportIdDecoder: ExportIdDecoder[SqlValue] = PostgreExportIdDecoder

  override def decode(message: ConsumerRecord[Array[Byte], Array[Byte]])(implicit conf: Config): Try[SqlDecoderResult] = {
    val idTry = exportIdDecoder.decode(message)
    val jsonTry = Try { Json.parse(message.value) }
    val nameStack = List(message.topic)
    val tableNamingStrategyTry = getSqlTableNamingStrategyWithFallback("repli.importer.destination.sql.tableName.strategy")
    val idGenerationStrategyTry = getIdGenerationStrategyWithFallback("repli.importer.destination.sql.id.generation.strategy")

    for {
      id <- idTry
      json <- jsonTry
      tableNamingStrategy <- tableNamingStrategyTry
      idGenerationStrategy <- idGenerationStrategyTry
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
          Seq.empty[(SqlTableName, SqlRow)]
        )))((acc, jsField) => (acc, jsField) match {
          case (Success(acc), (key: String, jsObj: JsObject)) => ??? // rootId.getOrElse(id)
          case (Success(acc), (key: String, jsArr: JsArray)) => ???
          case (Success(_), (key: String, jsVal: JsValue)) => {
            Failure(ImproperJsValueException(key, jsVal, json, id, parentId, rootId, nameStack))
          }
          case (accTry@Failure(_), _) => accTry
        })
      }
      case jsValue => Failure(ImproperJsValueException(json, id, parentId, rootId, nameStack))
    }
  }

  private def decode(
    acc: ((SqlTableName, SqlRow), Seq[(SqlTableName, SqlRow)]),
    key: String,
    jsObj: JsObject,
    currentId: SqlValue,
    rootId: SqlValue,
    nameStack: Seq[String]
  )(implicit
    tableNamingStrategy: SqlTableNamingStrategy,
    idGenerationStrategy: IdGenerationStrategy,
    conf: Config
  ): Try[((SqlTableName, SqlRow), Seq[(SqlTableName, SqlRow)])] =
    jsObj.value.get(getStringPropertyWithFallback("repli.schema.typeKey")) match {
      case Some(valueType) => ???
      case None => {
        val subObjDecoded = decode(jsObj, )
      }
    }

}
