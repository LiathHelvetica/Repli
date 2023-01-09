package lthv.utils

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.utils.exception.ImproperConfigException

import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Try
import cats.implicits._
import lthv.enums.EnumIdGenerationStrategy
import lthv.enums.EnumKafkaTopicStrategy
import lthv.enums.EnumSqlTableNamingStrategy
import lthv.kafka.FromCollectionName
import lthv.kafka.FromDbAndCollectionName
import lthv.kafka.KafkaTopicStrategy
import lthv.sql.model.FullPathStrategy
import lthv.sql.model.LastNameStrategy
import lthv.sql.model.SqlTableNamingStrategy
import lthv.utils.exception.EnumPropertyException
import lthv.utils.id.IdGenerationStrategy
import lthv.utils.id.RandomIdStrategy

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

object ConfigHelpers {

  val defaultConf: Config = ConfigFactory.load("default")

  def getStringProperty(path: String)(implicit conf: Config): String = {
    conf.getString(path)
  }

  def getStringPropertyWithFallback(path: String)(implicit conf: Config): String = {
    if (conf.hasPath(path)) conf.getString(path) else defaultConf.getString(path)
  }

  def getIntProperty(path: String)(implicit conf: Config): Int = {
    conf.getInt(path)
  }

  def getIntPropertyWithFallback(path: String)(implicit conf: Config): Int = {
    if (conf.hasPath(path)) conf.getInt(path) else defaultConf.getInt(path)
  }

  def getBooleanProperty(path: String)(implicit conf: Config): Boolean = {
    conf.getBoolean(path)
  }

  def getBooleanPropertyWithFallback(path: String)(implicit conf: Config): Boolean = {
    if (conf.hasPath(path)) conf.getBoolean(path) else defaultConf.getBoolean(path)
  }

  def getFiniteDurationProperty(path: String)(implicit conf: Config): FiniteDuration = {
    FiniteDuration(conf.getDuration(path).toNanos, TimeUnit.NANOSECONDS)
  }

  def getFiniteDurationPropertyWithFallback(path: String)(implicit conf: Config): FiniteDuration = {
    if (conf.hasPath(path)) getFiniteDurationProperty(path) else getFiniteDurationProperty(path)(defaultConf)
  }

  def getCharArrayProperty(path: String)(implicit conf: Config): Array[Char] = {
    getStringProperty(path).toCharArray
  }

  def getConfig(path: String)(implicit conf: Config): Config = {
    conf.getConfig(path)
  }

  def getStringSeqProperty(path: String)(implicit conf: Config): Seq[String] = {
    conf.getStringList(path).asScala.toSeq
  }

  def getUrlSeqProperty[OUT](path: String, transform: (String, String) => OUT)(implicit config: Config): Try[Seq[OUT]] = {
    getStringSeqProperty(path).map(s => {
      val parts = s.split(":").toSeq
      if (parts.size != 2) {
        Failure(ImproperConfigException(path, s))
      } else {
        Try {
          transform(parts.head, parts.last)
        }.recoverWith { case _ => Failure(ImproperConfigException(path, s)) }
      }
    }).sequence
  }

  def getKafkaTopicStrategyWithFallback(path: String)(implicit conf: Config): Try[KafkaTopicStrategy] = {
    val e = getStringPropertyWithFallback(path)
    Try {
      EnumKafkaTopicStrategy.withName(e)
    } match {
      case Success(v) => Success(v match {
        case EnumKafkaTopicStrategy.FromCollectionName => FromCollectionName
        case EnumKafkaTopicStrategy.FromDbAndCollectionName => FromDbAndCollectionName
      })
      case Failure(_) => Failure(EnumPropertyException(path, e, EnumKafkaTopicStrategy.values.map(v => v.toString)))
    }
  }

  def getSqlTableNamingStrategyWithFallback(path: String)(implicit conf: Config): Try[SqlTableNamingStrategy] = {
    val e = getStringPropertyWithFallback(path)
    Try {
      EnumSqlTableNamingStrategy.withName(e)
    } match {
      case Success(v) => Success(v match {
        case EnumSqlTableNamingStrategy.FullPathStrategy => FullPathStrategy
        case EnumSqlTableNamingStrategy.LastNameStrategy => LastNameStrategy
      })
      case Failure(_) => Failure(EnumPropertyException(path, e, EnumSqlTableNamingStrategy.values.map(v => v.toString)))
    }
  }

  def getIdGenerationStrategyWithFallback(path: String)(implicit conf: Config): Try[IdGenerationStrategy] = {
    val e = getStringPropertyWithFallback(path)
    Try {
      EnumIdGenerationStrategy.withName(e)
    } match {
      case Success(v) => Success(v match {
        case EnumIdGenerationStrategy.RandomIdStrategy => RandomIdStrategy(conf)
      })
      case Failure(_) => Failure(EnumPropertyException(path, e, EnumIdGenerationStrategy.values.map(v => v.toString)))
    }
  }

  def getTopicName(implicit conf: Config): String = {
    val prefix = getStringPropertyWithFallback("repli.importer.source.kafka.topic.prefix")
    val topic = getStringProperty("repli.importer.source.kafka.topic")
    prefix + topic
  }
}
