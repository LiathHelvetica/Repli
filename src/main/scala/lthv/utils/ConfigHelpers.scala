package lthv.utils

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

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

  def getCharArrayProperty(path: String)(implicit conf: Config): Array[Char] = {
    getStringProperty(path).toCharArray
  }

  def getConfig(path: String)(implicit conf: Config): Config = {
    conf.getConfig(path)
  }
}
