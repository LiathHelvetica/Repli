package lthv.utils

import com.typesafe.config.Config

object ConfigHelpers {

  def getStringProperty(path: String)(implicit conf: Config): String = {
    conf.getString(path)
  }

  def getIntProperty(path: String)(implicit conf: Config): Int = {
    conf.getInt(path)
  }

  def getCharArrayProperty(path: String)(implicit conf: Config): Array[Char] = {
    getStringProperty(path).toCharArray
  }
}
