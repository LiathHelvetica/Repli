package lthv.utils.id

import com.typesafe.config.Config
import lthv.utils.ConfigHelpers.getIntPropertyWithFallback
import org.joda.time.DateTimeUtils.currentTimeMillis

import scala.util.Random

class IdGeneratorImpl(stringPartPrecision: Int) extends IdGenerator {

  val RNG = new Random()

  override def generateId: String = this.synchronized {
    val time: String = currentTimeMillis.toString
    val random: String = RNG.alphanumeric.take(stringPartPrecision).mkString
    time + random
  }
}

object IdGeneratorImpl {

  def apply(implicit conf: Config): IdGeneratorImpl = {
    new IdGeneratorImpl(getIntPropertyWithFallback("repli.id.stringPartPrecision"))
  }
}
