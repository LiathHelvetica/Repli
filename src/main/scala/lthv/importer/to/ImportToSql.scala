package lthv.importer.to

import akka.Done
import akka.stream.scaladsl.Sink
import lthv.sql.executor.SqlCommandExecutor
import scalikejdbc.NoExtractor
import scalikejdbc.SQL

import scala.concurrent.Future

class ImportToSql(connectionPoolName: String) extends ImportTo[(SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), Future[Done]] {

  override val sink: Sink[(SQL[Nothing, NoExtractor], SQL[Nothing, NoExtractor]), Future[Done]] = Sink.foreach(commands => {
    val c = Seq(commands._1, commands._2)
    SqlCommandExecutor.executeCommands(connectionPoolName, c)
  })
}

object ImportToSql {
  def apply(connectionPoolName: String): ImportToSql = new ImportToSql(connectionPoolName)
}
