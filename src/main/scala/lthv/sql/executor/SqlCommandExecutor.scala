package lthv.sql.executor

import scalikejdbc.NamedDB
import scalikejdbc.NoExtractor
import scalikejdbc.SQL

object SqlCommandExecutor {

  def executeCommands(poolName: String, commands: Seq[SQL[Nothing, NoExtractor]]): Unit = {
    NamedDB(poolName) localTx { implicit session =>
      commands.foreach(c => c.execute.apply())
    }
  }
}
