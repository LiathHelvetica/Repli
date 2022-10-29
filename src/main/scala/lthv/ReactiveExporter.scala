package lthv

/*import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import lthv.utils.ConfigHelpers.getIntProperty
import lthv.utils.ConfigHelpers.getStringProperty
import reactivemongo.akkastream.State
import reactivemongo.akkastream.cursorProducer
import reactivemongo.api.AsyncDriver
import reactivemongo.api.Cursor
import reactivemongo.api.MongoConnection
import reactivemongo.api.MongoConnectionOptions
import reactivemongo.api.MongoConnectionOptions.Credential
import reactivemongo.api.ScramSha256Authentication
import reactivemongo.api.bson.BSONDocument
import reactivemongo.api.bson.collection.BSONCollection

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Success*/

object ReactiveExporter extends App {

  /*implicit val conf: Config = ConfigFactory.load("export")
  implicit val actorSystem: ActorSystem = ActorSystem()

  val driver = AsyncDriver()
  val uri = MongoConnection.fromString(getStringProperty("repli.exporter.target.uri"))
  val connection = uri.flatMap(u => driver.connect(u))*/


  /*val connection = driver.connect(
    List(getStringProperty("repli.exporter.target.host")),
    MongoConnectionOptions(
      authenticationDatabase = Some(getStringProperty("repli.exporter.target.authDb")),
      credentials = Map(
        getStringProperty("repli.exporter.target.db") ->
        Credential(
          getStringProperty("repli.exporter.target.user"),
          Some(getStringProperty("repli.exporter.target.password"))
        )
      )
    )
  )*/


  /*val db = connection.flatMap(c => c.database(getStringProperty("repli.exporter.target.db")))
  val collectionFuture: Future[BSONCollection] = db.map(db => db.collection[BSONCollection](getStringProperty("repli.exporter.target.collection")))

  val collection = Await.result(collectionFuture, Duration.Inf)

  val source: Source[BSONDocument, Future[State]] = collection.find(BSONDocument()).cursor[BSONDocument]().documentSource(100, Cursor.FailOnError())

  val parsingFlow: Flow[BSONDocument, String, NotUsed] = Flow[BSONDocument]
    .mapAsyncUnordered(getIntProperty("repli.exporter.parallelism")) {
      document =>
        Future {
          println("HERE")
          document.toString()
        }
    }

  // TODO: parallelism on sending to kafka
  val producerSink: Sink[String, Future[Int]] = Sink
    .fold[Int, String](0)((acc, json) => {
      println(json)
      acc + 1
    })

  val graph: RunnableGraph[Future[Int]] = source.via(parsingFlow).toMat(producerSink)(Keep.right)
  Await.ready(
    graph.run,
    Duration.Inf
  ).value.get match {
    case Success(v) => println(s"emitted $v documents")
    case _ => println("failure")
  }

  Await.result(driver.close(), Duration.Inf)
  actorSystem.terminate()*/
}
