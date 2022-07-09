package com.rockthejvm

import java.time.LocalDate
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object PrivateExecutionContext {

  val executor: ExecutorService = Executors.newFixedThreadPool(4)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)
}


object Main {
  import slick.jdbc.PostgresProfile.api._
  import PrivateExecutionContext._

  val bladeRunner: Movie = Movie(1L, "Blade Runner", LocalDate.of(1982, 6, 25), 117)
  val TheHolyGrail: Movie = Movie(2L, "Monty Python and the Holy Grail", LocalDate.of(1975, 5, 25), 91)

  // CREATE
  def demoInsertMovie(): Unit = {
    val queryDescription = SlickTables.movieTable += TheHolyGrail
    val futureId: Future[Int] = Connection.db.run(queryDescription)
    futureId.onComplete {
      case Success(newMovieId) => println(s"the Query was successful, new id is $newMovieId")
      case Failure(ex) => println(s"Query failed, reason: $ex")
    }

    Thread.sleep(10000)
  }

    // READ
    def demoReadAllMovies(): Unit = {
      val resultFuture: Future[Seq[Movie]]= Connection.db.run(SlickTables.movieTable.result) // select * from ...
      resultFuture.onComplete{
        case Success(movies) => println(s"Fetched: ${movies.mkString(",")}")
        case Failure(ex) => println(s"Fetching failed: $ex")
      }
      Thread.sleep(10000)
    }

  // READ
  def demoReadSomeMovies(): Unit = {
    val resultFuture: Future[Seq[Movie]]= Connection.db.run(SlickTables.movieTable.filter(_.name.like("%Runner%")).result)
    // select * from ... where name like "Runner"
    resultFuture.onComplete{
      case Success(movies) => println(s"Fetched: ${movies.mkString(",")}")
      case Failure(ex) => println(s"Fetching failed: $ex")
    }
    Thread.sleep(10000)
  }

  // UPDATE
  def demoUpdate(): Unit = {
    val queryDescriptor = SlickTables.movieTable.filter(_.id === 1L).update(bladeRunner.copy(lengthInMin = 95))
    val futureId: Future[Int] = Connection.db.run(queryDescriptor)
    futureId.onComplete {
      case Success(newMovieId) => println(s"the Query was successful, new id is $newMovieId")
      case Failure(ex) => println(s"Query failed, reason: $ex")
    }
    Thread.sleep(10000)
  }

    // DELETE
    def demoDelete(): Unit = {
      Connection.db.run(SlickTables.movieTable.filter(_.name.like("%Runner%")).delete)
      Thread.sleep(5000)
    }




  def main(args: Array[String]): Unit ={
    // CREATE
//    demoInsertMovie()
    // READ
//    demoReadAllMovies()
//    demoReadSomeMovies()

    //UPDATE
//    demoUpdate()

    // DELETE
    demoDelete()

  }

}
