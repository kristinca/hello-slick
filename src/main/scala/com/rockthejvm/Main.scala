package com.rockthejvm

import slick.jdbc.GetResult

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
  val bladeRunner2049: Movie = Movie(3L, "Blade Runner 2049", LocalDate.of(2017,10, 5), 163)
  val meaningOfLife: Movie = Movie(10L, "Monty Python's The Meaning of Life", LocalDate.of(1983, 4,22), 107)
  val harrisonFord: Actor = Actor(1L, "Harrison Ford")
  val seanYoung: Actor = Actor(2L, "Sean Young")
  val terryJones: Actor = Actor(3L,"Terry Jones")
  val grahamChapman: Actor = Actor(4L, "Graham Chapman")
  val johnCleese: Actor = Actor(5L, "John Cleese")
  val terryGilliam: Actor = Actor(6L, "Terry Gilliam")
  val ericIdle: Actor = Actor(7L, "Eric Idle")
  val michaelPalin: Actor = Actor(8L, "Michael Palin")


  // CREATE
  def demoInsertMovie(): Unit = {
    val queryDescription = SlickTables.movieTable += bladeRunner2049
    val futureId: Future[Int] = Connection.db.run(queryDescription)
    futureId.onComplete {
      case Success(newMovieId) => println(s"the Query was successful, new id is $newMovieId")
      case Failure(ex) => println(s"Query failed, reason: $ex")
    }

    Thread.sleep(10000)
  }

  // CREATE
  def demoInsertActors(): Unit = {
    val queryDescription = SlickTables.actorTable ++= Seq(grahamChapman,johnCleese,terryGilliam, ericIdle,michaelPalin)
    val futureId = Connection.db.run(queryDescription)
    futureId.onComplete {
      case Success(_) => println(s"the Query was successful")
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
      Connection.db.run(SlickTables.movieTable.filter(_.name.like("%Grail%")).delete)
      Thread.sleep(5000)
    }

  def readMoviesByPlainQuery(): Future[Vector[Movie]]={
    implicit val getResultMovie: GetResult[Movie] =
    // parsing the : [id, name, localDate, lengthInMin]
      GetResult(positionedResult => Movie(
        positionedResult.<<,
        positionedResult.<<,
        LocalDate.parse(positionedResult.nextString()),
        positionedResult.<<))
    val query = sql"""select * from movies."Movie" """.as[Movie]
    Connection.db.run(query)
  }

  def multipleQueriesSingleTransaction(): Unit ={
    val insertMovie = SlickTables.movieTable += meaningOfLife
    val insertActors = SlickTables.actorTable += terryJones
    val finalQuery = DBIO.seq(insertMovie, insertActors)
    Connection.db.run(finalQuery.transactionally)
  }

  def findAllActorsByMovie(movieId: Long): Future[Seq[Actor]] = {
    val joinQuery = SlickTables.movieActorMappingTable
      .filter(_.movieId === movieId)
      .join(SlickTables.actorTable)
      .on(_.actorId === _.id) // select * from movieActorMappingTable m join actorTable a on m.actorId == a.id
      .map(_._2)

    Connection.db.run(joinQuery.result)
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
//    demoDelete()
//
//    readMoviesByPlainQuery().onComplete{
//      case Success(newMovieID) => println(s"Query successful, movies: ${newMovieID}")
//      case Failure(ex)=> println(s"Query failed, $ex")
//    }
//    Thread.sleep(5000)

//    demoInsertActors()
//    multipleQueriesSingleTransaction()
    findAllActorsByMovie(8L).onComplete{
      case Success(actors) => print(s"Actors from Monty Python's The Meaning of Life:\n$actors")
      case Failure(ex) => println(s"Query failed: $ex")
    }
    Thread.sleep(5000)
    PrivateExecutionContext.executor.shutdown()
  }

}
