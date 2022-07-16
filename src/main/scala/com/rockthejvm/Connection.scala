package com.rockthejvm

import slick.jdbc.PostgresProfile.api._

object Connection {
 // like connection string
  val db = Database.forConfig("postgres")
}
