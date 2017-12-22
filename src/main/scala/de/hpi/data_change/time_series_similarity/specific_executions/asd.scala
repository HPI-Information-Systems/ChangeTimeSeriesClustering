package de.hpi.data_change.time_series_similarity.specific_executions

import hiho.TestClass

object asd extends App{
  val a = java.sql.Timestamp.valueOf("2001-01-18 00:00:01.0")
  val bucketSize = 60
  val bucketID = 90
  println(a.toLocalDateTime.plusDays(60*67))

  //test with java sources:
  val test = new TestClass(1,"asd")
  println(test)
}
