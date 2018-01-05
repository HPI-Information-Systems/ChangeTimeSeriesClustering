package de.hpi.data_change.time_series_similarity.specific_executions

//import hiho.TestClass
import dmlab.main.MainDriver
import net.sf.javaml.core.{DenseInstance, Instance}

object asd extends App{
  //165 166 167 191 346 389
  val a = java.sql.Timestamp.valueOf("2001-01-18 00:00:01.0")
  val bucketSize = 15
  val bucketID = List(165,166,167,191,346,389)
  bucketID.foreach( id => println(a.toLocalDateTime.plusDays(bucketSize*id) + " until " + a.toLocalDateTime.plusDays(bucketSize*(id+1))))

  //test with java sources:
  //val test = new TestClass(1,"asd")
  //println(test)
  //val dist = new net.sf.javaml.distance.dtw.DTWSimilarity().measure(new DenseInstance(Array(1.0,2.0,3.0)),new DenseInstance(Array(0.0,2.0,3.0)))
  //println(dist)
  //MainDriver.main(args)
}
