package de.hpi.data_change.time_series_similarity.onetimeuse

import de.hpi.data_change.time_series_similarity.Clustering
import org.apache.spark.sql.SparkSession

object PotsdamIsWeird extends App{
  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  //sparkBuilder = sparkBuilder.master("local[2]")
  val spark = sparkBuilder.getOrCreate()
  val filter = args(1)
  val ds = new Clustering("","",spark).getChangeRecordDataSet(args(0))
  val res = ds.filter(cr => cr.entity == filter) .collect()
  println(res.size)
  res.foreach( cr => println(cr))
  val ds2 = spark.read.csv(args(0)).filter(r => r.getString(0) == filter)
  val res2 = ds2.collect()
  println("new way of computing it:")
  println(res2.size)
  res2.foreach(println(_))
}
