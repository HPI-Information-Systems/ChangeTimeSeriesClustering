package de.hpi.data_change.time_series_similarity

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import de.hpi.data_change.time_series_similarity.category_extraction.MySQLDatabaseAccessor
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryExtractor extends App{

//  var spark:SparkSession = SparkSession
//    .builder()
//    .appName("Spark SQL basic example")
//    .master("local[1]")
//    .getOrCreate()
//  val connector = new MySQLDatabaseAccessor(spark)
//  val res = connector.findTitles()
//
//  val out = new ObjectOutputStream(new FileOutputStream("src/Main/resources/results/categoryMap.obj"))
//  out.writeObject(res)
//  out.close
  val in = new ObjectInputStream(new FileInputStream("src/Main/resources/results/categoryMap.obj"))
  val resRead = in.readObject().asInstanceOf[mutable.Map[String,mutable.Set[String]]]
  resRead.take(1000).mapValues( s => s.reduce( (a,b) => a+","+b)).foreach(t => println(t._1 + ":  [" + t._2 + "]"))
}
