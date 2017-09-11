package de.hpi.data_change.time_series_similarity

import java.io._

import de.hpi.data_change.time_series_similarity.category_extraction.MySQLDatabaseAccessor
import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data.BasicSparkQuerier
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryExtractor extends App{

  var spark:SparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[1]")
    .getOrCreate()
  val connector = new MySQLDatabaseAccessor(spark)
  //val in = new ObjectInputStream(new FileInputStream("src/main/resources/results/categoryMap.obj"))

  //val t = in.readObject().asInstanceOf[mutable.Map[String,mutable.Set[String]]]

  def writeFile() = {
    val connector = new MySQLDatabaseAccessor(spark)
    val t = connector.findTitles()

    val categoryMap = t._1
    val hasNoCategory = t._2
    println("has cat: " + hasNoCategory.size)
    categoryMap.keys.take(100).foreach(println(_))
    println("------------------------------------------------------------------------------")
    println("has no cat: " + hasNoCategory.size)
    hasNoCategory.take(Math.min(hasNoCategory.size,100)).foreach(println(_))
    val allPageNames = categoryMap.keySet.toList
    toFile(allPageNames,"/home/leon/Documents/data/wikidata/pageTitles.txt")
    toFile(hasNoCategory,"/home/leon/Documents/data/wikidata/pageWithNoCategories.txt")
    val out = new ObjectOutputStream(new FileOutputStream("src/main/resources/results/categoryMap.obj"))
    out.writeObject(categoryMap)
    out.close
  }

  writeFile()

  def testCategories() = {
    val connector = new MySQLDatabaseAccessor(spark)
    val res = connector.searchCategory(List("Counties of Hormozgan Province","Abumusa County","Hormozgan Province geography stubs"))
    res.foreach(println(_))
  }

  //testCategories()

  private def toFile(list:Seq[String],fname:String) = {
    val list1 = list.sorted
    val targetFile = fname
    val pr = new PrintWriter(new FileWriter(targetFile))
    list1.foreach(pr.println(_))
    pr.close()
  }

}
