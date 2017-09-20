package de.hpi.data_change.time_series_similarity.category_extraction

import java.sql.DriverManager
import java.sql.Connection

import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import de.hpi.data_change.time_series_similarity.io.DataIO
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * A Scala JDBC connection example by Alvin Alexander,
  * http://alvinalexander.com
  */
class MySQLDatabaseAccessor(sparkSession: SparkSession,username:String,password:String,databseURL:String) {

  val driver = "com.mysql.jdbc.Driver"

  var connection:Connection = null
  // make the connection
  Class.forName(driver)
  connection = DriverManager.getConnection(databseURL, username, password)

  def this(sparkSession: SparkSession){
    this(sparkSession,"root","admin","jdbc:mysql://localhost/wikiCategories?serverTimezone=UTC&useUnicode=true&characterEncoding=utf8")
  }

  def isSimilarTo(elem: String, category: String): Boolean = {
    val elem_new = elem.trim.toLowerCase
    val category_new = category.trim.toLowerCase
    category_new.contains(elem_new) || elem_new.contains(category_new)
  }

  def searchCategory(categories:Seq[String]):mutable.Map[String,Boolean] = {
    Class.forName(driver)
    val connection = DriverManager.getConnection(databseURL, username, password)
    println("starting database access")
    // create the statement, and run the select query
    val statement = connection.createStatement()
    statement.setFetchSize(Integer.MIN_VALUE)
    val resultSet = statement.executeQuery("SELECT cl_to FROM categorylinks")
    println("database access done, processing result set")
    val map = mutable.Map[String,Boolean]()
    categories.foreach( c => map(c) = false)
    while ( resultSet.next() ) {
      val category = resultSet.getString("cl_to")
      if(map.contains(category)){
        map(category) = true
      } else{
        for (elem <- categories) {
          if(isSimilarTo(elem,category)){
            println("Found similarity between Actual Category: " + elem  + " and database entry: " + category)
          }
        }
      }
    }
    map
  }

  def findTitles(filePath:String):(mutable.Map[String,mutable.Set[String]],Seq[String]) = {
    import sparkSession.implicits._
    val titlesInChangeDB = scala.collection.mutable.Set[String]()
    val agg = TimeSeriesAggregator(sparkSession,null,null,null)
    val titlesFromDB  =agg.getChangeRecordDataSet(filePath).map(_.entity).distinct().collect().toSet
    titlesInChangeDB ++= titlesFromDB
    titlesInChangeDB.take(100).foreach(e => println("this is some test output so spark finishes: " + e))
    println(titlesInChangeDB.size)
    // there's probably a better way to do this
    println("starting database access")
    //get ids for titles:
    val sizeBefore = titlesInChangeDB.size
    // create the statement, and run the select query
    val statement = connection.createStatement()
    statement.setFetchSize(Integer.MIN_VALUE)
    val resultSet = statement.executeQuery("SELECT page_title,cl_to FROM categorylinks,page WHERE cl_from = page_id")
    println("database access done, processing result set")
    var count:Long = 1
    val allCategories = mutable.Map[String,mutable.Set[String]]()
    while ( resultSet.next() ) {
      if(count % 100000==0){
        println("fetched " + count + " rows")
      }
      //val titleInDB = resultSet.getString("page_title")
      val titleInDBBytes = resultSet.getBytes("page_title")
      val titleInDB = new String(titleInDBBytes,"UTF-8") //we have to it this way since the encoding is not correct, even if we specify UTF8 in the connection

      val title = titleInDB.replace("_"," ")
      if(titlesInChangeDB.contains(title) || allCategories.contains(title)) {
        val category = resultSet.getString("cl_to")
        titlesInChangeDB.remove(title)
        if (allCategories.contains(title)) {
          allCategories(title) += category
        } else {
          allCategories(title) = mutable.Set[String](category)
        }
      }
      count +=1
    }
    println("iterated over " + count + " database entries")
    connection.close()
    val sizeAfter = titlesInChangeDB.size
    println("num With entry: " + (sizeBefore-sizeAfter))
    println("num without Entry: " + sizeAfter)
    (allCategories,titlesInChangeDB.toList)
  }

}
