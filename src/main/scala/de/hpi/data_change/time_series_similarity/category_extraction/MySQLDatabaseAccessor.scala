package de.hpi.data_change.time_series_similarity.category_extraction

import java.sql.DriverManager
import java.sql.Connection

import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * A Scala JDBC connection example by Alvin Alexander,
  * http://alvinalexander.com
  */
class MySQLDatabaseAccessor(sparkSession: SparkSession,username:String,password:String,databseURL:String) {

  val driver = "com.mysql.jdbc.Driver"

  def this(sparkSession: SparkSession){
    this(sparkSession,"root","admin","jdbc:mysql://localhost/wiki_categories")
  }

  def findTitles():mutable.Map[String,mutable.Set[String]] = {
    import sparkSession.implicits._
    val titlesInChangeDB = scala.collection.mutable.Set[String]()
    val agg = TimeSeriesAggregator(sparkSession,0,null,null)
    val filePath = "C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Data\\wikidata\\settlements\\dump.csv";
    val titlesFromDB  =agg.getChangeRecordDataSet(filePath).map(_.entity).distinct().collect().toSet
    titlesInChangeDB ++= titlesFromDB
    titlesInChangeDB.take(100).foreach(e => println("this is some test output so spark finishes: " + e))
    // there's probably a better way to do this
    var connection:Connection = null
    val sizeBefore = titlesInChangeDB.size
    // make the connection
    Class.forName(driver)
    connection = DriverManager.getConnection(databseURL, username, password)
    println("starting database access")
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
      val title = resultSet.getString("page_title")
        if(titlesInChangeDB.contains(title)) {
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
    connection.close()
    val sizeAfter = titlesInChangeDB.size
    println("num With entry: " + (sizeBefore-sizeAfter))
    println("num without Entry: " + sizeAfter)
    allCategories
  }

}
