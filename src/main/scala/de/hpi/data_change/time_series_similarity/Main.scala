package de.hpi.data_change.time_series_similarity

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Main entry to the clustering framework
  * First parameter is a .json file containing most user-configurable options
  * Other User-Defined Options (Grouping and filter on the groups) are defined here
  */
object Main extends App with Serializable{

  val isLocal = args.length==2 && args(1) == "-local"
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
  if(isLocal) {
    sparkBuilder = sparkBuilder.master("local[2]")
  }

  import java.sql.Connection
  import java.sql.DriverManager

  val databaseName = "db"
  val user = "monetdb"
  val pw = "monetdb"
  val tableName = "wiki"

  private val address = "172.16.64.69"
  private val port = "5050"
  val driver = "nl.cwi.monetdb.jdbc.MonetDriver"
  val url = s"jdbc:monetdb://$address:$port/$databaseName"
  val con = DriverManager.getConnection(url, s"$user", s"$pw")
  val st = con.createStatement()
  val query = s"SELECT * FROM $tableName LIMIT 100"
  val result = st.executeQuery(query)
  val resultTableName = "testResultTable"

  //print results:
  while(result.next()){
    println(result.getString(2));
  }

  def getArbitraryQueryResult(url:String,query:String) = {
    spark.sqlContext.read.format("jdbc").
        option("url", url).
        option("driver", driver).
        option("useUnicode", "true").
        //option("continueBatchOnError","true").
        option("useSSL", "false").
        option("user", user).
        option("password", pw).
        option("dbtable","(" + query + ") as queryresult").
        load()
  }

  def writeToDB(resultDF: DataFrame) = {
    val toWrite = resultDF
    println("dummy output simulating database write")
    val props = new Properties()
    props.setProperty("user",user)
    props.setProperty("password",pw)
    props.setProperty("driver",driver)
    toWrite.write.jdbc(url,resultTableName,props)
  }

  val spark = sparkBuilder.getOrCreate()
  val df = spark.read.csv("resources/test.csv")
  df.show()
  val df2 = getArbitraryQueryResult(url,query)
  df2.show(1000)
  //writeToDB(df2)

//  val clusterer = new Clustering(spark)
//  clusterer.setParams(args(0))
//  clusterer.grouper = (cr => Seq(cr.entity))
//  clusterer.groupFilter = (ds => ds.filter(t => t._2.size>50))
//  clusterer.clustering()
}
