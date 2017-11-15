package de.hpi.data_change.time_series_similarity.onetimeuse

import java.io.{File, FileWriter, PrintWriter}
import java.time.{LocalDateTime, ZoneOffset}

import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.visualization.TabluarResultFormatter
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.collection.mutable.ListBuffer

object IMDBExploration extends App with Serializable{

  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    sparkBuilder = sparkBuilder.master("local[2]")
  val spark = sparkBuilder.getOrCreate()

  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def changeRecordListEncoder: Encoder[List[ChangeRecord]] = org.apache.spark.sql.Encoders.kryo[List[ChangeRecord]]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]
  implicit def ordered: Ordering[LocalDateTime] = new Ordering[LocalDateTime] {
    def compare(x: LocalDateTime, y: LocalDateTime): Int = x compareTo y
  }

  implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(_.toEpochSecond(ZoneOffset.UTC))
  import spark.implicits._
  //wikidata()

  val minCount = 0
  val temporalIntervalInDays = 7
  val numClusters = 10
  val numIterations = 100
  val resultDirectory = "/home/leon/Documents/researchProjects/imdb/localResults/"
  val filePath = "/home/leon/Documents/researchProjects/imdb/cleaned/"
  val configIdentifier = "imdb_first_test"
  val start:java.sql.Timestamp = java.sql.Timestamp.valueOf("2014-02-21 00:00:00") //2014-02-21_Movies_changes
  val end:java.sql.Timestamp = java.sql.Timestamp.valueOf("2017-07-15 00:00:00") //2017-07-14

  def readFiltered() = {
    val rawData = spark.read.option("mode", "DROPMALFORMED").csv("/home/leon/Documents/researchProjects/imdb/localResults/spike.csv")
    val filtered = rawData.as[(String,String,String)]
    val collected = filtered.collect()
    println(collected.size)
    collected.map( t => t._2).toSet.toList.sorted.foreach(println(_))
    collected.foreach(println(_))
  }

  //readFiltered()

  def imdbProperties() = {
    var dataset = getChangeRecordDataSet(filePath)
    val filtered = dataset.groupByKey(cr => cr.entity + "|" + cr.property).mapGroups{ case (s,it) => {
      val list = it.toList
      (list.head.entity,list.head.property,list.size)
    }}.filter($"_2" === "role" || $"_2" === "positionInCredits")
    filtered.write.csv("/home/leon/Documents/researchProjects/imdb/localResults/roleAndPosition")
    filtered.take(1000).foreach(println(_))
  }

  imdbProperties()
  //imdb()

  def imdbSpikeAnalysis() = {
    var dataset = getChangeRecordDataSet(filePath)
    //grouping phase:
    //    println(dataset.filter( cr => cr.property == "title").count());
    //    println("num entities: " + dataset.map(cr => cr.entity).distinct().count())
    //    println("num properties: " + dataset.map(cr => cr.property).distinct().count())
    //    println("num values: " + dataset.map(cr => cr.value).distinct().count())
    //    println("num change Records: " + dataset.count())
    //    println("change per entity distribution:")
    //    dataset.groupByKey(cr => cr.entity).mapGroups{ case (s,it) => (s,it.size)}
    //        .groupByKey(t => t._2).mapGroups{case (count,it) => (count,it.size)}
    //        .collect().sortBy( t => t._1).foreach(t => println(t._1 + ","+t._2+","))
    //    println("change per property distribution:")
    //    dataset.groupByKey(cr => cr.property).mapGroups{ case (s,it) => (s,it.size)}
    //      .groupByKey(t => t._2).mapGroups{case (count,it) => (count,it.size)}
    //      .collect().sortBy( t => t._1).foreach(t => println(t._1 + ","+t._2+","))
    //    println("change per entity-property distribution:")
    val filtered = dataset.groupByKey(cr => cr.entity + "|" + cr.property).mapGroups{ case (s,it) => {
      val list = it.toList
      (list.head.entity,list.head.property,list.size)
    }}.filter($"_3" ===171)
    filtered.write.csv("/home/leon/Documents/researchProjects/imdb/localResults/spike.csv")
    println(filtered.count())
    filtered.collect().map( t => t._2).toSet.toList.sorted.foreach(println(_))
    filtered.collect().foreach(println(_))

    //println(dataset.filter( cr => cr.property == "title").count());
    //spark sort: dataset.sort($"col1".desc)
    //dataset.take(100).foreach(println(_))
  }

  def getChangeRecordDataSet(filePath:String): Dataset[ChangeRecord] ={
    val rawData = spark.read.option("mode", "DROPMALFORMED").csv(filePath)
    rawData.filter(r => r.getString(3) !=null && r.size == 4).map(r =>  {
      if(r.size != 4){
        println("huh")
        assert(false) //todo remove malformatted
      }
      new ChangeRecord(r)
    }
    )
  }
}
