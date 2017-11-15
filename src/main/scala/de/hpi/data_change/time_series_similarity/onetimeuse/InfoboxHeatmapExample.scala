package de.hpi.data_change.time_series_similarity.onetimeuse

import java.io.{File, FileWriter, PrintWriter}
import java.time.{LocalDateTime, ZoneOffset}

import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.visualization.TabluarResultFormatter
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.collection.mutable.ListBuffer

object InfoboxHeatmapExample extends App with Serializable{

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
  val filePath = "/home/leon/Documents/researchProjects/wikidata/data/old/oldCleaned.csv"
  val configIdentifier = "imdb_first_test"
  val start:java.sql.Timestamp = java.sql.Timestamp.valueOf("2014-02-21 00:00:00") //2014-02-21_Movies_changes
  val end:java.sql.Timestamp = java.sql.Timestamp.valueOf("2017-07-15 00:00:00") //2017-07-14

  //imdb()
  //imdb2()
  imdb3()

  def imdb() = {
    var dataset = getChangeRecordDataSet(filePath)
    //grouping phase:
    val properties = List("leader_title","leader_name","population_note","Name","latm","longd","Region","Website")
    dataset = dataset.filter(cr => properties.contains(cr.property))
    val totalCount = dataset.count()
    val results = dataset.groupByKey( cr => cr.entity + "|" + cr.property).mapGroups{case (id,it) => {
      val allVals = it.toList
      (allVals.head.entity,allVals.head.property,allVals.size.toDouble/totalCount)}}.collect()
    println(new TabluarResultFormatter().format(results.sortBy( t => t._1).map( t => List(t))))
    //dataset.take(100).foreach(println(_))
    val props = results.map(t => t._2).toSet.toList.sorted
    val entities = results.map(t=>t._1).toSet.toList.sorted
    val lines = ListBuffer[Seq[String]]()
    lines += (List("Settlement") ++ props)
    for(entity <- entities){
      val curList = ListBuffer[String]()
      curList +=entity
      for(prop <- props){
        val filtered = results.filter(t => t._2 == prop && t._1 == entity)
        assert(filtered.size<=1)
        if(filtered.size == 1){
          curList += filtered(0)._3.toString
        } else{
          curList += "0";
        }
      }
      lines += curList
    }
    val outFile = new File("/home/leon/Desktop/tableOut.csv")
    val pr = new PrintWriter(new FileWriter(outFile));
    lines.foreach{ list =>
      for(i <- (0 until list.size)){
        if(i!=list.size-1){
          pr.print(list(i) + ",")
        } else{
          pr.println(list(i))
        }
      }
    }
    pr.close()
  }

  def imdb2() = {
    var dataset = getChangeRecordDataSet(filePath)
    //grouping phase:
    val properties = List("leader_title","leader_name","population_note","Name","latm","longd","Region","Website")
    dataset = dataset.filter(cr => properties.contains(cr.property))
    val totalCount = dataset.count()
    val perProperty = dataset.groupByKey(cr => cr.property).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val perEntity = dataset.groupByKey(cr => cr.entity).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val lifespans = dataset.groupByKey(cr => cr.entity +"|"+ cr.property).mapGroups{case(id,it) =>{
      val allVals = it.toList
      val list = allVals.map( cr => cr.timestamp.toEpochSecond(ZoneOffset.UTC)).sorted
      (id,(list.max-list.min))
    }}
    val results = dataset.groupByKey( cr => cr.entity + "|" + cr.property).mapGroups{case (id,it) => {
      val allVals = it.toList
      (allVals.head.entity,allVals.head.property,allVals.size)}}.collect()
    println(new TabluarResultFormatter().format(results.sortBy( t => t._1).map( t => List(t))))
    //dataset.take(100).foreach(println(_))
    val props = results.map(t => t._2).toSet.toList.sorted
    val entities = results.map(t=>t._1).toSet.toList.sorted
    val lines = ListBuffer[Seq[String]]()
    lines += (List("Settlement") ++ props)
    for(entity <- entities){
      val curList = ListBuffer[String]()
      curList +=entity
      for(prop <- props){
        val filtered = results.filter(t => t._2 == prop && t._1 == entity)
        assert(filtered.size<=1)
        if(filtered.size == 1){
          curList += (filtered(0)._3.toDouble / perProperty(prop)).toString
        } else{
          curList += "0";
        }
      }
      lines += curList
    }
    val outFile = new File("/home/leon/Desktop/tableOut3.csv")
    val pr = new PrintWriter(new FileWriter(outFile));
    lines.foreach{ list =>
      for(i <- (0 until list.size)){
        if(i!=list.size-1){
          pr.print(list(i) + ",")
        } else{
          pr.println(list(i))
        }
      }
    }
    pr.close()
  }


  def imdb3() = {
    var dataset = getChangeRecordDataSet(filePath)
    //grouping phase:
    val properties = List("leader_title","leader_name","population_note","Name","latm","longd","Region","Website")
    dataset = dataset.filter(cr => properties.contains(cr.property))
    val totalCount = dataset.count()
    val perProperty = dataset.groupByKey(cr => cr.property).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val perEntity = dataset.groupByKey(cr => cr.entity).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val lifespans = dataset.groupByKey(cr => cr.entity +"|"+ cr.property).mapGroups{case(id,it) =>{
      val allVals = it.toList
      val list = allVals.map( cr => cr.timestamp.toEpochSecond(ZoneOffset.UTC)).sorted
      (id,(list.max-list.min))
    }}.collect().toMap
    val results = dataset.groupByKey( cr => cr.entity + "|" + cr.property).mapGroups{case (id,it) => {
      val allVals = it.toList
      (allVals.head.entity,allVals.head.property,allVals.size)}}.collect()
    println(new TabluarResultFormatter().format(results.sortBy( t => t._1).map( t => List(t))))
    //dataset.take(100).foreach(println(_))
    val props = results.map(t => t._2).toSet.toList.sorted
    val entities = results.map(t=>t._1).toSet.toList.sorted
    val lines = ListBuffer[Seq[String]]()
    lines += (List("Settlement") ++ props)
    for(entity <- entities){
      val curList = ListBuffer[String]()
      curList +=entity
      for(prop <- props){
        val filtered = results.filter(t => t._2 == prop && t._1 == entity)
        assert(filtered.size<=1)
        if(filtered.size == 1){
          curList += (filtered(0)._3.toDouble / lifespans(entity + "|" +  prop)).toString
        } else{
          curList += "0";
        }
      }
      lines += curList
    }
    val outFile = new File("/home/leon/Desktop/tableOut4.csv")
    val pr = new PrintWriter(new FileWriter(outFile));
    lines.foreach{ list =>
      for(i <- (0 until list.size)){
        if(i!=list.size-1){
          pr.print(list(i) + ",")
        } else{
          pr.println(list(i))
        }
      }
    }
    pr.close()
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
