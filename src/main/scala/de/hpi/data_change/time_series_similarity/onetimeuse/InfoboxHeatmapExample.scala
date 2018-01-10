package de.hpi.data_change.time_series_similarity.onetimeuse

import java.io.{File, FileWriter, PrintWriter}
import java.time.{LocalDateTime, ZoneOffset}

import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.visualization.TabluarResultFormatter
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object InfoboxHeatmapExample extends App with Serializable{

  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    sparkBuilder = sparkBuilder.master("local[2]")
  val spark = sparkBuilder.getOrCreate()
  implicit def ordered: Ordering[LocalDateTime] = new Ordering[LocalDateTime] {
    def compare(x: LocalDateTime, y: LocalDateTime): Int = x compareTo y
  }

  implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(_.toEpochSecond(ZoneOffset.UTC))
  import spark.implicits._

  imdb(args(0),args(1))

  def imdb(inPath:String,outPath:String) = {
    val cities = List("Berlin","Chicago","Rome","Stockholm","Tokyo","Cape Town","Istanbul","London")

    var dataset = getChangeRecordDataSet(inPath)
      .filter( cr => cities.contains(cr.entity))
//    dataset.write.csv("/home/leon/Documents/researchProjects/wikidata/data/selectedCities")
    //grouping phase:
    val properties = List("leader_title","leader_name","population_note","name","latm","longd","region","website","country")
    dataset = dataset.filter(cr => properties.contains(cr.property))
    val totalCount = dataset.count()
    println(totalCount)
    val results = dataset.groupByKey( cr => cr.entity + "|" + cr.property).mapGroups{case (id,it) => {
      val allVals = it.toList
      (allVals.head.entity,allVals.head.property,allVals.size.toDouble/totalCount)}}.collect()
    println(new TabluarResultFormatter().format(results.sortBy( t => t._1).map( t => List(t))))
    var all:Set[String] = null
    for(entity <- cities){
      if(all == null){
        all = results.filter(t => t._1 == entity).map(t => t._2).take(400).toSet
      } else{
        all = all.intersect(results.filter(t => t._1 == entity).map(t => t._2).take(400).toSet)
      }
      println("-----------------------------------")
      println(entity)
      val filtered = results.filter(t => t._1 == entity).sortBy( t => -t._3).take(100)
      filtered.foreach(println(_))
    }
    all.foreach(println(_))
    //dataset.take(100).foreach(println(_))
    val props = properties.sorted
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
    val outFile = new File(outPath)
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

  def imdb2(filePath:String) = {
    var dataset = getChangeRecordDataSet(filePath)
    //grouping phase:
    val properties = List("leader_title","leader_name","population_note","Name","latm","longd","Region","Website")
    dataset = dataset.filter(cr => properties.contains(cr.property))
    val totalCount = dataset.count()
    val perProperty = dataset.groupByKey(cr => cr.property).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val perEntity = dataset.groupByKey(cr => cr.entity).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val lifespans = dataset.groupByKey(cr => cr.entity +"|"+ cr.property).mapGroups{case(id,it) =>{
      val allVals = it.toList
      val list = allVals.map( cr => cr.timestamp.toLocalDateTime.toEpochSecond(ZoneOffset.UTC)).sorted
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


  def imdb3(filePath:String) = {
    var dataset = getChangeRecordDataSet(filePath)
    //grouping phase:
    val properties = List("leader_title","leader_name","population_note","Name","latm","longd","Region","Website")
    dataset = dataset.filter(cr => properties.contains(cr.property))
    val totalCount = dataset.count()
    val perProperty = dataset.groupByKey(cr => cr.property).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val perEntity = dataset.groupByKey(cr => cr.entity).mapGroups{case (prop,it) => (prop,it.size)}.collect().toMap
    val lifespans = dataset.groupByKey(cr => cr.entity +"|"+ cr.property).mapGroups{case(id,it) =>{
      val allVals = it.toList
      val list = allVals.map( cr => cr.timestamp.toLocalDateTime.toEpochSecond(ZoneOffset.UTC)).sorted
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
    new Clustering("","",spark).getChangeRecordDataSet(filePath)
  }
}
