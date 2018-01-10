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
  if(args(2) == "-local")
    sparkBuilder = sparkBuilder.master("local[2]")
  val spark = sparkBuilder.getOrCreate()

  implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(_.toEpochSecond(ZoneOffset.UTC))
  import spark.implicits._

  imdb(args(0),args(1))

  def imdb(inPath:String,outPath:String) = {
    val cities = List("Berlin","Chicago","Rome","Stockholm","Tokyo","Cape Town","Istanbul","London","Potsdam")
    var dataset = getChangeRecordDataSet(inPath)
      .filter( cr => cities.contains(cr.entity))
    val aggregated = dataset.groupByKey( cr => (cr.entity,cr.property))
      .mapGroups{case ((entity,prop),it) => (entity,prop,it.size)}
          .groupByKey( triple => triple._2)
          .mapGroups{case (prop,it) => {
            val list = it.toSeq
            if(list.size != cities.size){
              ("",List(("",-1)))
            } else{
              (prop,list.map( t => (t._1,t._3)).sortBy(t => t._1))
            }
          }}
      .filter(r => r._1 != "").collect().toSeq.sortBy( t => -t._2.toSeq.map( t1 => t1._2).sum).take(10).sortBy(t => t._1)
    aggregated.foreach(t => println(t._1 + t._2.toList.mkString(",")))
    val lines = ListBuffer[Seq[String]]()
    val props = aggregated.map( t => t._1).sorted
    lines += (List("Settlement") ++ props)
    cities.sorted.foreach( city =>{
      lines +=  (List(city) ++ aggregated.map(t => t._2.filter( t1 => t1._1==city).head._2.toString))
    })
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

    /*
    val newDs = dataset.withColumnRenamed(dataset.columns(0),"entity")
      .withColumnRenamed(dataset.columns(1),"property")
      .withColumnRenamed(dataset.columns(2),"value")
      .withColumnRenamed(dataset.columns(3),"timestamp")
    newDs.createOrReplaceTempView("changes")
    val ds = spark.sql("select entity,property,count(timestamp)" +
      "from changes " +
      "group by entity,property " +
      "order by entity,-count(timestamp)")
    ds.collect().foreach(println(_))
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
    pr.close()*/
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
