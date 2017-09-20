package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.io.{DataIO, ResultIO}
import de.hpi.data_change.time_series_similarity.visualization.TabluarResultFormatter

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object CategoryStatistics extends App{

  val cats = ResultIO.readFullCategoryMap()
  val allCats = cats.values.flatMap( s => s).toSet
  val reverseMap = scala.collection.mutable.Map[String,ListBuffer[String]]()
  var map = scala.collection.mutable.Map[String,(Int,Double)]()

  def incrementMap(map: mutable.Map[String, (Int, Double)], cat: String) = {
    if(map.contains(cat)){
      val cur = map(cat)
      map(cat) = (cur._1+1,cur._2)
    } else{
      map(cat) = (1,0.0)
    }
  }

  def overlapment(cat1:String,cat2:String) = {
    val list1 = reverseMap(cat1)
    val list2 = reverseMap(cat2)
    val intersectionSize = list1.toSet.intersect(list2.toSet).size
    intersectionSize
  }

  def getCorrelationMatrix(cats: List[String]) = {
    val res = new ListBuffer[ListBuffer[String]]()
    res.append(new ListBuffer[String]() ++= List("  ") ++ cats)
    for(cat1 <- cats){
      val curList = new ListBuffer[String]()
      curList.append(cat1)
      for(cat2 <- cats){
//        if(cat2 == cat1){
//          curList.append("x")
//        } else{
          curList.append((overlapment(cat1,cat2).toDouble / reverseMap(cat1).size.toDouble).toString)
        //}
      }
      res.append(curList)
    }
    res
  }
  cats.foreach{case (entity,cats) => cats.foreach(e => incrementMap(map,e))}

  def toTuple(i: Int, rel: Double):(Int,Double) = {
    (i,i.toDouble / cats.size.toDouble)
  }

  val newMap = map.mapValues{case (i,rel) => toTuple(i,rel)}
  newMap.toList.sortBy( t => - t._2._1).take(100).foreach(println(_))
  println("Total num distinct categories: " + newMap.size)

  def addToReverseMap(cat: String, entity: String) = {
    if(reverseMap.contains(cat)){
      reverseMap(cat) ++= List(entity)
    } else{
      reverseMap(cat) = new ListBuffer[String]() ++= List(entity)
    }
  }

  def fillReverseMap() = {
    cats.foreach{case (entity,cats) => cats.foreach(cat => addToReverseMap(cat,entity))}
  }

  fillReverseMap()
  val correlationMatrix = getCorrelationMatrix( newMap.toList.sortBy(t => - t._2._1).map(t => t._1).take(10))
  println(new TabluarResultFormatter().format(correlationMatrix))
  //TODO: test if something is still weird!
  val test1 = reverseMap("1980s_births")
  val test3 = reverseMap("1985_births")
  println(test1.size)
  println(test3.size)
  test1.foreach( c => println(c))
  println(overlapment("1980s_births","1985_births"))
  println(cats.filter{case (k,v) => v.size>1}.size)
}
