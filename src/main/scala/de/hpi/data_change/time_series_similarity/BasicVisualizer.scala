package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.visualization.BarChart
import org.apache.spark.sql.{Row, SparkSession}

case class BasicVisualizer(spark: SparkSession, filePath: String) {
  val clusteringResult = spark.read.csv(filePath)
  import spark.implicits._
  def groupFromName(name: String): String = {
    val r = scala.util.Random
    val randomNumber = r.nextInt(5)
    "group_" + randomNumber
  }

  def toTripleList(cluster:String,rows:Iterator[Row]):List[(String,String,Double)] = {
    val asList = rows.toList
    val firstElem = asList.head
    val res = asList.map(r => (r.getString(0),r.getString(1).replaceAll("\\\"","").toInt))
      .toList.groupBy{case (name,_) => groupFromName(name)}
      .toList.map{case (groupName,list) => (groupName,cluster,list.size.toDouble)}
    res
  }

  val grouped = clusteringResult.groupByKey(r => "cluster_"+r.getAs[Int](1))

  val finalDs = grouped.flatMapGroups{case (cluster,rowIterator) => toTripleList(cluster,rowIterator)}
  val finalList = finalDs.collect()
  var chartRelative = new BarChart("first Try Barchart Relative",finalList,true)
  var chartAbsolute = new BarChart("first Try Barchart Absolute",finalList,false)


  def draw(): Unit ={
    chartRelative.draw()
    chartAbsolute.draw()
  }
}
