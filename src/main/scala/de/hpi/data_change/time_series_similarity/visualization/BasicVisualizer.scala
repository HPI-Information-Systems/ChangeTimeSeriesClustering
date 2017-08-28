package de.hpi.data_change.time_series_similarity.visualization

import java.io.File

import de.hpi.data_change.time_series_similarity.io.{BasicFileIO, ResultIO}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{Row, SparkSession}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}
import org.jfree.data.xy.XYDataset
import org.jfree.chart.ChartFactory
import org.jfree.chart.JFreeChart
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import java.awt.{Color, Dimension}


case class BasicVisualizer(spark: SparkSession, filePath: String) {
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

  def drawClusteringCentroids(filePath:String,configIdentifier:String) = {
    val model = ResultIO.loadKMeansModel(filePath)
    val clusteringResult = ResultIO.loadClusteringResult(spark,filePath)
    val grouped = clusteringResult.groupByKey(r => r.getAs[Int](1))
    val clusterSizes = grouped.mapGroups{case (cluster,it) => (cluster,it.size)}.collect().toMap
    val collection = new XYSeriesCollection()
    model.clusterCenters
      .zipWithIndex
      .map{case (a,i) => (a,i,clusterSizes(i))}
      .sortBy( t => t._3)
      .map{case (a,i,size) => toXYSeries(a.toArray,i)}
      .foreach( series => collection.addSeries(series))
    //TODO: plot linechart
    val chart:MultiLineChart  = new MultiLineChart("configIdentifier",collection)
    chart.draw()
  }

  def toXYSeries(vector: Seq[Double], clusterNum:Int): XYSeries ={
    val series = new XYSeries("cluster " + clusterNum.toString)
    vector.zipWithIndex.foreach(t => series.add(t._2,t._1))
    series
  }

  def drawExampleCategoryBarChart(): Unit ={
    val clusteringResult = spark.read.load((filePath))
    val grouped = clusteringResult.groupByKey(r => "cluster_"+r.getAs[Int](1))
    val clusterSizes = grouped.mapGroups{case (cluster,it) => (cluster,it.size)}.collect()
    clusterSizes.foreach{case (cluster,size) => println(cluster + "has size "+ size )}
    val finalDs = grouped.flatMapGroups{case (cluster,rowIterator) => toTripleList(cluster,rowIterator)}
    val finalList = finalDs.collect()
    var chartRelative = new BarChart("first Try Barchart Relative",finalList,true)
    var chartAbsolute = new BarChart("first Try Barchart Absolute",finalList,false)
    chartRelative.draw()
    chartAbsolute.draw()
  }
}
