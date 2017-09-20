package de.hpi.data_change.time_series_similarity.visualization

import java.io.File

import de.hpi.data_change.time_series_similarity.io.{BasicFileIO, ResultIO}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

  implicit def rowEnc = org.apache.spark.sql.Encoders.kryo[Row]
  import spark.implicits._

  val categoryMap:Map[String,Seq[String]] = null;//ResultIO.readFullCategoryMap()

  def getCategories(title: String):List[String] = {
    if (categoryMap.contains(title)) categoryMap(title).toList else List("unknown")
  }

  def getTop5Categories(elementIterator: Iterator[(String, Int, Int)]):List[(String,Int)] = {
    val allElems = elementIterator.toList
    val top4Cats = allElems.map{case (category,cluster,count) => (category,count)}.sortBy{case (category,count) => count}.take(4)
    val others = ("other", allElems.map(t => t._3).sum - top4Cats.map( t => t._2).sum)
    top4Cats ++ List(others)
  }

  def buildTripleList(cluster: Int, list: Seq[(String, Int)]): Seq[(String,String,Double)] = {
    list.map{case (cat,count) => (cat,cluster.toString,count.toDouble)}
  }

  def toNewList(list: Seq[(String, Int)]):List[(String,Int)] = {
    println("haha")
    val lb = collection.mutable.ListBuffer[(String,Int)]()
    list.foreach( lb.append(_))
    println("hoho")
    lb.toList
  }

  def getClusterCategoryTripleList(clusteringResult: DataFrame):Seq[(String,String,Double)] = {
    import spark.implicits._
    val allRelevantData = clusteringResult.map( r => (r.getString(0),r.getInt(2)))
      .map{case (title,cluster) => (title,getCategories(title),cluster)}
    val res = allRelevantData.flatMap{case (_,categories,cluster) => categories.map( cat => (cat,cluster,0))}
      .groupByKey( t => (t._1,t._2))
      .mapGroups{case (catAndCluster,vals) => (catAndCluster._1,catAndCluster._2,vals.size)}
      .groupByKey{case (cat,cluster,count) => cluster}
      .mapGroups{case (cluster,elementIterator) => (cluster,getTop5Categories(elementIterator))}
        .collect()
    var buffer = collection.mutable.ListBuffer[(Int,List[(String,Int)])]()

    res.foreach( buffer.append(_))
    var bufferFinal = collection.mutable.ListBuffer[(Int,List[(String,Int)])]()
    for (elem <- buffer) {
      val value = (elem._1,toNewList(elem._2) )
      bufferFinal.append(value)
    }
    val finalRes = bufferFinal.flatMap{case (cluster,list) => buildTripleList(cluster,list)}
    finalRes
  }

  def drawClusterCategoryBarchart(resultDir: String) = {
    val config = ResultIO.loadConfig(resultDir)
    val model = ResultIO.loadKMeansModel(filePath)
    val clusteringResult = ResultIO.loadClusteringResult(spark,resultDir)
    val data = getClusterCategoryTripleList(clusteringResult)
    data.foreach(println(_))
    val chart = new BarChart("Cluster Category Distribution",data,true)
    chart.draw()
  }

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

  def drawClusteringCentroids(filePath:String,configIdentifier:String,x:Int,y:Int) = {
    val config = ResultIO.loadConfig(filePath)
    val model = ResultIO.loadKMeansModel(filePath)
    val clusteringResult = ResultIO.loadClusteringResult(spark,filePath)
    val grouped = clusteringResult.groupByKey(r => r.getAs[Int](2))
    val clusterSizes = grouped.mapGroups{case (cluster,it) => (cluster,it.size)}.collect().toMap
    val collection = new XYSeriesCollection()
    model.clusterCenters
      .zipWithIndex
      //.filter{case (_,i) => clusterSizes.contains(i)} //TODO: temporary filter because of really weird things happening
      .map{case (a,i) => (a,i,clusterSizes(i))}
      .sortBy( t => t._3)
      .map{case (a,i,size) => toXYSeries(a.toArray,i,clusterSizes(i))}
      .foreach( series => collection.addSeries(series))
    assert(model.clusterCenters.size == clusterSizes.keys.size)
    val chart:MultiLineChart  = new MultiLineChart(configIdentifier,config,collection)
    chart.draw(x,y)
  }

  def toXYSeries(vector: Seq[Double], clusterNum:Int,clusterSize:Int): XYSeries ={
    val series = new XYSeries("cluster " + clusterNum.toString + " (" + clusterSize + ")")
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
