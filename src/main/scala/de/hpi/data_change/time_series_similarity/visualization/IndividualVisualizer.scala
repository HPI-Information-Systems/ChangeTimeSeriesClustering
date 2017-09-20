package de.hpi.data_change.time_series_similarity.visualization

import de.hpi.data_change.time_series_similarity.io.ResultIO
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}

case class IndividualVisualizer(spark: SparkSession, singleResultPath: String) extends Serializable{

  implicit def rowEnc = org.apache.spark.sql.Encoders.kryo[Row]
  import spark.implicits._

  val categoryMap:Map[String,List[String]] = null//ResultIO.readSettlementsCategoryMap()

  @transient val config = ResultIO.loadConfig(singleResultPath)
  @transient val model = ResultIO.loadKMeansModel(singleResultPath)
  @transient val clusteringResult = ResultIO.loadClusteringResult(spark,singleResultPath)
  val configIdentifier = ResultIO.getConfigIdentifier(singleResultPath)

  def getCategories(title: String):List[String] = {
    if (categoryMap.contains(title)) categoryMap(title).toList else List("unknown")
  }

  def getTop5Categories(elementIterator: Iterator[(String, Int, Int)]):List[(String,Int)] = {
    val allElems = elementIterator.toList
    val top4Cats = allElems.map{case (category,cluster,count) => (category,count)}.sortBy{case (category,count) => - count}.take(7)
    val others = ("other", allElems.map(t => t._3).sum - top4Cats.map( t => t._2).sum)
    top4Cats.filter(t => t._1 != "unknown" && t._1 != "All_stub_articles")// ++ List(others)
  }

  def buildTripleList(cluster: Int, list: Seq[(String, Int)]): Seq[(String,String,Double)] = {
    list.map{case (cat,count) => (cat,cluster.toString,count.toDouble)}
  }

  def toNewList(list: Seq[(String, Int)]):List[(String,Int)] = {
    val lb = collection.mutable.ListBuffer[(String,Int)]()
    list.foreach( lb.append(_))
    lb.toList
  }

  def getClusterCategoryTripleList():Seq[(String,String,Double)] = {
    import spark.implicits._
    val allRelevantData = clusteringResult.map( r => (r.getString(0),r.getInt(2)))
      .map{case (title,cluster) => (title,getCategories(title),cluster)}
    println(allRelevantData.count())
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

  def printTripleList(data: Seq[(String, String, Double)]) = {
    data.groupBy( t => t._2).foreach( t => {println("cluster " + t._1); t._2.sortBy( t => t._3).foreach( t => println(t._1 + " : " + t._3)) })
  }

  def drawClusterCategoryBarchart() = {
    val data = getClusterCategoryTripleList()
    printTripleList(data)
    val chart = new BarChart("Cluster Category Distribution Relative",data,true)
    chart.draw()
    val chart1 = new BarChart("Cluster Category Distribution",data,false)
    chart1.draw()
  }

  def drawClusteringCentroids(x:Int=0,y:Int=0) = {
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

  def timeSeriesToString(r: Row): Any = {
    val vec = r.getAs[org.apache.spark.ml.linalg.Vector](1)
    timeSeriesToString(vec)
  }

  def timeSeriesToString(vec: org.apache.spark.ml.linalg.Vector) = {
    "[" + vec.toArray.map( d => "%.1f".format(d)).mkString(",") + "]"
  }

  def printRepresentatives(clusterId: Int) = {
    val clusterSize = clusteringResult.filter( r => r.getAs[Int](2) == clusterId).count()
    println("====================== Cluster Representatives for cluster " + clusterId + " (size:" + clusterSize +")======================")
    println("Centroid: " + timeSeriesToString(model.clusterCenters(clusterId)))
    println("-----------------------------------------------------------------------------------------------------------------------------------")
    clusteringResult.filter( r => r.getAs[Int](2) == clusterId)
      .take(100)
      .foreach( r => println(r.getString(0) + ", " + timeSeriesToString(r)))
  }

  def printClusterRepresentatives() = {
    println("====================== Cluster Representatives for " + configIdentifier + "======================")
    model.clusterCenters.zipWithIndex.foreach( t => printRepresentatives(t._2) )
  }

}
