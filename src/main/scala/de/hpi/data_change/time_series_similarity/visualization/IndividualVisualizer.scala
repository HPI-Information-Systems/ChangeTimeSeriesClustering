package de.hpi.data_change.time_series_similarity.visualization

import java.io.{FileWriter, PrintWriter}

import com.google.common.collect.{HashMultiset, Multiset}
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.io.ResultIO
import org.apache.spark.sql._
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}

import scala.collection.mutable

case class IndividualVisualizer(spark: SparkSession, singleResultPath: String) extends Serializable{


  implicit def rowEnc = org.apache.spark.sql.Encoders.kryo[Row]
  import spark.implicits._

  val categoryMap:Map[String,List[String]] = null//ResultIO.readSettlementsCategoryMap()

  @transient val model = ResultIO.loadKMeansModel(singleResultPath)
  @transient val clusteringResult = ResultIO.loadClusteringResult(spark,singleResultPath)

  def parseYear(e: String) = {
    if(e.contains('(') && e.contains(')')) {
      val begin = e.indexOf('(')
      e.substring(begin, begin + e.substring(e.indexOf('(')).indexOf(')') + 1)
    } else{
      "none"
    }
  }

  def addToMultiset(set: mutable.Map[String, Int], str: String)  = {
    if(set.contains(str)){
      set(str) = set(str) +1
    } else{
      set(str) = 1
    }
  }

  def dateEvaluation() = {
    val entities = clusteringResult.map(r => (getAssignedCluster(r),getId(r).split("\\|")(0)))
    val maps = new mutable.HashMap[Long,(mutable.Map[String,Int],mutable.Map[String,Int])]()
    val vals = entities.collect()
    (0 until model.clusterCenters.size).foreach(maps(_) = (mutable.Map[String,Int](),mutable.Map[String,Int]()))
    vals.foreach{ case (id,e) =>{
      if(e.startsWith("\"") ){
        addToMultiset(maps(id)._2,(parseYear(e)))
      } else{
        addToMultiset(maps(id)._1,(parseYear(e)))
      }
    }}
    maps.foreach{case (id,(movies,series)) => {
      println("Cluster" + id)
      println("\t movies")
      movies.toList.sortBy(t => -t._2).foreach(t => println("\t" + t._1 + ":\t\t" + t._2))
      //println("\t series")
      //series.toList.sortBy(t => -t._2).foreach(t => println("\t" + t._1 + ":\t\t" + t._2))
    }}
  }

  def addQuotes(string: String) = {
    "\"" + string +"\""
  }

  def fixQuotes(str: String) = str.replace("\"","\"\"")

  def toLineString(row: Row): String = {
    val vector = getFeatureVector(row)
    val assignedCluster = getAssignedCluster(row);
    val id = fixQuotes(getId(row));
    val entity = fixQuotes(id.split("\\|")(0))
    val property = fixQuotes(id.split("\\|")(1))
    val values = vector.toArray.mkString(",")
    return addQuotes(id) + "," + addQuotes(entity) + "," + addQuotes(property) +","+ addQuotes(assignedCluster.toString)+ "," +values;
  }

  def toFlattenedTuples(row: Row): scala.TraversableOnce[(String,String,String,Long,Int,Double)] = {
    val vector = getFeatureVector(row)
    val assignedCluster = getAssignedCluster(row);
    val id = getId(row);
    val entity = id.split("\\|")(0)
    val property = id.split("\\|")(1)
//    if(property.contains(",") || entity.contains(",")){
//      println("alert!")
//    }
    vector.toArray.zipWithIndex.map{case (y,x) => (id,entity,property,assignedCluster,x,y)}
  }

  def serializeToCsv(filePathValues:String,filePathValuesTransposed:String, filepathClusterCenters:String, filepathClusterCentersTransposed:String): Unit ={
    var pr = new PrintWriter(new FileWriter(filePathValues))
    pr.print("\"id\",\"entity\",\"property\",assignedCluster")
    for(i <- 0 until model.clusterCenters(0).size){
      pr.print(",val_" + i);
    }
    pr.println();
    //first variant
    clusteringResult.map( r => toLineString(r)).collect().foreach(pr.println(_))
    pr.close()
    //second variant:
    clusteringResult.flatMap(r => toFlattenedTuples(r)).coalesce(1).write.option("escape","\"").csv(filePathValuesTransposed)
    //serialize Center
    val grouped = clusteringResult.groupByKey(r => getAssignedCluster(r))
    val clusterSizes = grouped.mapGroups{case (cluster,it) => (cluster,it.size)}.collect().toMap
    pr = new PrintWriter(new FileWriter(filepathClusterCenters))
    pr.print("\"clusterNumber\",\"clusterSize\"")
    for(i <- 0 until model.clusterCenters(0).size){
      pr.print(",val_" + i);
    }
    pr.println()
    (0 until model.clusterCenters.length).foreach({ cluster =>
      val clusterSize = if(clusterSizes.contains(cluster)) clusterSizes(cluster) else 0
      pr.println(addQuotes(cluster.toString) + "," + addQuotes(clusterSize.toString) + ","+model.clusterCenters(cluster.toInt).toArray.mkString(","))
    })
    pr.close()
    //second variant:
    pr = new PrintWriter(new FileWriter(filepathClusterCentersTransposed))
    pr.println("X,Y,clusterID_With_Size,clusterID")
    (0 until model.clusterCenters.length).foreach({ cluster =>
      model.clusterCenters(cluster.toInt).toArray.zipWithIndex.foreach{case (y,x) => pr.println(x + "," + y + "," + cluster + " (" + clusterSizes(cluster) + "),"+cluster)}
    })
    pr.close()
  }

  def drawClusteringCentroids(x:Int=0,y:Int=0) = {
    val grouped = clusteringResult.groupByKey(r => getAssignedCluster(r))
    val clusterSizes = grouped.mapGroups{case (cluster,it) => (cluster,it.size)}.collect().toMap
    val collection = new XYSeriesCollection()
    model.clusterCenters
      .zipWithIndex
      .map{case (a,i) => (a,i,if(clusterSizes.contains(i)) clusterSizes(i) else 0)}
      .sortBy( t => t._3)
      .map{case (a,i,size) => toXYSeries(a.toArray,i,size)}
      .foreach( series => collection.addSeries(series))
    //assert(model.clusterCenters.size == clusterSizes.keys.size)
    val chart:MultiLineChart  = new MultiLineChart(collection)
    chart.draw(x,y)
  }

  def toXYSeries(vector: Seq[Double], clusterNum:Int,clusterSize:Int): XYSeries ={
    val series = new XYSeries("cluster " + clusterNum.toString + " (" + clusterSize + ")")
    vector.zipWithIndex.foreach(t => series.add(t._2,t._1))
    series
  }

  def timeSeriesToString(r: Row): Any = {
    val vec: mutable.WrappedArray[Double] = getFeatureVector(r)
    timeSeriesToString(vec)
  }

  private def getFeatureVector(r: Row) = {
    val vec = r.getAs[Row]("features").getAs[mutable.WrappedArray[Double]](1)
    vec
  }

  def timeSeriesToString(vec: Seq[Double]) = {
    "[" + vec.toArray.map( d => "%.1f".format(d)).mkString(",") + "]"
  }

  def printRepresentatives(clusterId: Int) = {
    val clusterSize = clusteringResult.filter( r => getAssignedCluster(r) == clusterId).count()
    println("====================== Cluster Representatives for cluster " + clusterId + " (size:" + clusterSize +")======================")
    println("Centroid: " + timeSeriesToString(model.clusterCenters(clusterId).toArray))
    println("-----------------------------------------------------------------------------------------------------------------------------------")
    clusteringResult.filter( r => getAssignedCluster(r) == clusterId)
      .take(100)
      .foreach( r => println(getId(r) + ", " + timeSeriesToString(r)))
  }

  private def getId(r: Row) = {
    r.getAs[String]("name")
  }

  private def getAssignedCluster(r: Row) = {
    r.getAs[Long]("assignedCluster")
  }

  def printClusterRepresentatives() = {
    println("====================== Cluster Representatives ======================")
    model.clusterCenters.zipWithIndex.foreach( t => printRepresentatives(t._2) )
  }

}
