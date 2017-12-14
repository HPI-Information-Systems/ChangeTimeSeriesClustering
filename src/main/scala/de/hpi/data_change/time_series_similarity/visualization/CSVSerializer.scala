package de.hpi.data_change.time_series_similarity.visualization

import java.io.{FileWriter, PrintWriter}
import java.sql.Timestamp

import com.google.common.collect.{HashMultiset, Multiset}
import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}

import scala.collection.mutable

case class CSVSerializer(spark: SparkSession, sparkResultPath: String, csvResultPath:String) extends Serializable{


  implicit def rowEnc = org.apache.spark.sql.Encoders.kryo[Row]
  implicit def lolEnc = org.apache.spark.sql.Encoders.kryo[Seq[String]]
  //implicit def rowEnc2 = org.apache.spark.sql.Encoders.kryo[(Long,Row,Seq[String],String)]
  import spark.implicits._

  val categoryMap:Map[String,List[String]] = null//ResultIO.readSettlementsCategoryMap()

  @transient val model = KMeansModel.load(sparkResultPath + "/model")
  @transient var clusteringResult = spark.read.json(sparkResultPath+"/result")//ResultIO.loadClusteringResult(spark,singleResultPath)

  def addGroundTruth() ={
    val clusterer = new Clustering("","",spark)
    var templates = clusterer.getArbitraryQueryResult(clusterer.url,"SELECT * FROM templates_infoboxes").as[(String,String)]
    templates = templates.as("template")
    //clusteringResult.withColumn("key",$"name".apply(0))
    val changerecords = clusteringResult.as("result")
    val joined = templates.join(changerecords,$"name".apply(0) === $"template.entity")
    val actual = Set("infobox settlement",
      "infobox album",
      "infobox person",
      "infobox football biography",
      "infobox musical artist",
      "infobox film",
      "infobox single",
      "infobox actor",
      "infobox french commune",
      "infobox company",
      "infobox football biography 2",
      "infobox book",
      "infobox television",
      "infobox military person",
      "infobox nrhp",
      "infobox school",
      "infobox vg",
      "infobox officeholder",
      "infobox uk place",
      "infobox commune de france",
      "infobox radio station",
      "infobox road",
      "infobox mlb player",
      "infobox television episode",
      "infobox indian jurisdiction",
      "infobox_nrhp",
      "infobox_company",
      "infobox writer",
      "infobox university",
      "infobox city",
      "infobox military unit",
      "infobox german location",
      "infobox mountain",
      "infobox military conflict",
      "infobox scientist",
      "infobox airport",
      "infobox ice hockey player",
      "infobox_film",
      "infobox cvg",
      "infobox nfl player",
      "infobox football club",
      "infobox station",
      "infobox software")
    println("done")
    /*val withGroundTruth = joined.groupByKey(r => r.getString(0)).mapGroups{case (entity,rIt) =>
      val list = rIt.toList
      var trueCluster = list.head.getString(1)
      if(list.size > 1){
        trueCluster = "multi"
      }
      val assignedCluster = list.head.getLong(2)
      val features = list.head.getAs[Row](3)
      val name = list.head.getAs[Seq[String]](4)
      (assignedCluster,features,name,trueCluster)
    }
    var res = withGroundTruth.toDF()
    res = res.withColumnRenamed(res.columns(0),"assignedCluster")
      .withColumnRenamed(res.columns(1),"features")
      .withColumnRenamed(res.columns(2),"name")
      .withColumnRenamed(res.columns(3),"trueCluster")*/
    clusteringResult = joined.withColumnRenamed("template","trueCluster").filter(r => actual.contains(r.getAs[String]("trueCluster")))
    this
  }

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
    val entities = clusteringResult.map(r => (getAssignedCluster(r),getId(r).split(toRegex(Clustering.KeySeparator))(0)))
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

  def toRegex(s: String): String = s.replace("|","\\|")

  def toLineString(row: Row): String = {
    val vector = getFeatureVector(row)
    val assignedCluster = getAssignedCluster(row)
    val trueCluster = getTrueCluster(row)
    val id = fixQuotes(getId(row));
    //val entity = fixQuotes(id.split(toRegex(Clustering.KeySeparator))(0))
    //val property = fixQuotes(id.split(toRegex(Clustering.KeySeparator))(1))
    val values = vector.toArray.mkString(",")
    return addQuotes(id) + ","+ addQuotes(assignedCluster.toString)+ "," + addQuotes(trueCluster) + "," +values;
  }

  def toFlattenedTuples(row: Row): scala.TraversableOnce[(String,Long,String,Int,Double)] = {
    val vector = getFeatureVector(row)
    val assignedCluster = getAssignedCluster(row);
    val trueCluster = getTrueCluster(row)
    val id = getId(row);
    //val entity = id.split(toRegex(Clustering.KeySeparator))(0)
    //val property = id.split(toRegex(Clustering.KeySeparator))(1)
//    if(property.contains(",") || entity.contains(",")){
//      println("alert!")
//    }
    vector.toArray.zipWithIndex.map{case (y,x) => (id,assignedCluster,trueCluster,x,y)}
  }

  def serializeToCsv(): Unit ={
    var pr = new PrintWriter(new FileWriter(csvResultPath + "/members.csv"))
    pr.print("\"id\",assignedCluster","trueCluster")//pr.print("\"id\",\"entity\",\"property\",assignedCluster")
    for(i <- 0 until model.clusterCenters(0).size){
      pr.print(",val_" + i);
    }
    pr.println();
    //first variant
    val a = clusteringResult.map( r => toLineString(r)).collect()
    a.foreach(pr.println(_))
    pr.close()
    //second variant:
    //clusteringResult.flatMap(r => toFlattenedTuples(r)).coalesce(1).write.option("escape","\"").csv(csvResultPath + "/membersTransposed.csv")
    //serialize Center
    val grouped = clusteringResult.groupByKey(r => getAssignedCluster(r))
    val clusterSizes = grouped.mapGroups{case (cluster,it) => (cluster,it.size)}.collect().toMap
    pr = new PrintWriter(new FileWriter(csvResultPath + "/clusterCenters.csv"))
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
    pr = new PrintWriter(new FileWriter(csvResultPath + "/clusterCentersTransposed.csv"))
    pr.println("X,Y,clusterID_With_Size,clusterID")
    (0 until model.clusterCenters.length).foreach({ cluster =>
      model.clusterCenters(cluster.toInt).toArray.zipWithIndex.foreach{case (y,x) => pr.println(x + "," + y + "," + cluster + " (" + clusterSizes(cluster) + "),"+cluster)}
    })
    pr.close()
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
    r.getAs[Seq[String]]("name").mkString(Clustering.KeySeparator)
  }

  private def getAssignedCluster(r: Row) = {
    r.getAs[Long]("assignedCluster")
  }

  private def getTrueCluster(r: Row) = {
    r.getAs[String]("trueCluster")
  }


  def printClusterRepresentatives() = {
    println("====================== Cluster Representatives ======================")
    model.clusterCenters.zipWithIndex.foreach( t => printRepresentatives(t._2) )
  }

}
