package de.hpi.data_change.time_series_similarity.serialization

import java.io._

import de.hpi.data_change.time_series_similarity.Clustering
import org.apache.spark.sql._

import scala.util.Try

/**
  * Class to serialize results as csv files
  * @param spark
  * @param resultPath
  * @param clusterCenters
  * @param clusteringResult
  */
case class CSVSerializer(spark: SparkSession, resultPath: String, clusterCenters:Seq[Array[Double]],clusteringResult:DataFrame) extends Serializable{

  import spark.implicits._

  def addQuotes(string: String) = {
    "\"" + string +"\""
  }

  def fixQuotes(str: String) = str.replace("\"","\"\"")

  def toRegex(s: String): String = s.replace("|","\\|")

  def hasTrueCluster(row: Row): Boolean = Try(row.getAs[String]("trueCluster")).isSuccess

  def toLineString(row: Row): String = {
    val features = getFeatures(row)
    val assignedCluster = getAssignedCluster(row)
    var trueCluster:String = null
    if(hasTrueCluster(row)) {
      trueCluster = getTrueCluster(row)
    }
    val id = fixQuotes(getId(row));
    val values = features.mkString(",")
    if(trueCluster!=null) {
      addQuotes(id) + "," + addQuotes(assignedCluster.toString) + "," + addQuotes(trueCluster) + "," + values;
    } else{
      addQuotes(id) + "," + addQuotes(assignedCluster.toString) + "," + values;
    }
  }


  def toMaxLength(center: Array[Double], maxCenterLength: Int) = {
    center ++ Array.fill(maxCenterLength-center.size)(Double.NaN)
  }

  def printLinesToSingleFile(ds: Dataset[String], filename: String) = {
    ds.coalesce(1).write.text(resultPath + filename)
  }

  def serializeToCsv(): Unit ={
    printLinesToSingleFile(clusteringResult.map(toLineString(_)),"/members.csv")
    //centers:
    val maxCenterLength = clusterCenters.map(_.size).max
    val centersAsString = spark.createDataset(clusterCenters).map(center => toMaxLength(center,maxCenterLength).mkString(","))
    printLinesToSingleFile(centersAsString,"/centers.csv")
    //centers transposed:
    val transposedLines = (0 until clusterCenters.size).flatMap({ cluster =>
      toMaxLength(clusterCenters(cluster),maxCenterLength).zipWithIndex.map{case (y,x) => x + "," + y + "," + cluster }
    })
    printLinesToSingleFile(spark.createDataset(transposedLines),"/clusterCentersTransposed.csv")
  }

  def timeSeriesToString(r: Row): Any = {
    val vec = getFeatures(r)
    timeSeriesToString(vec)
  }

  def timeSeriesToString(vec: Seq[Double]) = {
    "[" + vec.toArray.map( d => "%.1f".format(d)).mkString(",") + "]"
  }

  private def getFeatures(r: Row) = {
    val res = r.getAs[Any]("features")
    if(res.isInstanceOf[org.apache.spark.ml.linalg.Vector]){
      res.asInstanceOf[org.apache.spark.ml.linalg.Vector].toArray
    } else {
      res.asInstanceOf[Seq[Double]].toArray
    }
  }
  private def getId(r: Row) = {
    r.getAs[Seq[String]]("name").mkString(Clustering.KeySeparator)
  }

  private def getAssignedCluster(r: Row) = {
    r.getAs[Int]("assignedCluster")
  }

  private def getTrueCluster(r: Row) = {
    r.getAs[String]("trueCluster")
  }
}
