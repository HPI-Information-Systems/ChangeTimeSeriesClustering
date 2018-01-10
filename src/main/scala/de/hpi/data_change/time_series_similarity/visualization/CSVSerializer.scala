package de.hpi.data_change.time_series_similarity.visualization

import java.io._
import java.sql.Timestamp

import com.google.common.collect.{HashMultiset, Multiset}
import de.hpi.data_change.time_series_similarity.Clustering
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.jfree.data.xy.{XYSeries, XYSeriesCollection}

import scala.collection.{immutable, mutable}
import scala.util.Random

case class CSVSerializer(spark: SparkSession, resultPath: String, clusterCenters:Seq[Array[Double]],clusteringResult:DataFrame) extends Serializable{


  //implicit def lolEnc = org.apache.spark.sql.Encoders.kryo[Seq[String]]
  //implicit def rowEnc2 = org.apache.spark.sql.Encoders.kryo[(Long,Row,Seq[String],String)]
  import spark.implicits._

  def addQuotes(string: String) = {
    "\"" + string +"\""
  }

  def fixQuotes(str: String) = str.replace("\"","\"\"")

  def toRegex(s: String): String = s.replace("|","\\|")

  def toLineString(row: Row): String = {
    val vector = getFeatures(row)
    val assignedCluster = getAssignedCluster(row)
    val trueCluster = getTrueCluster(row)
    val id = fixQuotes(getId(row));
    val values = vector.toArray.mkString(",")
    return addQuotes(id) + ","+ addQuotes(assignedCluster.toString)+ "," + addQuotes(trueCluster) + "," +values;
  }

  def toFlattenedTuples(row: Row): scala.TraversableOnce[(String,Long,String,Int,Double)] = {
    val vector = getFeatures(row)
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

  def toMaxLength(center: Array[Double], maxCenterLength: Int) = {
    center ++ Array.fill(maxCenterLength-center.size)(Double.NaN)
  }

  def printLinesToSingleFile(ds: Dataset[String], filename: String) = {
    ds.coalesce(1).write.text(resultPath + filename)
  }

  def serializeToCsv(): Unit ={
    //members:
    val header = List("id","assignedCluster","trueCluster") ++ (0 until getFeatures(clusteringResult.head).size).toList.map("val_"+_)
    val headerAsString = spark.sparkContext.parallelize(List(header.mkString(","))).toDS()
    printLinesToSingleFile(headerAsString.union(clusteringResult.map(toLineString(_))),"/members.csv")
    //centers:
    val maxCenterLength = clusterCenters.map(_.size).max
    val centerHeadeAsrString = spark.createDataset((0 until maxCenterLength).toList.map("val_"+_))
    val centersAsString = spark.createDataset(clusterCenters).map(center => toMaxLength(center,maxCenterLength).mkString(","))
    printLinesToSingleFile(centerHeadeAsrString.union(centersAsString),"/centers.csv")
    //centers transposed:
    val transposedLines = (0 until maxCenterLength).flatMap({ cluster =>
      clusterCenters(cluster.toInt).toArray.zipWithIndex.map{case (y,x) => x + "," + y + "," + cluster }
    })
    printLinesToSingleFile(spark.createDataset(transposedLines),"/clusterCentersTransposed.csv")
  }

  def timeSeriesToString(r: Row): Any = {
    val vec: mutable.WrappedArray[Double] = getFeatures(r)
    timeSeriesToString(vec)
  }

  def timeSeriesToString(vec: Seq[Double]) = {
    "[" + vec.toArray.map( d => "%.1f".format(d)).mkString(",") + "]"
  }

  private def getFeatures(r: Row) = {
    r.getAs[Row]("features").getAs[mutable.WrappedArray[Double]](1)
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
}
