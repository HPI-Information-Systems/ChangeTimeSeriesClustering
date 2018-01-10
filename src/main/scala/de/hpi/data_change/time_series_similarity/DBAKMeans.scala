package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.dba.DBA
import net.sf.javaml.core.DenseInstance
import net.sf.javaml.distance.dtw.DTWSimilarity
import org.apache.spark.sql._
import org.apache.spark.ml.linalg.Vector


class DBAKMeans(k: Int, maxIter: Int, seed: Long, spark: SparkSession) {

  import spark.implicits._

  implicit def changeRecordListEncoder: Encoder[(String,Array[Double],Int)] = org.apache.spark.sql.Encoders.kryo[(String,Array[Double],Int)]

  def getFeatures(r: Row) = r.getAs[Vector]("features").toArray

  def getName(r: Row) = r.getAs[Seq[String]]("name")

  def getMembership(name: Seq[String], features: Array[Double], centers: Seq[Array[Double]]):(Seq[String],Array[Double],Int) = {
    //TODO: maybe replace this with the dba implementation - just to be safe
    val sim: DTWSimilarity = new DTWSimilarity
    val curTimeSeries = features
    var minDist = Double.PositiveInfinity
    var minI = -1
    for (i <- 1 until centers.size) {
      val dist = sim.measure(new DenseInstance(curTimeSeries), new DenseInstance(centers(i).toArray))
      if (dist < minDist) {
        minDist = dist
        minI = i
      }
    }
    (name, features, minI)
  }

  def getMembership(r: Row, centers:Seq[Array[Double]]):(Seq[String],Array[Double],Int) = {
    val name = getName(r)
    val features = getFeatures(r)
    getMembership(name,features,centers)
  }

  def getNewCenter(iterator: Iterator[(Seq[String], Array[Double], Int)]): Array[Double] = {
    val timeSeries = scala.collection.JavaConversions.asJavaIterator(iterator.map( t => t._2));
    val toAverage =  Array(1.0,2.0)//TODO: select initial center
    DBA.DBA(toAverage,timeSeries)
  }

  def recalculateCenters(curAssignements: Dataset[(Seq[String], Array[Double], Int)]): scala.Seq[_root_.scala.Array[Double]] = {
    curAssignements.groupByKey(t => t._3)
      .mapGroups{case (clusterId,iterator) => getNewCenter(iterator)}
      .collect()
  }

  def fit(finalDF: DataFrame) = {
    //randomize k cluster centers from finalDF
    var centers:Seq[Array[Double]] = finalDF.take(k).map( r => getFeatures(r)).toSeq //TODO: make this randomized
    var curAssignements = finalDF.map( r => getMembership(r,centers) )
    for(i <- 0 until maxIter){
      centers = recalculateCenters(curAssignements)
      curAssignements = curAssignements.map{case (name,features,_) => getMembership(name,features,centers)}
    }
    (centers,curAssignements.toDF())
  }


}
