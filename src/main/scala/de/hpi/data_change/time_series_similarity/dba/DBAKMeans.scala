package de.hpi.data_change.time_series_similarity.dba

import de.hpi.data_change.time_series_similarity.dba.java.ModifiedDBA
import net.sf.javaml.core.DenseInstance
import net.sf.javaml.distance.dtw.DTWSimilarity
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql._


class DBAKMeans(k: Int, maxIter: Int, seed: Long, spark: SparkSession) extends Serializable {

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
    for (i <- 0 until centers.size) {
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

  def getNewCenter(oldCenter:Array[Double],iterator: Iterator[(Seq[String], Array[Double], Int)]): Array[Double] = {
    val timeSeries = scala.collection.JavaConversions.asJavaIterator(iterator.map( t => t._2));
    val dba = new ModifiedDBA()
    dba.DBA(oldCenter,timeSeries)
  }

  def recalculateCenters(centers:Seq[Array[Double]],curAssignements: Dataset[(Seq[String], Array[Double], Int)]) = {
    curAssignements.groupByKey(t => t._3)
      .mapGroups{case (clusterId,iterator) => (getNewCenter(centers(clusterId),iterator),clusterId)}
      .collect().sortBy( t => t._2).map( t => t._1) //sort to preserve cluster order - which is important for the check if something changed!
  }

  def nothingChanged(lastAssignments: Dataset[(Seq[String], Array[Double], Int)], curAssignements: Dataset[(Seq[String], Array[Double], Int)]): Boolean = {
    var prev = lastAssignments.map( t => (t._1,t._3))
    var now = curAssignements.map( t => (t._1,t._3))
    val prevDF = prev.withColumnRenamed(prev.columns(0),"name")
      .withColumnRenamed(prev.columns(1),"cluster")
        .as("prev")
    val nowDF = now.withColumnRenamed(now.columns(0),"name")
      .withColumnRenamed(now.columns(1),"cluster")
      .as("now")
    val res = prevDF.join(nowDF,$"prev.name" === $"now.name").filter($"prev.cluster" =!= $"now.cluster")
    res.count() == 0
  }

  def fit(finalDF: DataFrame) = {
    //randomize k cluster centers from finalDF
    val fraction = k / finalDF.count().toDouble + 0.0001
    var centers:Seq[Array[Double]] = finalDF.sample(false,fraction,seed).take(k).map(r => getFeatures(r))
    if(centers.size!=k){
      centers = centers ++ finalDF.take(k-centers.size).map(r => getFeatures(r))
    }
    var lastAssignments = finalDF.map( r => getMembership(r,centers) )
    var curAssignements = lastAssignments
    var i = 0
    var done = false
    while(i < maxIter && !done){ //TODO: check if assignements changed!
      println($"Iteration $i out of $maxIter")
      centers = recalculateCenters(centers,lastAssignments)
      curAssignements = lastAssignments.map{case (name,features,_) => getMembership(name,features,centers)}
      if(nothingChanged(lastAssignments,curAssignements)){
        done = true
      } else {
        i +=1
      }

    }
    (centers,curAssignements
      .withColumnRenamed(curAssignements.columns(0),"name")
      .withColumnRenamed(curAssignements.columns(1),"features")
      .withColumnRenamed(curAssignements.columns(2),"assignedCluster"))
  }


}
