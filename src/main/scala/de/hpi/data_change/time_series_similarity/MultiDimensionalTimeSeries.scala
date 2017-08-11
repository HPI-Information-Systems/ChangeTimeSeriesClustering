package de.hpi.data_change.time_series_similarity

import java.sql.Timestamp
import scala.collection.Map

case class MultiDimensionalTimeSeries(name:String, data:Map[String,Seq[Double]], timeAxis:Seq[Timestamp]) {

  val timeSeries: Array[Array[Double]] = Array.ofDim[Double](data.keys.size,timeAxis.size)
  val dimNames = data.keys.toList.sorted
  //TODO: assert that time series is sorted
  dimNames.foreach(k => putData(k,data(k)))
  val numNonZeroYValues: Int = timeSeries.flatten.count(d => d != 0)

  def putData(k: String, values: Seq[Double]): Unit = {
    assert(values.size==timeAxis.size)
    if(values.size!=timeAxis.size){
      throw new IllegalArgumentException("number of values does not match number of timestamps for key " + k)
    }
    timeSeries(dimNames.indexOf(k)) = values.toArray
  }

  def get(dim:String,i:Int) = timeSeries(dimNames.indexOf(dim))(i)

  def get(dim:Int,col:Int) = timeSeries(dim)(col)

  def getDim(dim:String) = timeSeries(dimNames.indexOf(dim))

  def manhattenDistance(other:MultiDimensionalTimeSeries):Double = {
    assert(dims()==other.dims())
    val otherTimeSeries = other.timeSeries
    var dist = 0.0
    for( i <- 0 until dims()._1; j <- 0 until dims()._2){
      dist += Math.abs(other.get(i,j) - get(i,j))
    }
    dist
  }

  def dims() = (dimNames.size,timeAxis.size)

  override def toString: String = {
    name + "(numDimensions: " + dimNames.size + ", numTimePoints: " + timeSeries.length + ")"
  }
}
