package de.hpi.data_change.time_series_similarity.data

import java.sql.Timestamp

import scala.collection.Map

case class TimeSeries(name:String, values:Seq[Double], timeAxis:Seq[Timestamp]) {

  //TODO: assert timeAxis sorted
  def normalize():TimeSeries = {
    val max = values.max
    val mappedData = values.map(y => y/max)
    TimeSeries(name,mappedData,timeAxis)
  }

  def yValues() = timeSeries

  def getClusteringFeatures() = timeSeries

  val timeSeries: Array[Double] = values.toArray

  val numNonZeroYValues: Int = timeSeries.count(d => d != 0)

  def safeToLog(y: Double): Double = if(y >1 ) Math.log(y) else y

  def yAsLog() = {
    val mappedData = values.map(y => safeToLog(y))
    TimeSeries(name,mappedData,timeAxis)
  }

  def manhattenDistance(other:TimeSeries):Double = {
    assert(dims()==other.dims())
    val otherTimeSeries = other.timeSeries
    var dist = 0.0
    for( i <- 0 until dims()){
      dist += Math.abs(other.timeSeries(i) - timeSeries(i))
    }
    dist
  }

  def dims() = timeAxis.size

  override def toString: String = {
    name + "(numTimePoints: " + timeSeries.length + ")"
  }
}
