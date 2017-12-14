package de.hpi.data_change.time_series_similarity.data

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import scala.collection.mutable.ListBuffer

case class TimeSeries(id:Seq[String], yValues:Seq[Double], step:Integer, stepUnit:String, begin:java.sql.Timestamp) {

  def transform(method:String): TimeSeries ={
    method.toLowerCase.trim match {
        case "none" => this
        case "normalize" => {
          val max = yValues.max
          TimeSeries(id,yValues.map(y => y /max),step,stepUnit,begin)
        }
        //TODO: if we want to do that we need to find a way to represent NaN (?) in KMeans -> check if this works case "removestartingzeros" =>
        case "log" => TimeSeries(id,yValues.map(y => Math.log(y)),step,stepUnit,begin)
        case "sqrt" => TimeSeries(id,yValues.map(y => Math.sqrt(y)),step,stepUnit,begin)
        case _  => throw new AssertionError("unknown time series transformation method")
      }
  }

  def featureExtraction(method:String):Array[Double] = {
    method.toLowerCase.trim match {
      case "raw" => yValues.toArray
      case "statistics" => Array(yValues.min,yValues.max,yValues.sum/yValues.size)
      case _ => throw new AssertionError("unknown time series feature extraction method")
    }
  }

  def filter(): Boolean = {true}

}
