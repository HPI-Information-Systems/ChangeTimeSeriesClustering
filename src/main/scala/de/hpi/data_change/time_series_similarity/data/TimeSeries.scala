package de.hpi.data_change.time_series_similarity.data

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class TimeSeries(id:Seq[String], yValues:Seq[Double], step:Integer, stepUnit:String, begin:java.sql.Timestamp) {

  def transform(methods:Seq[String]): TimeSeries ={
    //test if this works
    methods.size match{
      case 0 => this
      case _ => {
        transform(methods.head).transform(methods.tail)
      }
    }
  }

  def transform(method: String): TimeSeries ={
    method.toLowerCase.trim match {
    case "none" => this
    case "normalize" => normalize
    case "log" => log
    case "sqrt" => sqrt
    case "removeleadingzero" => removeLeadingZeros
    case "cutoffat" => cutoffAt
    case _  => throw new AssertionError("unknown time series transformation method")
    }
  }

  def pointDist(d: Double, d1: Double) = Math.pow(d-d1,2) //TODO - wirklich quadrieren?

  def dtwDistance(other: TimeSeries): (Double,(Int,Int)) ={
    val A = yValues
    val B = other.yValues
    val S = A.size
    val T = B.size
    val m:mutable.Seq[mutable.Seq[Double]] = mutable.ListBuffer.fill(S)(mutable.ListBuffer.fill(T)(0.0))
    m(0)(0) = pointDist(A(0),B(0))
    for(i <- 1 until S){
      m(i)(0) = m(i - 1)(0) + pointDist(A(i), B(0))
    }
    for(j <- 1 until T){
      m(0)(j) = m(0)(j-1) + pointDist(A(0),B(j))
    }
    (1 until S).foreach(i => {
      (1 until T).foreach( j => {
        val minimum = List( m(i-1)(j),m(i)(j-1),m(i-1)(j-1)).min
        m(i)(j) = minimum + pointDist(A(i),B(j))
      })
    })
    (m(S-1)(T-1),(-1,-1))
  }

  def removeLeadingZeros(): TimeSeries = {
    val sliceAfter = yValues.slice(yValues.indexWhere(y => y>0),yValues.size)
    val zeroSlice = List.fill(yValues.size-sliceAfter.size)(0.0)
    TimeSeries(id,sliceAfter ++ zeroSlice,step,stepUnit,begin)
  }

  def cutoffAt(): TimeSeries ={
    val cutoff = 3 //TODO: make this a parameter
    val unit = ChronoUnit.YEARS
    val border = begin.toLocalDateTime.plus(cutoff,unit)
    val otherUnit = ChronoUnit.valueOf(stepUnit.toUpperCase)
    val toKeep = otherUnit.between(begin.toLocalDateTime,border) / step
    TimeSeries(id,yValues.slice(0,toKeep.toInt),step,stepUnit,begin)
  }

  def sqrt: TimeSeries = TimeSeries(id,yValues.map(y => Math.sqrt(y)),step,stepUnit,begin)

  def log(): TimeSeries = TimeSeries(id,yValues.map(y => Math.log(y+1)),step,stepUnit,begin)

  def normalize(): TimeSeries ={
      val mean = yValues.sum/yValues.size
      val std = Math.sqrt(yValues.map( y => Math.pow(y-mean,2)).sum/yValues.size)
      TimeSeries(id,yValues.map(y => (y -mean)/std),step,stepUnit,begin)
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
