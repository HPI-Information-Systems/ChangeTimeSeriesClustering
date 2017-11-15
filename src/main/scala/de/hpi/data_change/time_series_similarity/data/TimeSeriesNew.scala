package de.hpi.data_change.time_series_similarity.data

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import scala.collection.mutable.ListBuffer

case class TimeSeriesNew(id:String,yValues:Seq[Double],aggregationGranularityInDays:Integer,earliestTimestamp:java.sql.Timestamp,latestTimestamp:java.sql.Timestamp) {

  def filterByMinNumNonZeroValues(min:Integer): Boolean = yValues.filter(value => value > 0).size >= min

  def toFeatures() = yValues.toArray

  def filter(): Boolean = {true}


  def transform(): TimeSeriesNew = {
    this
  }


}
