package de.hpi.data_change.time_series_similarity.configuration

import de.hpi.data_change.time_series_similarity.data.MultiDimensionalTimeSeries

case class TimeSeriesFilter(params:Map[String,String]) extends Serializable{

  assert(params.contains("name"))
  var maxavg:Double = Double.MaxValue
  var minNumYVals:Double = 0.0

  var filter: (MultiDimensionalTimeSeries => Boolean) = null

  params("name") match {
    case "MaxAverageY" => initAverageYFilter()
    case "MinNonZeryYVals" => initMinNonZeroYFilter()
    case _ => assert(false)
  }

  def avgYFilter(ts: MultiDimensionalTimeSeries):Boolean = {
    val yVals = ts.yValues()
    yVals.sum / yVals.size <= maxavg
  }

  def minNumNonZeroFilter(ts: MultiDimensionalTimeSeries):Boolean = {
    ts.numNonZeroYValues >= minNumYVals
  }

  def initAverageYFilter() = {
    assert(params.contains("maxAvg"))
    maxavg = params("maxAvg").toDouble
    filter = avgYFilter
  }

  def initMinNonZeroYFilter() = {
    assert(params.contains("minNumNonZero"))
    minNumYVals = params("minNumNonZero").toInt
    filter = minNumNonZeroFilter
  }

}
