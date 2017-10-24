package de.hpi.data_change.time_series_similarity.data

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import de.hpi.data_change.time_series_similarity.configuration.TimeGranularity
import org.apache.spark.sql.catalyst.expressions.Month

import scala.collection.mutable.ListBuffer

class TimeGranularityGrouper(minYear:Int, maxYear:Int, padWithZeroesFront:Boolean = true) extends Serializable{

  def this(){
    this(0,0,false)
  }

  def toSingleDimensionalTimeSeries(key: String, changeRecords: Iterator[ChangeRecord], granularity:TimeGranularity.Value ):TimeSeries = {
    granularity match {
      case TimeGranularity.Yearly => toSingleDimensionalYearlyTimeSeries(key,changeRecords)
      case TimeGranularity.Monthly => toSingleDimensionalMonthlyTimeSeries(key,changeRecords)
      case TimeGranularity.Daily => toSingleDimensionalDailyTimeSeries(key,changeRecords)
      case TimeGranularity.MonthOfYear => toMonthOfYearTimeSeries(key,changeRecords)
      case TimeGranularity.WeekOfYear => toMonthOfYearTimeSeries(key,changeRecords)
      case _ => println("received: " + granularity);throw new AssertionError("unknown time Granularity specified: " + granularity)
    }
  }

  def toStandardMonth(month: Int) = {
    LocalDateTime.of(1, month, 1, 0, 0)
  }

  def toMonthOfYearTimeSeries(key: String, changeRecords: Iterator[ChangeRecord]): TimeSeries = {
    val asMap = changeRecords.toList.groupBy(cr => cr.timestamp.getMonthValue)
    val asTupleList = asMap.mapValues(crList => crList.size).toList.toBuffer
    (1 to 12).toList
      .filter(m => !asMap.contains(m))
      .foreach(m => asTupleList.append((m,0)))
    val sorted = asTupleList.toList.sortBy ( x => x._1)
    val finalVals = sorted.map( t => t._2.toDouble)//  Map(("num Changes",sorted.map( t => t._2))).mapValues(l => l.map(i => i.toDouble))
    val timestamps = sorted.map(t => Timestamp.valueOf(toStandardMonth(t._1)))
    TimeSeries(key,finalVals,timestamps)
  }

  def toSingleDimensionalYearlyTimeSeries(key: String, changeRecords: Iterator[ChangeRecord]): TimeSeries = {
    val asMap = changeRecords.toList.groupBy(cr => toStandardDateInYear(cr.timestamp.getYear))
    val asTupleList = asMap.mapValues(crList => crList.size).toList.toBuffer
    if (padWithZeroesFront) {
    (minYear to maxYear).toList
      .filter(year => !asMap.contains(toStandardDateInYear(year)))
      .foreach(year => asTupleList.append((toStandardDateInYear(year), 0)))
    }
    val sorted = asTupleList.toList.sortBy ( x => x._1.toEpochSecond(ZoneOffset.UTC))
    val finalVals = sorted.map( t => t._2.toDouble)//  Map(("num Changes",sorted.map( t => t._2))).mapValues(l => l.map(i => i.toDouble))
    val timestamps = sorted.map(t => Timestamp.from(t._1.toInstant(ZoneOffset.UTC)))
    TimeSeries(key,finalVals,timestamps)
  }

  def toSingleDimensionalMonthlyTimeSeries(label: String, changeRecords: Iterator[ChangeRecord]): TimeSeries = {
    val asMap = changeRecords.toList.groupBy( cr => toStandardDateInMonth(cr.timestamp.getYear,cr.timestamp.getMonthValue) )
    val asTupleList = asMap.mapValues(crList => crList.size).toList.toBuffer
    if(padWithZeroesFront) {
      (minYear to maxYear).toList
        .flatMap(year => (1 to 12).map(month => (year, month)))
        .filter { case (year, month) => !asMap.contains(toStandardDateInMonth(year, month)) }
        .foreach { case (year, month) => asTupleList.append((toStandardDateInMonth(year, month), 0)) }
    }
    val sorted = asTupleList.toList.sortBy ( x => x._1.toEpochSecond(ZoneOffset.UTC))
    val finalVals = sorted.map( t => t._2.toDouble)
    val timestamps = sorted.map(t => Timestamp.from(t._1.toInstant(ZoneOffset.UTC)))
    TimeSeries(label,finalVals,timestamps)
  }

  def getAllDays(year: Int): TraversableOnce[LocalDateTime] = {
    val firstDay = toStandardDayInYear(year,1,1)
    val lastDay = toStandardDayInYear(year,12,31)
    val days = new ListBuffer[LocalDateTime]
    var curDay = firstDay
    while(curDay.compareTo(lastDay)<= 0 ){
      days.append(curDay)
      curDay = curDay.plusDays(1)
    }
    days
  }

  def toSingleDimensionalDailyTimeSeries(label: String, changeRecords: Iterator[ChangeRecord]): TimeSeries = {
    val asMap = changeRecords.toList.groupBy( cr => toStandardDayInYear(cr.timestamp.getYear,cr.timestamp.getMonthValue,cr.timestamp.getDayOfMonth) )
    val asTupleList = asMap.mapValues(crList => crList.size).toList.toBuffer
    if(padWithZeroesFront) {
      (minYear to maxYear).toList
        .flatMap(year => getAllDays(year))
        .filter(date => !asMap.contains(date))
        .foreach(date => asTupleList.append((date, 0)))
    }
    val sorted = asTupleList.toList.sortBy ( x => x._1.toEpochSecond(ZoneOffset.UTC))
    val finalVals = sorted.map( t => t._2.toDouble)
    val timestamps = sorted.map(t => Timestamp.from(t._1.toInstant(ZoneOffset.UTC)))
    TimeSeries(label,finalVals,timestamps)
  }

  def toStandardDateInYear(year: Int):LocalDateTime = {
    val (month,day,hour,second) = (1,1,12,0)
    LocalDateTime.of(year, month, day, hour, second)
  }

  def toStandardDateInMonth(year: Int, month: Int): LocalDateTime = {
    val (day,hour,second) = (1,12,0)
    LocalDateTime.of(year, month, day, hour, second)
  }

  def toStandardDayInYear(year: Int, month: Int, day: Int): LocalDateTime = {
    val (hour,second) = (12,0)
    LocalDateTime.of(year, month, day, hour, second)
  }
}
