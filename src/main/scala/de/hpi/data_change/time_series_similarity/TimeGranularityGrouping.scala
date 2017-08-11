package de.hpi.data_change.time_series_similarity

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}

import scala.collection.mutable.ListBuffer

class TimeGranularityGrouping(minYear:Int,maxYear:Int) extends Serializable{

  def toSingleDimensionalTimeSeries(entity: String, changeRecords: Iterator[ChangeRecord], granularity:TimeGranularity.Value ):MultiDimensionalTimeSeries = {
    granularity match {
      case TimeGranularity.Yearly => toSingleDimensionalYearlyTimeSeries(entity,changeRecords)
      case TimeGranularity.Monthly => toSingleDimensionalMonthlyTimeSeries(entity,changeRecords)
      case TimeGranularity.Daily => toSingleDimensionalDailyTimeSeries(entity,changeRecords)
      case _ => println("received: " + granularity);throw new AssertionError("unknown time Granularity specified: " + granularity)
    }
  }


  def toSingleDimensionalYearlyTimeSeries(label: String, changeRecords: Iterator[ChangeRecord]): MultiDimensionalTimeSeries = {
    val asMap = changeRecords.toList.groupBy( cr => toStandardDateInYear(cr.timestamp.getYear) )
    val asTupleList = asMap.mapValues(crList => crList.size).toList.toBuffer
    (minYear to maxYear).toList
      .filter(year => !asMap.contains(toStandardDateInYear(year)))
        .foreach(year => asTupleList.append((toStandardDateInYear(year),0)))
    val sorted = asTupleList.toList.sortBy ( x => x._1.toEpochSecond(ZoneOffset.UTC))
    val finalMap = Map(("num Changes",sorted.map( t => t._2))).mapValues(l => l.map(i => i.toDouble))
    val timestamps = sorted.map(t => Timestamp.from(t._1.toInstant(ZoneOffset.UTC)))
    MultiDimensionalTimeSeries(label,finalMap,timestamps)
  }

  def toSingleDimensionalMonthlyTimeSeries(label: String, changeRecords: Iterator[ChangeRecord]): MultiDimensionalTimeSeries = {
    val asMap = changeRecords.toList.groupBy( cr => toStandardDateInMonth(cr.timestamp.getYear,cr.timestamp.getMonthValue) )
    val asTupleList = asMap.mapValues(crList => crList.size).toList.toBuffer
    (minYear to maxYear).toList
        .flatMap(year => (1 to 12).map( month => (year,month)) )
        .filter { case (year,month) => !asMap.contains(toStandardDateInMonth(year,month))}
        .foreach{ case (year,month) => asTupleList.append((toStandardDateInMonth(year,month),0))}
    val sorted = asTupleList.toList.sortBy ( x => x._1.toEpochSecond(ZoneOffset.UTC))
    val finalMap = Map(("num Changes",sorted.map( t => t._2))).mapValues(l => l.map(i => i.toDouble))
    val timestamps = sorted.map(t => Timestamp.from(t._1.toInstant(ZoneOffset.UTC)))
    MultiDimensionalTimeSeries(label,finalMap,timestamps)
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

  def toSingleDimensionalDailyTimeSeries(label: String, changeRecords: Iterator[ChangeRecord]): MultiDimensionalTimeSeries = {
    val asMap = changeRecords.toList.groupBy( cr => toStandardDayInYear(cr.timestamp.getYear,cr.timestamp.getMonthValue,cr.timestamp.getDayOfMonth) )
    val asTupleList = asMap.mapValues(crList => crList.size).toList.toBuffer
    (minYear to maxYear).toList
      .flatMap(year => getAllDays(year) )
      .filter( date => !asMap.contains(date))
      .foreach(date => asTupleList.append( (date,0)))
    val sorted = asTupleList.toList.sortBy ( x => x._1.toEpochSecond(ZoneOffset.UTC))
    val finalMap = Map(("num Changes",sorted.map( t => t._2))).mapValues(l => l.map(i => i.toDouble))
    val timestamps = sorted.map(t => Timestamp.from(t._1.toInstant(ZoneOffset.UTC)))
    MultiDimensionalTimeSeries(label,finalMap,timestamps)
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
