package de.hpi.data_change.time_series_similarity.data

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.csv.CSVRecord
import org.apache.spark.sql.Row

class ChangeRecord(val entity:String, val property:String,val value:String,val timestamp:LocalDateTime) {

  def timestampAsString: String = ChangeRecord.formatter.format(timestamp)

  def this(r:CSVRecord){
    this(ChangeRecord.transformIfNull(r.get(ChangeRecord.entityIndex)),
      ChangeRecord.transformIfNull(r.get(ChangeRecord.propertyIndex)),
      ChangeRecord.transformIfNull(r.get(ChangeRecord.valueIndex)),
      LocalDateTime.parse(r.get(ChangeRecord.datetimeIndex),ChangeRecord.formatter))
  }


  def this(r:Row){
    this(ChangeRecord.transformIfNull(r.getString(ChangeRecord.entityIndex)),
      ChangeRecord.transformIfNull(r.getString(ChangeRecord.propertyIndex)),
      ChangeRecord.transformIfNull(r.getString(ChangeRecord.valueIndex)),
      ChangeRecord.getTimeStamp(r))
  }


  override def toString = s"ChangeRecord($entity, $property, $value, $timestamp)"
}

//for storing indice constants:
object ChangeRecord{
  //","2009-11-29 00:31:15.000000"
  def getTimeStamp(r: Row): java.time.LocalDateTime = {
    val ts = r.get(datetimeIndex)
    if(ts.isInstanceOf[String]) {
      var datetimestr = r.getString(datetimeIndex)
      if (!datetimestr.contains(' ')) datetimestr = datetimestr + " 00:00:01.000000"
      LocalDateTime.parse(datetimestr, ChangeRecord.formatter)
    } else{
      assert(ts.isInstanceOf[java.sql.Timestamp])
      ts.asInstanceOf[java.sql.Timestamp].toLocalDateTime
    }
  }


  def transformIfNull(str: String): String = {
    if(str==null) "null" else str
  }

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.[S][S][S][S][S][S]") //TODO: make time optional
  val (entityIndex,propertyIndex,valueIndex,datetimeIndex) = (0,1,2,3)
}
