package de.hpi.data_change.time_series_similarity.data

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.Row

class ChangeRecord(val entity:String, val property:String,val value:String,val timestamp:LocalDateTime) {

  def this(r:Row){
    this(r.getString(ChangeRecord.entityIndex)
      ,r.getString(ChangeRecord.propertyIndex),
      r.getString(ChangeRecord.valueIndex),
      LocalDateTime.parse(r.getString(ChangeRecord.datetimeIndex),ChangeRecord.formatter))
  }
}

//for storing indice constants:
object ChangeRecord{
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
  val (entityIndex,propertyIndex,valueIndex,datetimeIndex) = (0,1,2,3)
}
