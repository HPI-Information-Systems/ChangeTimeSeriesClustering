package de.hpi.data_change.time_series_similarity.data_mining

import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.configuration.{FeatureExtractionMethod, GroupingKey, TimeGranularity, TimeSeriesFilter}
import de.hpi.data_change.time_series_similarity.data.{ChangeRecord, MultiDimensionalTimeSeries, TimeGranularityGrouper}
import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset, SparkSession}

case class TimeSeriesAggregator(spark:SparkSession,timeSeriesFilter:TimeSeriesFilter, granularity: TimeGranularity.Value, groupingKey: GroupingKey.Value) {

  import spark.implicits._
  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]

  def aggregateToTimeSeries(filePath:String,featureExtractionMethod:FeatureExtractionMethod.Value = FeatureExtractionMethod.EntireTimeSeries): Dataset[MultiDimensionalTimeSeries] = {
    val changeRecords = getChangeRecordDataSet(filePath)
    val distinctYears = changeRecords.map(cr => cr.timestamp.getYear).distinct().collect().toList //TODO: if we aggregate daily, we will maybe get some zeros padded to all timeseries
    val groupingObject = new TimeGranularityGrouper(distinctYears.min,distinctYears.max)
    //accumulation to multidimensional time series:
    println("num Distinct years: " + distinctYears.size)
    aggregateToTimeSeries(changeRecords,groupingObject,featureExtractionMethod)
  }

  def getChangeRecordDataSet(filePath:String): Dataset[ChangeRecord] ={
    val rawData = spark.read.csv(filePath)
    rawData.filter(r => r.getString(3) !=null).map( new ChangeRecord(_))
  }

  def aggregateToTimeSeries(resAsCR: Dataset[ChangeRecord], groupingObject: TimeGranularityGrouper,featureExtractionMethod:FeatureExtractionMethod.Value): Dataset[MultiDimensionalTimeSeries] = {
    groupingKey match {
      case GroupingKey.Entity => toTimeSeries(resAsCR.groupByKey(cr => cr.entity),groupingObject,featureExtractionMethod)
      case GroupingKey.Property => toTimeSeries(resAsCR.groupByKey(cr => cr.property),groupingObject,featureExtractionMethod)
      case GroupingKey.Value_ => toTimeSeries(resAsCR.groupByKey(cr => cr.value),groupingObject,featureExtractionMethod)
      case _ => throw new AssertionError("unknown grouping key")
    }
  }

  private def toTimeSeries(groupedByKey: KeyValueGroupedDataset[String, ChangeRecord],groupingObject:TimeGranularityGrouper,featureExtractionMethod:FeatureExtractionMethod.Value) = {
    var res = groupedByKey.mapGroups((key, changeRecords) => groupingObject.toSingleDimensionalTimeSeries(key, changeRecords, granularity))
    if(featureExtractionMethod == FeatureExtractionMethod.EntireTimeSeriesLogY){
      res = res.map( mdts => mdts.yAsLog())
    }
    if(timeSeriesFilter!=null) {
      res = res.filter(ts => timeSeriesFilter.filter(ts))
        .filter(ts => ts.name != null)
    }
    println("total number of ts:  " + res.count())
    res
  }

}
