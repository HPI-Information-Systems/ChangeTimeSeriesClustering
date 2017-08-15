package de.hpi.data_change.time_series_similarity.data_mining

import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data.{ChangeRecord, MultiDimensionalTimeSeries, TimeGranularityGrouper}
import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset, SparkSession}

case class TimeSeriesAggregator(spark:SparkSession,minNumNonZeroYValues: Int, granularity: TimeGranularity.Value, groupingKey: GroupingKey.Value) {

  import spark.implicits._
  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]

  def aggregateToTimeSeries(filePath:String): Dataset[MultiDimensionalTimeSeries] = {
    val rawData = spark.read.csv(filePath)
    val changeRecords = rawData.map( new ChangeRecord(_))
    val distinctYears = changeRecords.map(cr => cr.timestamp.getYear).distinct().collect().toList //TODO: if we aggregate daily, we will maybe get some zeros padded to all timeseries
    val groupingObject = new TimeGranularityGrouper(distinctYears.min,distinctYears.max)
    //accumulation to multidimensional time series:
    println("num Distinct years: " + distinctYears.size)
    aggregateToTimeSeries(changeRecords,groupingObject)

  }

  def aggregateToTimeSeries(resAsCR: Dataset[ChangeRecord], groupingObject: TimeGranularityGrouper): Dataset[MultiDimensionalTimeSeries] = {
    groupingKey match {
      case GroupingKey.Entity => toTimeSeries(resAsCR.groupByKey(cr => cr.entity),groupingObject)
      case GroupingKey.Property => toTimeSeries(resAsCR.groupByKey(cr => cr.property),groupingObject)
      case GroupingKey.Value_ => toTimeSeries(resAsCR.groupByKey(cr => cr.value),groupingObject)
      case _ => throw new AssertionError("unknown grouping key")
    }
  }

  private def toTimeSeries(groupedByKey: KeyValueGroupedDataset[String, ChangeRecord],groupingObject:TimeGranularityGrouper) = {
    groupedByKey.mapGroups((key, changeRecords) => groupingObject.toSingleDimensionalTimeSeries(key, changeRecords, granularity))
      .filter(ts => ts.numNonZeroYValues >= minNumNonZeroYValues)
  }

}
