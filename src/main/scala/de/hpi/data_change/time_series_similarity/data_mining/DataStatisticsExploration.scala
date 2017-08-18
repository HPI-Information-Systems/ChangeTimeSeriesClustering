package de.hpi.data_change.time_series_similarity.data_mining

import java.time.LocalDateTime

import breeze.plot._
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

case class DataStatisticsExploration(spark: SparkSession, filePath: String) {

  import spark.implicits._
  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]

  def aggregateResults(prop: String, entries: Iterator[ChangeRecord]): (String,Int,Int) = {
    (prop,entries.size,entries.map(cr => cr.entity).toSet.size)
  }

  def propertyStatistics(changeRecords: Dataset[ChangeRecord],resultDir:String) = {
    //TODO: weird things get plotted
    val results = changeRecords.groupByKey(cr => cr.property)
        .mapGroups((prop,entries) => aggregateResults(prop,entries))
    val histogramByNumChanges = results.groupByKey{case (prop,i1,i2) => i1}.mapGroups((i,iterator) => (i,iterator.size))
    val resultsNumChanges = histogramByNumChanges.collect()
    val fig1 = Figure()
    val plt1 = fig1.subplot(0)
    plt1 += plot(resultsNumChanges.map(t => t._1), resultsNumChanges.map(t => t._2),name = "#Change distribution (grouped by property)")
    fig1.refresh()
    val histogramByNumDistinctEntities = results.groupByKey{case (prop,i1,i2) => i2}.mapGroups((i,iterator) => iterator.size)
    val resultsNumDistinctEntities = histogramByNumChanges.collect()
    val fig2 = Figure()
    val plt2 = fig2.subplot(0)
    plt2 += plot(resultsNumDistinctEntities.map(t => t._1), resultsNumDistinctEntities.map(t => t._2),name = "Distribution of the number of properties in an entity")
    fig2.refresh()
  }

  def explore(resultDir:String) = {
    val rawData = spark.read.csv(filePath)
    val changeRecords = rawData.map( new ChangeRecord(_))
    entityStatistics(changeRecords,resultDir)
    propertyStatistics(changeRecords,resultDir)

//    for(granularity <- TimeGranularity.values;groupingKey <- GroupingKey.values) {
//      val aggregator = new TimeSeriesAggregator(spark, 0, granularity, groupingKey)
//      val timeSeriesDataset: Dataset[MultiDimensionalTimeSeries] = aggregator.aggregateToTimeSeries(filePath)
//      timeSeriesDataset
//    }
  }

  private def entityStatistics(changeRecords: Dataset[ChangeRecord],resultDir:String) = {
    val changeCount = changeRecords.groupByKey(cr => cr.entity).mapGroups((e, crIterator) => (e, crIterator.size))
    val histogram = changeCount.groupByKey { case (a, b) => b }
      .mapGroups { case (count, iterator) => (count, iterator.size) }
    //TODO: save results
    histogram.write.csv(resultDir + "entity_numChange_Histogram")
  }

  private def plottingExploration(histogram:Dataset[(Int, Int)]): Unit ={
    //plot that thing:
    val results = histogram.collect().sortBy(t => t._1)
    results.foreach(println(_))
    val fig = Figure()
    val plt = fig.subplot(0)
    plt += plot(results.map(t => t._1), results.map(t => t._2),name = "#Change distribution (grouped by entity)")
    fig.refresh()
  }
}
