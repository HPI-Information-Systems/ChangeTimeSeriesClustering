package de.hpi.data_change.time_series_similarity.data_mining

import java.sql.Timestamp
import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data.{ChangeRecord, MultiDimensionalTimeSeries}
import org.apache.spark.sql._

import scala.collection.Map

case class PairwiseSimilarityExtractor(minNumNonZeroYValues: Int, granularity: TimeGranularity.Value, groupingKey: GroupingKey.Value, spark: SparkSession, filePath: String) {

  import spark.implicits._
  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]

  def calculatePairwiseSimilarity(): Dataset[(MultiDimensionalTimeSeries,MultiDimensionalTimeSeries,Double)] = {
    val aggregator = new TimeSeriesAggregator(spark,minNumNonZeroYValues,granularity,groupingKey)
    val timeSeriesDataset: Dataset[MultiDimensionalTimeSeries] = aggregator.aggregateToTimeSeries(filePath)
    //create cartesian product:
    println("total elements to process: " +timeSeriesDataset.count())
    val joinResult = getUnorderedPairs(timeSeriesDataset)
    println("num cols before Dist: " +joinResult.columns.length)
    val distances = joinResult.map(t => (t._1,t._2,t._1.manhattenDistance(t._2))) //t._1.manhattenDistance(t._2)
    println("num cols: " +distances.columns.length)
    val sorted = distances.sort(distances.col(distances.columns(2)))
    sorted.head(100).foreach { case ( e1,e2,dist) => println("Distance between " + e1.name + " and " + e2.name + " is " + dist)}
    sorted
  }

  def toTimeSeriesTuple(r: Row): (MultiDimensionalTimeSeries,MultiDimensionalTimeSeries) = {
    assert(r.length==6)
    val first= MultiDimensionalTimeSeries(r.getAs[String](0), r.getAs[Map[String, Seq[Double]]](1), r.getAs[Seq[Timestamp]](2))
    val second = MultiDimensionalTimeSeries(r.getAs[String](3), r.getAs[Map[String, Seq[Double]]](4), r.getAs[Seq[Timestamp]](5))
    (first,second)
  }

  private def getUnorderedPairs(timeSeriesDataset: Dataset[MultiDimensionalTimeSeries]) = {
    spark.conf.set("spark.sql.crossJoin.enabled", value = true)
    println("\"spark.sql.crossJoin.enabled\": " + spark.conf.get("spark.sql.crossJoin.enabled"))
    val joinResult = timeSeriesDataset.toDF().join(timeSeriesDataset)
      .map(r => toTimeSeriesTuple(r))
      .as[(MultiDimensionalTimeSeries, MultiDimensionalTimeSeries)]
      .filter((t) => t._1.name < t._2.name)
    //var joinResult = timeSeriesDataset.joinWith(toJoinWith, trivialCondition, "cross").filter((t) => t._1.name < t._2.name)
    joinResult
  }

}