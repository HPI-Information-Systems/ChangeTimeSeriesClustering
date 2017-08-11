package main

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql._

import scala.collection.Map

object Main extends App{

  //constants for execution:
  val minNumNonZeroYValues = 3
  val granularity = TimeGranularity.Monthly
  val groupingKey = GroupingKey.Entity

  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  if(args.length==1 && args(0) == "local" ){
    sparkBuilder = sparkBuilder.master("local[1]")
  }
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._
  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]

  executeCode()

  def aggregateToTimeSeries(resAsCR: Dataset[ChangeRecord], groupingObject: TimeGranularityGrouping): Dataset[MultiDimensionalTimeSeries] = {
    groupingKey match {
      case GroupingKey.Entity => toTimeSeries(resAsCR.groupByKey(cr => cr.entity),groupingObject)
      case GroupingKey.Property => toTimeSeries(resAsCR.groupByKey(cr => cr.property),groupingObject)
      case GroupingKey.Value_ => toTimeSeries(resAsCR.groupByKey(cr => cr.value),groupingObject)
      case _ => throw new AssertionError("unknown grouping key")
    }
  }

  def executeCode() = {
    val rawData = spark.read.csv("C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\wikidata\\settlements\\small.csv")
    val byEntity = rawData.groupByKey(r => r.getString(0))
    val resAsCR = rawData.map( new ChangeRecord(_))
    val distinctYears = resAsCR.map(cr => cr.timestamp.getYear).distinct().collect().toList //TODO: if we aggregate daily, we will maybe get some zeros padded to all timeseries
    val groupingObject = new TimeGranularityGrouping(distinctYears.min,distinctYears.max)
    //accumulation to multidimensional time series:
    println("num Distinct years: " + distinctYears.size)
    val timeSeriesDataset: Dataset[MultiDimensionalTimeSeries] = aggregateToTimeSeries(resAsCR,groupingObject)
    //create cartesian product:
    println("total elements to process: " +timeSeriesDataset.count())
    var joinResult = getUnorderedPairs(timeSeriesDataset)
    println("num cols before Dist: " +joinResult.columns.length)
    val distances = joinResult.map(t => (t._1,t._2,t._1.manhattenDistance(t._2))) //t._1.manhattenDistance(t._2)
    println("num cols: " +distances.columns.length)


    //This is actually the real program:
    val sorted = distances.sort(distances.col(distances.columns(2)))
    sorted.head(100).foreach { case ( e1,e2,dist) => println("Distance between " + e1.name + " and " + e2.name + " is " + dist)}

  }

  private def toTimeSeries(groupedByKey: KeyValueGroupedDataset[String, ChangeRecord],groupingObject:TimeGranularityGrouping) = {
    groupedByKey.mapGroups((entity, changeRecords) => groupingObject.toSingleDimensionalTimeSeries(entity, changeRecords, granularity))
      .filter(ts => ts.numNonZeroYValues >= minNumNonZeroYValues)
  }

  def toTimeSeriesTuple(r: Row): (MultiDimensionalTimeSeries,MultiDimensionalTimeSeries) = {
    assert(r.length==6)
    val first= new MultiDimensionalTimeSeries(r.getAs[String](0), r.getAs[Map[String, Seq[Double]]](1), r.getAs[Seq[Timestamp]](2))
    val second = new MultiDimensionalTimeSeries(r.getAs[String](3), r.getAs[Map[String, Seq[Double]]](4), r.getAs[Seq[Timestamp]](5))
    (first,second)
  }

  private def getUnorderedPairs(timeSeriesDataset: Dataset[MultiDimensionalTimeSeries]) = {
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    println("\"spark.sql.crossJoin.enabled\": " + spark.conf.get("spark.sql.crossJoin.enabled"))
    var toJoinWith = timeSeriesDataset.map(ts => ts)
    val colname = timeSeriesDataset.columns(0)
    val trivialCondition = timeSeriesDataset.col(colname).equalTo(timeSeriesDataset.col(colname))
    var joinResult = timeSeriesDataset.toDF().join(timeSeriesDataset)
        .map(r => toTimeSeriesTuple(r))
        .as[(MultiDimensionalTimeSeries,MultiDimensionalTimeSeries)]
        .filter((t) => t._1.name < t._2.name)
    //var joinResult = timeSeriesDataset.joinWith(toJoinWith, trivialCondition, "cross").filter((t) => t._1.name < t._2.name)
    joinResult
  }
}
