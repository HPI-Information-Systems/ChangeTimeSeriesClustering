package de.hpi.data_change.time_series_similarity

import java.io.File
import java.sql.{Time, Timestamp}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit

import de.hpi.data_change.time_series_similarity.ClusteringMain.{isLocalMode, spark, sparkBuilder}
import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity, TimeSeriesFilter}
import de.hpi.data_change.time_series_similarity.data.{ChangeRecord, TimeSeries, TimeSeriesNew}
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import de.hpi.data_change.time_series_similarity.io.{DataIO, ResultSerializer}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Dataset, Encoder, RowFactory, SparkSession}

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer

object LocalExplorationMain extends App with Serializable{

  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    sparkBuilder = sparkBuilder.master("local[2]")
  val spark = sparkBuilder.getOrCreate()

  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def changeRecordListEncoder: Encoder[List[ChangeRecord]] = org.apache.spark.sql.Encoders.kryo[List[ChangeRecord]]
  implicit def changeRecordTupleListEncoder: Encoder[(String,List[ChangeRecord])] = org.apache.spark.sql.Encoders.kryo[(String,List[ChangeRecord])]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]
  implicit def ordered: Ordering[LocalDateTime] = new Ordering[LocalDateTime] {
    def compare(x: LocalDateTime, y: LocalDateTime): Int = x compareTo y
  }

  //implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(_.toEpochSecond(ZoneOffset.UTC))
  import spark.implicits._
  //wikidata()

  val minNonZeryYValueCount = 1
  val minGroupSize = 50
  val temporalIntervalInDays = 7
  val numClusters = 10
  val numIterations = 100
  val resultDirectory = "/users/leon.bornemann/results/"
  val filePath = args(0)
  val configIdentifier = "imdb_first_test"

//  val resultDirectory = "/home/leon/Documents/researchProjects/imdb/localResults"
//  val filePath = "/home/leon/Documents/researchProjects/imdb/localTest/"
//  val configIdentifier = "imdb_second_test"
  val start:java.sql.Timestamp = java.sql.Timestamp.valueOf("2014-02-21 00:00:00") //2014-02-21_Movies_changes
  val end:java.sql.Timestamp = java.sql.Timestamp.valueOf("2017-07-15 00:00:00") //2017-07-14

  imdb()

  def toTimeSeries(changeRecords: List[ChangeRecord], aggregationGranularityInDays: Int, earliestTimestamp: Timestamp, latestTimestamp: Timestamp) = {
    //174 "City Guide" (2011) {Fun for Your Brain (#3.2)}
    //177 "Circles" (2008) {Tyler (#1.6)}
    val start = earliestTimestamp.toLocalDateTime
    val end = latestTimestamp.toLocalDateTime
    //todo: make this more efficient:
    //val map:scala.collection.mutable.Map[LocalDateTime,Integer] = new TreeMap[LocalDateTime,Integer]()
    var curTime = start;
    //while(curTime < end){

    //}
    val yValues = ListBuffer[Double]()
    var allCrs = changeRecords.toList.sortBy(cr => cr.timestamp)
    while(curTime.compareTo(end) <0){
      val intervalEnd = curTime.plusDays(aggregationGranularityInDays)
      val eventsToAdd = allCrs.filter( cr => cr.timestamp.compareTo(intervalEnd) <0)
      allCrs = allCrs.filter( cr => cr.timestamp.compareTo(intervalEnd) >= 0)
      yValues += eventsToAdd.size
      curTime = intervalEnd;
    }
    assert(allCrs.isEmpty)
    yValues

//    val diffInDays = ChronoUnit.DAYS.between(start,allCrs.head.timestamp)
//    val numZerosToPadFront = diffInDays.toInt / aggregationGranularityInDays
//    (0 until numZerosToPadFront).foreach(_ => yValues +=0)
//    var intervalStart = start.plusDays(diffInDays*numZerosToPadFront)
//    while(!allCrs.isEmpty){
//      val intervalEnd = intervalStart.plusDays(aggregationGranularityInDays)
//      val eventsToAdd = allCrs.filter( cr => cr.timestamp.compareTo(intervalEnd) <0)
//      yValues += eventsToAdd.size
//      allCrs = allCrs.filter( cr => cr.timestamp.compareTo(intervalEnd) >= 0)
//      intervalStart = intervalEnd;
//    }
//    val numZerosToPadBack = ChronoUnit.DAYS.between(intervalStart,end).toInt / aggregationGranularityInDays
//    (0 until numZerosToPadBack).foreach(_ => yValues +=0)
//    yValues
  }

  def imdb() = {
    var dataset = getChangeRecordDataSet(filePath)
    //grouping phase:
    var groupedDataset = dataset.groupByKey(cr => cr.entity + "|" +cr.property )
    //TODO: first filtering phase:
    var filteredGroups = groupedDataset.mapGroups{case (id,crIterator) => (id,crIterator.toList) }
    filteredGroups = filteredGroups.filter( g => g._2.size > minGroupSize)// .filter{case (id,list) => list.size >= minGroupSize}
    //groupedDataset
    //create time series:
    //var timeSeriesDataset = groupedDataset.mapGroups{case (id,it) => TimeSeriesNew(id,toTimeSeries(it.toList,temporalIntervalInDays,start,end),temporalIntervalInDays,start,end)}
    var timeSeriesDataset = filteredGroups.map{case (id,list) => TimeSeriesNew(id,toTimeSeries(list,temporalIntervalInDays,start,end),temporalIntervalInDays,start,end)}
    timeSeriesDataset = timeSeriesDataset.filter( ts => ts !=null)
    //transform time series:
    timeSeriesDataset = timeSeriesDataset.map( ts => ts.transform())
    //filter transformed time series:
    timeSeriesDataset = timeSeriesDataset.filter(ts => ts.filterByMinNumNonZeroValues(minNonZeryYValueCount))
    //extract features:
    val rdd = timeSeriesDataset.rdd.map(ts => {assert(ts.id !=null);RowFactory.create(ts.id,Vectors.dense(ts.toFeatures()))})
    val fields = Array(DataTypes.createStructField("name",DataTypes.StringType,false),DataTypes.createStructField("features",VectorType,false))
    val schema = new StructType(fields)
    val finalDf = spark.createDataFrame(rdd,schema)
    //Clustering:
    val clusteringAlg = new KMeans()
      .setFeaturesCol("features")
      .setK(numClusters)
      .setMaxIter(numIterations)
      .setPredictionCol("assignedCluster")
    val kmeansModel = clusteringAlg.fit(finalDf)
    val resultDF = kmeansModel.transform(finalDf)
    println("Cost is: " + kmeansModel.computeCost(finalDf))
    println("Starting to save results")
    resultDF.write.json(resultDirectory + configIdentifier + "/result")
    kmeansModel.save(resultDirectory + configIdentifier + "/model")




//    println(dataset.filter( cr => cr.property == "title").count());
//    println("num entities: " + dataset.map(cr => cr.entity).distinct().count())
//    println("num properties: " + dataset.map(cr => cr.property).distinct().count())
//    println("num values: " + dataset.map(cr => cr.value).distinct().count())
//    println("num change Records: " + dataset.count())
//    println("change per entity distribution:")
//    dataset.groupByKey(cr => cr.entity).mapGroups{ case (s,it) => (s,it.size)}
//        .groupByKey(t => t._2).mapGroups{case (count,it) => (count,it.size)}
//        .collect().sortBy( t => t._1).foreach(t => println(t._1 + ","+t._2+","))
//    println("change per property distribution:")
//    dataset.groupByKey(cr => cr.property).mapGroups{ case (s,it) => (s,it.size)}
//      .groupByKey(t => t._2).mapGroups{case (count,it) => (count,it.size)}
//      .collect().sortBy( t => t._1).foreach(t => println(t._1 + ","+t._2+","))
//    println("change per entity-property distribution:")
//    dataset.groupByKey(cr => cr.entity + "|" + cr.property).mapGroups{ case (s,it) => (s,it.size)}
//        .groupByKey(t => t._2).mapGroups{case (count,it) => (count,it.size)}
//        .collect().sortBy( t => t._1).foreach(t => println(t._1 + ","+t._2+","))
    //println(dataset.filter( cr => cr.property == "title").count());
    //spark sort: dataset.sort($"col1".desc)
    //dataset.take(100).foreach(println(_))
  }

  def getChangeRecordDataSet(filePath:String): Dataset[ChangeRecord] ={
    val rawData = spark.read.option("mode", "DROPMALFORMED").csv(filePath)
    rawData.filter(r => r.getString(3) !=null && r.size == 4).map(r =>  {
      if(r.size != 4){
        println("huh")
        assert(false) //todo remove malformatted
      }
      new ChangeRecord(r)
    }
    )
  }


  private def wikidata() =  {
    val filePath = DataIO.getFullWikidataSparkCompatibleFile();
    val aggregator = new TimeSeriesAggregator(spark, null, TimeGranularity.Monthly, GroupingKey.Entity_Property);
    //TimeSeriesAggregator(spark:SparkSession,timeSeriesFilter:TimeSeriesFilter, granularity: TimeGranularity.Value, groupingKey: GroupingKey.Value)
    val dataset = aggregator.getChangeRecordDataSet(filePath.getAbsolutePath);
    dataset.filter(cr => cr.entity == "Berlin").collect().foreach(println(_))
    var props = Set("lat", "long", "Mayor", "Country", "State", "Country", "Counties", "Founded by", "Settled", "Area", "Population", "Density", "City", "Estimate")
    //
    //  dataset.filter(cr => props.contains(cr.property))
  }

}
