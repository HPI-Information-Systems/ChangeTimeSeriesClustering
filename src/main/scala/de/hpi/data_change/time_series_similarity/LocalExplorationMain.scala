package de.hpi.data_change.time_series_similarity

import java.io.File
import java.sql.{DriverManager, Time, Timestamp}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.Properties

import de.hpi.data_change.time_series_similarity.ClusteringMain.{isLocalMode, spark, sparkBuilder}
import de.hpi.data_change.time_series_similarity.configuration.{GroupingKey, TimeGranularity, TimeSeriesFilter}
import de.hpi.data_change.time_series_similarity.data.{ChangeRecord, TimeSeries, TimeSeriesNew}
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesAggregator
import de.hpi.data_change.time_series_similarity.io.{DataIO, ResultSerializer}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql._

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer

class LocalExplorationMain(filePath:String,configIdentifier:String,spark:SparkSession) extends Serializable{

  def main(args: Array[String]): Unit = {
    val isLocal = args.length==3 && args(2) == "-local"
    var sparkBuilder = SparkSession
      .builder()
      .appName("Clustering")
    if(isLocal) {
      sparkBuilder = sparkBuilder.master("local[2]")
      useDB = true
    }
    val spark = sparkBuilder.getOrCreate()
    val exploration = new LocalExplorationMain(args(0),args(2),spark)
    exploration.imdb()
  }

  //constants:
  val KeySeparator = "||||"

  var useDB = false

  implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def changeRecordListEncoder: Encoder[List[ChangeRecord]] = org.apache.spark.sql.Encoders.kryo[List[ChangeRecord]]
  implicit def changeRecordTupleListEncoder: Encoder[(String,List[ChangeRecord])] = org.apache.spark.sql.Encoders.kryo[(String,List[ChangeRecord])]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]
  implicit def localDateTimeTupleEncoder: Encoder[(String,LocalDateTime)] = org.apache.spark.sql.Encoders.kryo[(String,LocalDateTime)]
  implicit def localDateTimeListTupleEncoder: Encoder[(String,List[LocalDateTime])] = org.apache.spark.sql.Encoders.kryo[(String,List[LocalDateTime])]
  implicit def ordered: Ordering[LocalDateTime] = new Ordering[LocalDateTime] {
    def compare(x: LocalDateTime, y: LocalDateTime): Int = x compareTo y
  }

  //implicit val localDateTimeOrdering: Ordering[LocalDateTime] = Ordering.by(_.toEpochSecond(ZoneOffset.UTC))
  import spark.implicits._
  //wikidata()
  val usePrefilteredQuery = false
  val minNonZeryYValueCount = 1
  val minGroupSize = 0
  val temporalIntervalInDays = 7
  val numClusters = 10
  val numIterations = 100
  val resultDirectory = "/users/leon.bornemann/results/"

//  val resultDirectory = "/home/leon/Documents/researchProjects/imdb/localResults"
//  val filePath = "/home/leon/Documents/researchProjects/imdb/localTest/"
//  val configIdentifier = "imdb_second_test"
  val start:java.sql.Timestamp = java.sql.Timestamp.valueOf("2014-02-21 00:00:00") //2014-02-21_Movies_changes
  val end:java.sql.Timestamp = java.sql.Timestamp.valueOf("2017-07-15 00:00:00") //2017-07-14

  //input query:
  val querystring = "select * from imdbchanges limit 1000"
  //database config
  val databaseURL = "jdbc:postgresql://localhost/changedb"
  val user = "dummy"
  val password = "dummy"
  val driver = "org.postgresql.Driver"

  def getArbitraryQueryResult(url:String,query:String) = {
    spark.sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("useUnicode", "true").
      //option("continueBatchOnError","true").
      option("useSSL", "false").
      option("user", user).
      option("password", password).
      option("dbtable","((" + query + ")) as queryresult").
      load()
  }

  def getChangeRecordSetFromDB() = {
    val df = getArbitraryQueryResult(databaseURL,querystring)
    getChangeRecordDataSet(df)
  }

  def toTimeSeries(timestamps: List[LocalDateTime], aggregationGranularityInDays: Int, earliestTimestamp: Timestamp, latestTimestamp: Timestamp) = {
    val start = earliestTimestamp.toLocalDateTime
    val end = latestTimestamp.toLocalDateTime
    //todo: make this more efficient:
    var curTime = start;
    val yValues = ListBuffer[Double]()
    var timestampsSorted = timestamps.sorted
    while(curTime.compareTo(end) <0){
      val intervalEnd = curTime.plusDays(aggregationGranularityInDays)
      val eventsToAdd = timestampsSorted.filter( ts => ts.compareTo(intervalEnd) <0)
      timestampsSorted = timestampsSorted.filter( ts => ts.compareTo(intervalEnd) >= 0)
      yValues += eventsToAdd.size
      curTime = intervalEnd;
    }
    assert(timestampsSorted.isEmpty)
    yValues
  }

  def getFilteredGroups(dataset: Dataset[ChangeRecord]) = {
    //var dataset = input.filter(cr => cr.property == "Rating.Votes" && cr.entity.contains("\"\"Doctor Who\"\" (1963)"))
    var groupedDataset = dataset.groupByKey(cr => (cr.entity + "|" + cr.property) )
    //TODO: first filtering phase:
    var filteredGroups = groupedDataset.mapGroups{case (id,crIterator) => (id,crIterator.toList) }
    filteredGroups = filteredGroups.filter( g => g._2.size > minGroupSize)// .filter{case (id,list) => list.size >= minGroupSize}
    filteredGroups.map{case (id,list) => {
      val entity = list.head.entity
      val timestamps = list.map(cr => cr.timestamp)
      (entity,timestamps)
    }}
  }

  def transformArbitraryDatasetToGroup(dataset: DataFrame) = {
    dataset.map(r =>{
      val keys = r.toSeq.slice(0,r.size-1).map( e => e.toString)
      (keys.mkString(KeySeparator),r.getAs[java.sql.Timestamp](r.size-1).toLocalDateTime)
    }).groupByKey{case (key,_) => key}
      .mapGroups{case (key,it) => (key,it.toList.map{case (key,ts) => ts})}
  }

  def individualFilter(dataset: Dataset[ChangeRecord]): Dataset[ChangeRecord] = {
    val keys = Set("","")
    dataset.filter( cr => {
      cr.property == "Rating.Votes" &&
      keys.exists(cr.entity.contains(_)) &&
      cr.entity.contains("{")
    })
  }

  def imdb() = {
    var filteredGroups:Dataset[(String,List[LocalDateTime])] = null
    if(useDB){
      if(usePrefilteredQuery){
        var dataset = getArbitraryQueryResult(databaseURL,querystring)
        filteredGroups = transformArbitraryDatasetToGroup(dataset)
      } else {
        var dataset = getChangeRecordSetFromDB()
        filteredGroups = getFilteredGroups(dataset)
      }
    } else{
      var dataset = getChangeRecordDataSet(filePath)
      //individual filter
      dataset = individualFilter(dataset)
      filteredGroups = getFilteredGroups(dataset)
    }
    //create time series:
    var timeSeriesDataset = filteredGroups.map{case (id,list) => TimeSeriesNew(id,toTimeSeries(list,temporalIntervalInDays,start,end),temporalIntervalInDays,start,end)}
    timeSeriesDataset = timeSeriesDataset.filter( ts => ts !=null)
    //transform time series:
    timeSeriesDataset = timeSeriesDataset.map( ts => ts.transform())
    //filter transformed time series:
    timeSeriesDataset = timeSeriesDataset.filter(ts => ts.filterByMinNumNonZeroValues(minNonZeryYValueCount))
    //extract features:
    val rdd = timeSeriesDataset.rdd.map(ts => {assert(ts.id !=null);RowFactory.create(ts.id,Vectors.dense(ts.toFeatures()))})
    rdd.cache()
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
  }

  def getChangeRecordDataSet(rawData: DataFrame) = {
    rawData.map(r => new ChangeRecord(r))
  }


  def getChangeRecordDataSet(filePath:String): Dataset[ChangeRecord] ={
    val rawData = spark.read.option("mode", "DROPMALFORMED").csv(filePath)
    getChangeRecordDataSet(rawData)
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
