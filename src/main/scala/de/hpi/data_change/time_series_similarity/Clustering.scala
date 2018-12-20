package de.hpi.data_change.time_series_similarity

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.Properties

import de.hpi.data_change.time_series_similarity.JsonMain.spark
import de.hpi.data_change.time_series_similarity.data.TimeSeries
import de.hpi.data_change.time_series_similarity.dba.DBAKMeans
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Framework class for clustering changes
  * @param spark
  */
class Clustering(spark:SparkSession,config:Config) extends Serializable {

  val dbAccess = DBAccess(spark,config)

  //------------------------------------------- Begin Dataset Specific Parameters --------------------------------------------------------
  var start:java.sql.Timestamp = null
  var end:java.sql.Timestamp = null
  //------------------------------------------- End Dataset Specific Parameters ----------------------------------------------------------

  //local parameters:
  implicit def enc1: Encoder[(Seq[String],List[LocalDateTime])] = org.apache.spark.sql.Encoders.kryo[(Seq[String],List[LocalDateTime])]
//
  implicit def enc2: Encoder[(Seq[String],LocalDateTime)] = org.apache.spark.sql.Encoders.kryo[(Seq[String],LocalDateTime)]
  implicit def localDateTimeOrdering: Ordering[LocalDateTime] = new Ordering[LocalDateTime] {
    def compare(x: LocalDateTime, y: LocalDateTime): Int = x compareTo y
  }
  implicit def timestampOrdering: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }
  import spark.implicits._

  def toTimeSeries(timestamps: List[LocalDateTime],
                   aggregationGranularity: Int,
                   timeUnit:String,
                   earliestTimestamp: Timestamp,
                   latestTimestamp: Timestamp) = {
    val start = earliestTimestamp.toLocalDateTime
    val end = latestTimestamp.toLocalDateTime
    //todo: make this more efficient:
    var curTime = start;
    val yValues = ListBuffer[Double]()
    var timestampsSorted = timestamps.sorted
    while(curTime.compareTo(end) <0){
      val intervalEnd = curTime.plus(aggregationGranularity,ChronoUnit.valueOf(timeUnit))
      val eventsToAdd = timestampsSorted.filter( ts => ts.compareTo(intervalEnd) <0)
      timestampsSorted = timestampsSorted.filter( ts => ts.compareTo(intervalEnd) >= 0)
      yValues += eventsToAdd.size
      curTime = intervalEnd;
    }
    assert(timestampsSorted.isEmpty)
    yValues
  }

  def transformArbitraryDatasetToGroup(dataset: DataFrame) = {
    dataset.map(r =>{
      val keys = r.toSeq.slice(0,r.size-1).map( e => e.toString)
      (keys,r.getAs[java.sql.Timestamp](r.size-1).toLocalDateTime)
    }).groupByKey{case (key,_) => key}
      .mapGroups{case (key,it) => (key.asInstanceOf[Seq[String]],it.toList.map{case (key,ts) => ts})}
  }

  def clustering() = {
    var df = dbAccess.getInputDataframe
    val originalSchema = df.columns
    println(df.count())
    var filteredGroups = transformArbitraryDatasetToGroup(df)
    println(filteredGroups.count())
    //create time series:
    if(end == null || start == null){
      val startEnd = filteredGroups
          .map( t => (Timestamp.valueOf(t._2.min),Timestamp.valueOf(t._2.max)))
          .reduce( (t1,t2) => (List(t1._1,t2._1).min,List(t1._2,t2._2).max))
      start = startEnd._1
      end = startEnd._2
    }
    println("Successfully Calculated min/max Timestamp")
    val timeUnit = config.unit
    val aggregationGranularity = config.granularity
    var timeSeriesDataset = filteredGroups.map { case (id, list) => {
      TimeSeries(id, toTimeSeries(list, aggregationGranularity,timeUnit, start, end), aggregationGranularity, timeUnit, end)
    }}
    timeSeriesDataset = timeSeriesDataset.filter( ts => ts !=null)
    //transform time series:
    val transformation = config.transformation
    timeSeriesDataset = timeSeriesDataset.map( ts => ts.transform(transformation))
    //filter transformed time series:
    //timeSeriesDataset = timeSeriesDataset.filter(ts => ts.filter())
    println("Number of filtered Time Series: " + timeSeriesDataset.count())
    //extract features:
    val rdd = timeSeriesDataset.rdd.map(ts => {assert(ts.id !=null);RowFactory.create(ts.id.toArray,Vectors.dense(ts.featureExtraction("raw")))})
    rdd.cache()
    val fields = Array(DataTypes.createStructField("name",DataTypes.createArrayType(DataTypes.StringType),false),DataTypes.createStructField("features",VectorType,false)) //TODO: how to save this as row/list
    val schema = new StructType(fields)
    val finalDf = spark.createDataFrame(rdd,schema)
    println("Final DF Size: " + finalDf.count())
    //Clustering:
    var resultDF:Dataset[Row] = null
    var centers:Seq[Array[Double]] = null

    val clusteringAlg = config.clusteringAlgorithm.name
    val numClusters = config.clusteringAlgorithm.k
    val numIterations = config.clusteringAlgorithm.maxIter
    val seed = config.clusteringAlgorithm.seed

    if(clusteringAlg == "KMeans") {
      val clusteringAlg = new KMeans()
        .setFeaturesCol("features")
        .setK(numClusters)
        .setMaxIter(numIterations)
        .setSeed(seed)
        .setPredictionCol("assignedCluster")
      val kmeansModel = clusteringAlg.fit(finalDf)
      resultDF = kmeansModel.transform(finalDf)
      //kmeansModel.save(resultDirectory + configIdentifier + "/model")
      centers = kmeansModel.clusterCenters.map( v => v.toArray)
    } else if(clusteringAlg == "DBAKMeans"){
      val (newCenters,newResultDF) = new DBAKMeans(numClusters,numIterations,seed,spark).fit(finalDf)
      resultDF = newResultDF
      centers = newCenters
    } else {
      throw new AssertionError("unknown clustering algorithm")
    }
    println("Size of result df: " + resultDF.count())
    dbAccess.writeToDB(resultDF, centers,originalSchema)
    spark.stop()
  }

}
