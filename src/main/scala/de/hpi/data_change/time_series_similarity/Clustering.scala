package de.hpi.data_change.time_series_similarity

import java.io.{File, PrintWriter}
import java.sql.{DriverManager, Time, Timestamp}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.temporal.ChronoUnit
import java.util.Properties

import de.hpi.data_change.time_series_similarity.data.{ChangeRecord, TimeSeries}
import de.hpi.data_change.time_series_similarity.io.DataIO
import de.hpi.data_change.time_series_similarity.visualization.CSVSerializer
import dmlab.main.{FloatPoint, FunctionSet, MainDriver}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.codehaus.jackson.JsonNode
import org.json4s.JsonAST.JObject
import org.json4s.jackson.Json
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ListBuffer
import scala.util.Random

class Clustering(resultDirectory:String, configIdentifier:String, spark:SparkSession) extends Serializable{

  var filter: Dataset[ChangeRecord] => Dataset[ChangeRecord] = null
  var grouper: ChangeRecord => Seq[String] = null

  def setIndividualFilter(filter: Dataset[ChangeRecord] => Dataset[ChangeRecord]): Unit = this.filter = filter

  def setGrouper(grouper:ChangeRecord => Seq[String]) = this.grouper = grouper


  //------------------------------------------- Begin Parameters settable via json config object -------------------------------------------
  //Parameters are initilaized with default values
  var aggregationGranularity = 7
  var aggregationTimeUnit = "DAYS"
  var transformation:List[String] = List() //empty list: "None"
  var featureExtraction = "raw"
  var clusteringAlg = "KMeans"
  var distanceMeasure = "Euclidean"
  //clustering features settable via config
  var numClusters = 10
  var numIterations = 100
  var seed = new Random().nextLong()
  //------------------------------------------- End Parameters settable via json config object ---------------------------------------------

  //------------------------------------------- Begin Input Options ------------------------------------------------------------------------
  var filePath:String = null
  var querystring:String = null
  //------------------------------------------- End Input Options ------------------------------------------------------------------------

  //------------------------------------------- Begin Database Access parameters -----------------------------------------------------------
  //database config
  var url = "jdbc:postgresql://localhost/changedb"
  var user = "dummy"
  var pw = "dummy"
  var driver = "org.postgresql.Driver"
  //do we use the database or files?
  var useDB = false
  //is the query pre-filtered or do we get all change records from a table?
  var queryIsPrefiltered = false
  //------------------------------------------- End Database Access parameters -----------------------------------------------------------

  //------------------------------------------- Begin Filtering Parameters ---------------------------------------------------------------
  var minGroupSize = 0
  //------------------------------------------- End Filtering Parameters -----------------------------------------------------------------

  //------------------------------------------- Begin Dataset Sepcific Parameters --------------------------------------------------------
  var start:java.sql.Timestamp = java.sql.Timestamp.valueOf("2014-02-21 00:00:00") //2014-02-21_Movies_changes
  var end:java.sql.Timestamp = java.sql.Timestamp.valueOf("2017-07-15 00:00:00") //2017-07-14
  //------------------------------------------- End Dataset Specific Parameters ----------------------------------------------------------

  def medoidAssigner(medoidList: List[FloatPoint]) = udf(
    (r: Row) => medoidList.map( medoid => FunctionSet.distance(toFloatPoint(r),medoid)).zipWithIndex.min._2
  )

  def setFileAsDataSource(filePath:String): Unit = {
    this.filePath = filePath
    useDB = false
  }

  def setDBQueryAsDataSource(query:String,queryIsPrefiltered:Boolean): Unit ={
    this.querystring = query
    this.queryIsPrefiltered = queryIsPrefiltered
    useDB = true
  }

  def setDatabaseAccess(url:String,user:String,pw:String,driver:String) = {
    this.url =url
    this.user = user
    this.pw = pw
    this.driver = driver
  }

  def setTimeBorders(start:Timestamp,end:Timestamp): Unit ={
    this.start = start
    this.end = end
  }

  def setParams(config:JsonNode) = {
    aggregationGranularity = config.get("granularity").getIntValue
    aggregationTimeUnit =config.get("unit").getTextValue
    assert ( ChronoUnit.values().exists(s => s.toString == aggregationTimeUnit))
    val transformationString = (config.get("transformation").getTextValue) //TODO: expect a list here
    transformation = List(transformationString)
    featureExtraction = config.get("featureExtraction").getTextValue
    clusteringAlg = config.get("clusteringAlg").getTextValue
    if(clusteringAlg == "KMeans" || clusteringAlg == "BisectingKMeans"){
      val subElementName = clusteringAlg
      numClusters = config.get(subElementName + " Parameters").get("k").getIntValue
      numIterations = config.get(subElementName + " Parameters").get("maxIter").getIntValue
      seed = config.get(subElementName + " Parameters").get("seed").getIntValue
    } else if(clusteringAlg == "PAMAE"){
      val subElementName = clusteringAlg
      numClusters = config.get(subElementName + " Parameters").get("k").getIntValue
      distanceMeasure = "DTW"
    }
  }

  //local parameters:
  //implicit def rowEncoder: Encoder[Row] = org.apache.spark.sql.Encoders.kryo[Row]
  //implicit def changeRecordEncoder: Encoder[ChangeRecord] = org.apache.spark.sql.Encoders.kryo[ChangeRecord]
  implicit def changeRecordListEncoder: Encoder[List[ChangeRecord]] = org.apache.spark.sql.Encoders.kryo[List[ChangeRecord]]
  implicit def changeRecordTupleListEncoder: Encoder[(String,List[ChangeRecord])] = org.apache.spark.sql.Encoders.kryo[(String,List[ChangeRecord])]
  implicit def changeRecordTupleListEncoder_2: Encoder[(Seq[String],List[ChangeRecord])] = org.apache.spark.sql.Encoders.kryo[(Seq[String],List[ChangeRecord])]
  implicit def localDateTimeEncoder: Encoder[LocalDateTime] = org.apache.spark.sql.Encoders.kryo[LocalDateTime]
  implicit def localDateTimeTupleEncoder: Encoder[(String,LocalDateTime)] = org.apache.spark.sql.Encoders.kryo[(String,LocalDateTime)]
  implicit def localDateTimeListTupleEncoder: Encoder[(String,List[LocalDateTime])] = org.apache.spark.sql.Encoders.kryo[(String,List[LocalDateTime])]
  implicit def localDateTimeListTupleEncoderWithStringList: Encoder[(Seq[String],List[LocalDateTime])] = org.apache.spark.sql.Encoders.kryo[(Seq[String],List[LocalDateTime])]
  implicit def localDateTimeListTupleEncoderWithStringListAndAssignedCluster: Encoder[(Seq[String],List[LocalDateTime],Int)] = org.apache.spark.sql.Encoders.kryo[(Seq[String],List[LocalDateTime],Int)]

  implicit def localDateTimeListTupleEncoderWithStringList_2: Encoder[(Seq[String],LocalDateTime)] = org.apache.spark.sql.Encoders.kryo[(Seq[String],LocalDateTime)]
  implicit def ordered: Ordering[LocalDateTime] = new Ordering[LocalDateTime] {
    def compare(x: LocalDateTime, y: LocalDateTime): Int = x compareTo y
  }
  import spark.implicits._

  def writeToDB(resultDF: DataFrame) = {
    //resultDF.map(r => r.getAs[String](id)) TODO: it better to try and group by a list of strings?
//    val fields = resultDF.head.getAs[Seq[String]](0).zipWithIndex.map(a => StructField("key_" + a._2.toString, DataTypes.StringType, nullable = true))
//    val schema = StructType(fields)
    val toWrite = resultDF.map(r => {
      val keyArray = r.getAs[Seq[String]](0)
      val cluster = r.getAs[Int]("assignedCluster")
      (keyArray.mkString(Clustering.KeySeparator),cluster)
      //Row.fromSeq(keyArray)//++cluster.toString
    })
    println("dummy output simulating database write")
    val props = new Properties()
    props.setProperty("user",user)
    props.setProperty("password",pw)
    props.setProperty("driver",driver)
    toWrite.write.jdbc(url,configIdentifier,props)
      /*.format("jdbc") //map(r => RowFactory.create(r.get(0),r.get(2)))
      .option("url",url)
      .option("driver",driver)
      .option("useUnicode","true")
      .option("useSSL","false")
      .option("user",user)
      .option("password",pw)
      .option("table",configIdentifier)
        .saveAsTable(configIdentifier)*/

  }

  def getArbitraryQueryResult(url:String,query:String) = {
    spark.sqlContext.read.format("jdbc").
      option("url", url).
      option("driver", driver).
      option("useUnicode", "true").
      //option("continueBatchOnError","true").
      option("useSSL", "false").
      option("user", user).
      option("password", pw).
      option("dbtable","((" + query + ")) as queryresult").
      load()
  }

  def getChangeRecordSetFromDB() = {
    val df = getArbitraryQueryResult(url,querystring)
    getChangeRecordDataSet(df)
  }

  def toTimeSeries(timestamps: List[LocalDateTime], aggregationGranularity: Int, earliestTimestamp: Timestamp, latestTimestamp: Timestamp) = {
    val start = earliestTimestamp.toLocalDateTime
    val end = latestTimestamp.toLocalDateTime
    //todo: make this more efficient:
    var curTime = start;
    val yValues = ListBuffer[Double]()
    var timestampsSorted = timestamps.sorted
    while(curTime.compareTo(end) <0){
      val intervalEnd = curTime.plus(aggregationGranularity,ChronoUnit.valueOf(aggregationTimeUnit.toUpperCase))
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
    var groupedDataset = dataset.groupByKey(grouper )
    //TODO: first filtering phase:
    var filteredGroups = groupedDataset.mapGroups{case (id,crIterator) => (id,crIterator.toList) }
    filteredGroups = filteredGroups.filter( g => g._2.size > minGroupSize)// .filter{case (id,list) => list.size >= minGroupSize}
    filteredGroups.map{case (id,list) => {
      val entity = list.head.entity
      val timestamps = list.map(cr => cr.timestamp.toLocalDateTime)
      (id.asInstanceOf[Seq[String]],timestamps)
    }}
  }

  def transformArbitraryDatasetToGroup(dataset: DataFrame) = {
    println(dataset == null)
    dataset.map(r =>{
      val keys = r.toSeq.slice(0,r.size-1).map( e => e.toString)
      (keys,r.getAs[java.sql.Timestamp](r.size-1).toLocalDateTime)
    }).groupByKey{case (key,_) => key}
      .mapGroups{case (key,it) => (key.asInstanceOf[Seq[String]],it.toList.map{case (key,ts) => ts})}
  }

  def individualFilter(dataset: Dataset[ChangeRecord]): Dataset[ChangeRecord] = {
    val keys = Set("\"Arrow\" (2012)","\"The Big Bang Theory\" (2007)","\"House of Cards\" (2013)","\"Walker, Texas Ranger\" (1993)","\"Star Trek\" (1966)",
    "\"Lost\" (2004)","\"The Fresh Prince of Bel-Air\" (1990)","\"Friends\" (1994)","\"The Simpsons\" (1989)","\"Doctor Who\" (1963)")
    dataset.filter( cr => {
      cr.property == "Rating.Votes" &&
      keys.exists(cr.entity.contains(_)) &&
      cr.entity.contains("{")
    })
  }

  def noInputSet(): Boolean = filePath == null && querystring == null

  def toFloatPoint(r: Row): FloatPoint = {
    val vec = r.getAs[org.apache.spark.ml.linalg.Vector]("features");
    val fp = new FloatPoint(vec.size,-1)
    fp.setvalues(vec.toArray.map(d => d.toFloat))
    fp
  }

  def transformToJavaRDD(finalDf: DataFrame): _root_.org.apache.spark.api.java.JavaRDD[_root_.dmlab.main.FloatPoint] = {
    val a = finalDf.toJavaRDD.rdd.map(r => toFloatPoint(r)).toJavaRDD()
    a
  }

  def clustering() = {
    if(noInputSet()){
      throw new AssertionError("Either file input or database input must be specified")
    }
    var filteredGroups:Dataset[(Seq[String],List[LocalDateTime])] = null
    if(useDB){
      if(queryIsPrefiltered){
        var dataset = getArbitraryQueryResult(url,querystring)
        filteredGroups = transformArbitraryDatasetToGroup(dataset)
      } else {
        var dataset = getChangeRecordSetFromDB()
        filteredGroups = getFilteredGroups(dataset)
      }
    } else{
      var dataset = getChangeRecordDataSet(filePath)
      //individual filter
      if(filter != null) {
        dataset = individualFilter(dataset)
      }
      filteredGroups = getFilteredGroups(dataset)
    }
    //create time series:
    var timeSeriesDataset = filteredGroups.map { case (id, list) => {
      TimeSeries(id, toTimeSeries(list, aggregationGranularity, start, end), aggregationGranularity, aggregationTimeUnit.toUpperCase, end)
    }}
    timeSeriesDataset = timeSeriesDataset.filter( ts => ts !=null)
    //transform time series:
    timeSeriesDataset = timeSeriesDataset.map( ts => ts.transform(transformation))
    //filter transformed time series:
    timeSeriesDataset = timeSeriesDataset.filter(ts => ts.filter())
    //extract features:
    val rdd = timeSeriesDataset.rdd.map(ts => {assert(ts.id !=null);RowFactory.create(ts.id.toArray,Vectors.dense(ts.featureExtraction(featureExtraction)))})
    rdd.cache()
    val fields = Array(DataTypes.createStructField("name",DataTypes.createArrayType(DataTypes.StringType),false),DataTypes.createStructField("features",VectorType,false)) //TODO: how to save this as row/list
    val schema = new StructType(fields)
    val finalDf = spark.createDataFrame(rdd,schema)
    //Clustering:
    var resultDF:Dataset[Row] = null
    if(clusteringAlg == "KMeans") {
      val clusteringAlg = new KMeans()
        .setFeaturesCol("features")
        .setK(numClusters)
        .setMaxIter(numIterations)
        .setSeed(seed)
        .setPredictionCol("assignedCluster")
      val kmeansModel = clusteringAlg.fit(finalDf)

      /*val clusteringAlg2 = new BisectingKMeans()
      .setFeaturesCol("features")
      .setK(numClusters)
      .setMaxIter(numIterations)
      .setSeed(seed)
      .setPredictionCol("assignedCluster")
    val hierarchicalModel = clusteringAlg2.fit(finalDf)*/
      //hierarchicalModel.

      resultDF = kmeansModel.transform(finalDf)
      println("Cost is: " + kmeansModel.computeCost(finalDf))
      println("Starting to save results")
      kmeansModel.save(resultDirectory + configIdentifier + "/model")
    } else if (clusteringAlg == "PAMAE"){
      val numOfSampledObjects = 100;
      val numOfSamples = 40
      val numOfCores = 2
      val numOfIterations = 1
      val medoids = scala.collection.JavaConversions.asScalaBuffer(
        MainDriver.executePAMAE(transformToJavaRDD(finalDf), numClusters, numOfSampledObjects, 10, numOfCores, numOfIterations, spark.sparkContext)
      ).toList
      resultDF = finalDf.withColumn("assignedCluster",medoidAssigner(medoids)($"features"))
      val filename = "KMedoidCenters.csv"
      val pr = new PrintWriter(new File(resultDirectory + configIdentifier + filename))
      medoids.map(fp => fp.getValues.mkString(",")).foreach(println(_))
    } else if(clusteringAlg == "DBAKMeans"){
      val model = new DBAKMeans(numClusters,numIterations,seed,spark)/*.fit(finalDf)
      resultDF = model.transform(finalDF)
      model.serializeCenters("centers.csv");*/
    } else {
      throw new AssertionError("unknown clustering algorithm")
    }
    if(useDB){
      writeToDB(resultDF)
      //TODO: add column to database with cluster id
      //TODO: create database containing the cluster centers
    }
    resultDF.write.json(resultDirectory + configIdentifier + "/result")
    val csvResultPath = resultDirectory + configIdentifier + "/csvResults/"
    new File(csvResultPath).mkdirs()
    new CSVSerializer(spark, resultDirectory + configIdentifier,csvResultPath).addGroundTruth().serializeToCsv()
  }

  def getChangeRecordDataSet(rawData: DataFrame) = {
    rawData.map(r => new ChangeRecord(r))
  }

  def getChangeRecordDataSet(filePath:String): Dataset[ChangeRecord] ={
    val rawData = spark.read.option("mode", "DROPMALFORMED").csv(filePath)
    getChangeRecordDataSet(rawData)
  }
}
object Clustering{
  //constants:
  val KeySeparator = "||||"
}
