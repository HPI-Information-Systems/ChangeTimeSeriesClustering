package de.hpi.data_change.time_series_similarity

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.Properties

import de.hpi.data_change.time_series_similarity.data.{ChangeRecord, TimeSeries}
import de.hpi.data_change.time_series_similarity.dba.DBAKMeans
import de.hpi.data_change.time_series_similarity.serialization.CSVSerializer
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Framework class for clustering changes
  * @param spark
  */
class Clustering(spark:SparkSession) extends Serializable{

  var filter: Dataset[ChangeRecord] => Dataset[ChangeRecord] = null
  var grouper: ChangeRecord => Seq[String] = null
  var groupFilter: Dataset[(Seq[String],List[ChangeRecord])] => Dataset[(Seq[String],List[ChangeRecord])] = null //g => g._2.size > minGroupSize

  def setIndividualFilter(filter: Dataset[ChangeRecord] => Dataset[ChangeRecord]): Unit = this.filter = filter

  def setGrouper(grouper:ChangeRecord => Seq[String]) = this.grouper = grouper

  //------------------------------------------- Begin Parameters relevant for GroundTruth -------------------------------------------
  var addGroundTruth = true
  var templatesPath = "/home/leon/Documents/researchProjects/wikidata/data/templates.csv"
  //------------------------------------------- End Parameters relevant for GroundTruth -------------------------------------------

  var writeResults = true

  //------------------------------------------- Begin Parameters settable via json config object -------------------------------------------
  var resultDirectory:String = null
  var configIdentifier:String = null
  //Parameters are initilaized with default values
  var aggregationGranularity = 7
  var aggregationTimeUnit = "DAYS"
  var transformation:List[String] = List() //empty list: "None"
  var featureExtraction = "raw"
  var clusteringAlg = "KMeans"
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
  var user:String = null
  var pw:String = null
  var driver = "org.postgresql.Driver"
  //do we use the database or files?
  var useDB = false
  //is the query pre-filtered or do we get all change records from a table?
  var queryIsPrefiltered = false
  //------------------------------------------- End Database Access parameters -----------------------------------------------------------

  //------------------------------------------- Begin Dataset Specific Parameters --------------------------------------------------------
  var start:java.sql.Timestamp = null
  var end:java.sql.Timestamp = null
  //------------------------------------------- End Dataset Specific Parameters ----------------------------------------------------------

  def setFileAsDataSource(filePath:String): Unit = {
    this.filePath = filePath
    useDB = false
  }

  def setDBQueryAsDataSource(query:String,queryIsPrefiltered:Boolean): Unit ={
    this.querystring = query
    this.queryIsPrefiltered = queryIsPrefiltered
    useDB = true
  }

  def setDatabaseAccess(url:String,user:String,pw:String,driver:String):Unit = {
    this.url =url
    this.user = user
    this.pw = pw
    this.driver = driver
  }

  def setDatabaseAccess(url:String,driver:String):Unit = {
    setDatabaseAccess(url,Clustering.STANDARD_USER,Clustering.STANDARD_PW,driver)
  }

  def setTimeBorders(start:Timestamp,end:Timestamp): Unit ={
    this.start = start
    this.end = end
  }

  def setParams(jsonPath:String) = {
    val config = new ObjectMapper().readTree(new File(jsonPath))
    //----------begin new
    if(config.has("start") && config.has("end")) {
      start = java.sql.Timestamp.valueOf(config.get("start").getTextValue)
      end = java.sql.Timestamp.valueOf(config.get("end").getTextValue)
    } else{
      start = null
      end = null
    }
    configIdentifier = config.get("configIdentifier").getTextValue
    addGroundTruth = config.get("addGroundTruth").getBooleanValue
    if(config.has("fileSource")){
      if(config.has("dbSource"))
      println("WARN: both file source and database source specified, file source will be used")
      resultDirectory = config.get("resultDirectory").getTextValue
      setFileAsDataSource(config.get("fileSource").getTextValue)
    } else if (config.has("dbSource")){
      val url = config.get("dbSource").get("url").getTextValue
      val driver = config.get("dbSource").get("driver").getTextValue
      setDatabaseAccess(url,driver)
      val query = config.get("dbSource").get("query").getTextValue
      setDBQueryAsDataSource(query,true)
    }
    //----------end new
    aggregationGranularity = config.get("granularity").getIntValue
    aggregationTimeUnit =config.get("unit").getTextValue
    assert ( ChronoUnit.values().exists(s => s.toString == aggregationTimeUnit))
    val transformationList = scala.collection.JavaConversions.asScalaIterator(config.get("transformation").getElements).toList //TODO: expect a list here
    transformation = transformationList.map( n => n.getTextValue)
    featureExtraction = config.get("featureExtraction").getTextValue
    clusteringAlg = config.get("clusteringAlg").getTextValue
    if(clusteringAlg == "KMeans" || clusteringAlg == "DBAKMeans"){
      val subElementName = clusteringAlg
      numClusters = config.get(subElementName + " Parameters").get("k").getIntValue
      numIterations = config.get(subElementName + " Parameters").get("maxIter").getIntValue
      seed = config.get(subElementName + " Parameters").get("seed").getIntValue
    }
    println(queryIsPrefiltered)
    println(useDB)
  }

  //local parameters:
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
  implicit def ordered2: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }
  import spark.implicits._

  def writeToDB(resultDF: DataFrame,centers:Seq[Array[Double]]) = {
    val centerDF = spark.createDataset(centers.zipWithIndex)
    val toWrite = resultDF.map(r => {
      val keyArray = r.getAs[Seq[String]](0)
      val cluster = r.getAs[Int]("assignedCluster")
      (keyArray.mkString(Clustering.KeySeparator),cluster)
    })
    println("dummy output simulating database write")
    val props = new Properties()
    props.setProperty("user",user)
    props.setProperty("password",pw)
    props.setProperty("driver",driver)
    toWrite.write.jdbc(url,configIdentifier,props)
    centerDF.write.jdbc(url,configIdentifier + "_centers",props)
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
    if(groupFilter != null) {
      filteredGroups = groupFilter(filteredGroups) // .filter{case (id,list) => list.size >= minGroupSize}
    }
    filteredGroups.map{case (id,list) => {
      val entity = list.head.entity
      val timestamps = list.map(cr => cr.timestamp.toLocalDateTime)
      (id.asInstanceOf[Seq[String]],timestamps)
    }}
  }

  def transformArbitraryDatasetToGroup(dataset: DataFrame) = {
    dataset.map(r =>{
      val keys = r.toSeq.slice(0,r.size-1).map( e => e.toString)
      (keys,r.getAs[java.sql.Timestamp](r.size-1).toLocalDateTime)
    }).groupByKey{case (key,_) => key}
      .mapGroups{case (key,it) => (key.asInstanceOf[Seq[String]],it.toList.map{case (key,ts) => ts})}
  }

  def noInputSet(): Boolean = filePath == null && querystring == null

  def clustering() = {
    if(noInputSet()){
      throw new AssertionError("Either file input or database input must be specified")
    }
    var filteredGroups:Dataset[(Seq[String],List[LocalDateTime])] = null
    if(useDB){
      if(queryIsPrefiltered){
        var dataset = getArbitraryQueryResult(url,querystring)
        println(dataset.count())
        filteredGroups = transformArbitraryDatasetToGroup(dataset)
        println(filteredGroups.count())
      } else {
        var dataset = getChangeRecordSetFromDB()
        println(dataset.count())
        filteredGroups = getFilteredGroups(dataset)
        println(filteredGroups.count())
      }
    } else{
      var dataset = getChangeRecordDataSet(filePath)
      //individual filter
      if(filter != null) {
        dataset = filter(dataset)
      }
      val distinctEntities = dataset.map(r => r.entity).distinct().count()
      println("Number of distinct entities: " +distinctEntities)
      filteredGroups = getFilteredGroups(dataset)
      println("Number of filtered Groups: " + filteredGroups.count())
    }
    //create time series:
    if(end == null || start == null){
      val startEnd = filteredGroups.map( t => (Timestamp.valueOf(t._2.min),Timestamp.valueOf(t._2.max))).reduce( (t1,t2) => (List(t1._1,t2._1).min,List(t1._2,t2._2).max))
      start = startEnd._1
      end = startEnd._2
    }
    var timeSeriesDataset = filteredGroups.map { case (id, list) => {
      TimeSeries(id, toTimeSeries(list, aggregationGranularity, start, end), aggregationGranularity, aggregationTimeUnit.toUpperCase, end)
    }}
    timeSeriesDataset = timeSeriesDataset.filter( ts => ts !=null)
    //transform time series:
    timeSeriesDataset = timeSeriesDataset.map( ts => ts.transform(transformation))
    //filter transformed time series:
    timeSeriesDataset = timeSeriesDataset.filter(ts => ts.filter())
    println("Number of filtered Time Series: " + timeSeriesDataset.count())
    //extract features:
    val rdd = timeSeriesDataset.rdd.map(ts => {assert(ts.id !=null);RowFactory.create(ts.id.toArray,Vectors.dense(ts.featureExtraction(featureExtraction)))})
    rdd.cache()
    val fields = Array(DataTypes.createStructField("name",DataTypes.createArrayType(DataTypes.StringType),false),DataTypes.createStructField("features",VectorType,false)) //TODO: how to save this as row/list
    val schema = new StructType(fields)
    val finalDf = spark.createDataFrame(rdd,schema)
    println("Final DF Size: " + finalDf.count())
    //Clustering:
    var resultDF:Dataset[Row] = null
    var centers:Seq[Array[Double]] = null
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
    if(addGroundTruth){
      resultDF = addGroundTruth(resultDF)
    }
    if(writeResults) {
      if (useDB) {
        writeToDB(resultDF, centers)
      } else {
        new CSVSerializer(spark, resultDirectory + configIdentifier, centers, resultDF).serializeToCsv()
      }
    }
  }

  def addGroundTruth(clusteringResult:DataFrame):DataFrame ={
    //var templates = clusterer.getArbitraryQueryResult(clusterer.url,"SELECT * FROM templates_infoboxes").as[(String,String)]
    val actualString = "infobox settlement\n infobox album\n infobox football biography\n infobox musical artist\n infobox film\n infobox person\n infobox single\n infobox company\n infobox actor\n infobox nrhp\n infobox french commune\n infobox book\n infobox television\n infobox military person\n infobox radio station\n infobox university\n infobox television episode\n infobox video game\n infobox officeholder\n infobox indian jurisdiction\n infobox uk place\n infobox school\n infobox road\n infobox writer\n infobox baseball biography\n infobox military unit\n infobox mountain\n infobox military conflict\n infobox german location\n infobox airport\n infobox ice hockey player\n infobox scientist\n infobox football club"
    val actual = actualString.split("\n").map(s => s.trim).toSet
    var templates = spark.read.csv(templatesPath)
        .filter( r => actual.contains(r.getString(1)))
    templates = templates.withColumnRenamed(templates.columns(0),"entity")
      .withColumnRenamed(templates.columns(1),"template")
    templates = templates.as("template")
    println("Templates size: " + templates.count())
    val changerecords = clusteringResult.as("result")
    val joined = templates.join(changerecords,lower(col("name").apply(0)) === lower(col("template.entity")))
    println("After join size: " + joined.count())
    joined.withColumnRenamed("template","trueCluster")
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

  val STANDARD_USER = "monetdb"
  val STANDARD_PW = "monetdb"
}
