package de.hpi.data_change.time_series_similarity

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.configuration.{ClusteringAlgorithm, GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesClusterer
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.xml.XML
import scala.collection.Map

object ClusteringMain extends App with Serializable{
  if(args.length<1){
    throw new AssertionError("No Config file provided - terminating")
  }
  val configAsXML = XML.loadFile(args(0))
  //extract config
  val sourceFilePath = (configAsXML \ "sourceFilePath").text
  var resultDirectory = (configAsXML \ "resultDirectory").text
  val granularity = TimeGranularity.withName((configAsXML \ "TimeGranularity" ).text)
  val groupingKey = GroupingKey.withName((configAsXML \ "GroupingKey" ).text)
  val minNumNonZeroYValues = (configAsXML \ "minNumNonZeroYValues" ).text.toInt
  val clusteringAlgorithmParameters = (configAsXML \ "clusteringAlgorithm" \ "_").map( node => (node.label,node.text))
  assert(ClusteringAlgorithm.values.map(_.toString).contains((configAsXML \"clusteringAlgorithm" \"name").text))
  if(!resultDirectory.endsWith(File.separator)){
    resultDirectory = resultDirectory + File.separator
  }
  val configIdentifier = new File(args(0)).getName.split("\\.")(0)
  //intitialize spark
  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  if(args.length==2 && args(1) == "-local" ){
    sparkBuilder = sparkBuilder.master("local[4]")
    resultDirectory = null; //we don't save anything in local mode
  } else{
    //serialize config to hadoop
    System.setProperty("HADOOP_USER_NAME", "leon.bornemann")
    val configPath = new Path(resultDirectory + configIdentifier + ".xml")
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://mut:8020/") //TODO: make this a parameter?
    val fs = FileSystem.get(conf)
    val os = fs.create(configPath)
    os.write(configAsXML.toString().getBytes())
    fs.close()
  }
  val spark = sparkBuilder.getOrCreate()
  new TimeSeriesClusterer(spark,sourceFilePath,minNumNonZeroYValues,granularity,groupingKey,configIdentifier).buildClusters(clusteringAlgorithmParameters.toMap,resultDirectory)
}
