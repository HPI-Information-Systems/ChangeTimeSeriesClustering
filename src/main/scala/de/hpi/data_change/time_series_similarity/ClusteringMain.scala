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
  val config = new ClusteringConfig(args(0))
  //extract config
  val sourceFilePath = config.sourceFilePath
  var resultDirectory = config.resultDirectory
  val granularity = config.granularity
  val groupingKey = config.groupingKey
  val minNumNonZeroYValues = config.minNumNonZeroYValues
  val clusteringAlgorithmParameters = config.clusteringAlgorithmParameters
  if(!resultDirectory.endsWith(File.separator)){
    resultDirectory = resultDirectory + File.separator
  }
  val configIdentifier = config.configIdentifier
  //intitialize spark
  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  if(args.length==2 && args(1) == "-local" ){
    sparkBuilder = sparkBuilder.master("local[4]")
    resultDirectory = null; //we don't save anything in local mode
  } else{
    //serialize config to hadoop
    config.serializeToHadoop()
  }
  val spark = sparkBuilder.getOrCreate()
  new TimeSeriesClusterer(spark,sourceFilePath,minNumNonZeroYValues,granularity,groupingKey,configIdentifier).buildClusters(clusteringAlgorithmParameters,resultDirectory)
}
