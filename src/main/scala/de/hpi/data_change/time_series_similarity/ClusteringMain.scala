package de.hpi.data_change.time_series_similarity

import java.io.{File, FileWriter, PrintWriter}
import java.sql.Timestamp
import java.time.LocalDateTime

import de.hpi.data_change.time_series_similarity.configuration.{ClusteringAlgorithm, ClusteringConfig, GroupingKey, TimeGranularity}
import de.hpi.data_change.time_series_similarity.data.ChangeRecord
import de.hpi.data_change.time_series_similarity.data_mining.TimeSeriesClusterer
import de.hpi.data_change.time_series_similarity.io.{HadoopInteraction, ResultSerializer}
import org.apache.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.xml.XML
import scala.collection.Map

object ClusteringMain extends App with Serializable{

  private def isLocalMode = {
    args.length == 2 && args(1) == "-local"
  }

  if(args.length<1){
    throw new AssertionError("No Config file provided - terminating")
  }
  //intitialize spark
  var sparkBuilder = SparkSession
    .builder()
    .appName("Spark SQL basic example")
  if (isLocalMode) {
    sparkBuilder = sparkBuilder.master("local[4]")
  }
  val spark = sparkBuilder.getOrCreate()
  for(file <- new File(args(0)).listFiles().filter(f => f.getName.endsWith(".xml"))) {
    val writer = new PrintWriter(new FileWriter(new File("selfmadelog.txt"),true))
    val config = new ClusteringConfig(file.getAbsolutePath)
    //extract config
    val sourceFilePath = config.sourceFilePath
    var resultDirectory = config.resultDirectory
    val granularity = config.granularity
    val groupingKey = config.groupingKey
    val minNumNonZeroYValues = config.minNumNonZeroYValues
    val clusteringAlgorithmParameters = config.clusteringAlgorithmParameters
    if (!resultDirectory.endsWith(File.separator)) {
      resultDirectory = resultDirectory + File.separator
    }
    val configIdentifier = config.configIdentifier
    writer.println(LocalDateTime.now() +  ":  starting config " + configIdentifier)
    writer.close()
    var hadoopInteraction = new HadoopInteraction()
    if (isLocalMode) {
      resultDirectory = null;
      hadoopInteraction = null;
    } else {
      //serialize config to hadoop
      hadoopInteraction.writeToFile(config.getAsXML(),resultDirectory +configIdentifier + "/" + configIdentifier + ".xml")
    }
    val res = new TimeSeriesClusterer(spark, sourceFilePath, minNumNonZeroYValues, granularity, groupingKey, configIdentifier).buildClusters(clusteringAlgorithmParameters)
    val resultDf = res._1
    val model = res._2
    if(!isLocalMode){
      val tablePath = resultDirectory + "results.CSV.txt"
      val resultSerializer = new ResultSerializer(spark,resultDf,model)
      val res = resultSerializer.serialize(resultDirectory,configIdentifier,config)
      //hadoopInteraction.appendLineToCsv(res,tablePath)
    }
  }
}
