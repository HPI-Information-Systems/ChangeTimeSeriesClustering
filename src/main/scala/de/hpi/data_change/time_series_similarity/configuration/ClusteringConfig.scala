package de.hpi.data_change.time_series_similarity.configuration

import java.io.{File, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.xml.{Elem, XML}

class ClusteringConfig() {
  def serializeToFile(filePath: String) = {
    val pw = new PrintWriter(new File(filePath))
    pw.write(getAsXML())
    pw.close
  }

  var sourceFilePath: String=null
  var resultDirectory:String=null
  var granularity:TimeGranularity.Value=null
  var groupingKey:GroupingKey.Value=null
  var timeSeriesFilter:TimeSeriesFilter= null;
  var clusteringAlgorithmParameters:Map[String,String]=null
  var configIdentifier:String=null
  var configAsXML:Elem = null

  //assert(ClusteringAlgorithm.values.map(_.toString).contains((configAsXML \"clusteringAlgorithm" \"name").text))

  def this(filePath: String) ={
    this()
    configAsXML = XML.loadFile(filePath)
    sourceFilePath = (configAsXML \ "sourceFilePath").text
    resultDirectory = (configAsXML \ "resultDirectory").text
    granularity = TimeGranularity.withName((configAsXML \ "TimeGranularity" ).text)
    groupingKey = GroupingKey.withName((configAsXML \ "GroupingKey" ).text)
    clusteringAlgorithmParameters = (configAsXML \ "clusteringAlgorithm" \ "_").map( node => (node.label,node.text)).toMap
    timeSeriesFilter = new TimeSeriesFilter((configAsXML \ "timeSeriesFilter" \ "_").map( node => (node.label,node.text)).toMap)
    configIdentifier = new File(filePath).getName.split("\\.")(0)
  }

  def getAsXML() = {
    val x = new xml.NodeBuffer
    x += <sourceFilePath>{sourceFilePath}</sourceFilePath>
    x += <resultDirectory>{resultDirectory}</resultDirectory>
    x += <TimeGranularity>{granularity}</TimeGranularity>
    x += <GroupingKey>{groupingKey}</GroupingKey>

    val y = new xml.NodeBuffer
    clusteringAlgorithmParameters.toList.foreach{case(k,v) => y += <xml>{v}</xml>.copy(label=k)}
    x+= <clusteringAlgorithm>{y}</clusteringAlgorithm>

    val z = new xml.NodeBuffer
    timeSeriesFilter.params.toList.foreach{case(k,v) => z += <xml>{v}</xml>.copy(label=k)}
    x += <timeSeriesFilter>{z}</timeSeriesFilter>
    val prettyPrinter = new scala.xml.PrettyPrinter(500,2)
    prettyPrinter.format(<config>{x}</config>)
  }

  def serializeToHadoop() = {
    System.setProperty("HADOOP_USER_NAME", "leon.bornemann")
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://mut:8020/") //TODO: make this a parameter?
    val fs = FileSystem.get(conf)
    val configPath = new Path(resultDirectory +configIdentifier + org.apache.hadoop.fs.Path.SEPARATOR + configIdentifier + ".xml")
    val os = fs.create(configPath)
    os.write(getAsXML.getBytes())
    //fs.close()
  }

//  val configAsXML = XML.loadFile(filePath)
//  //extract config
//  val sourceFilePath = (configAsXML \ "sourceFilePath").text
//  var resultDirectory = (configAsXML \ "resultDirectory").text
//  val granularity = TimeGranularity.withName((configAsXML \ "TimeGranularity" ).text)
//  val groupingKey = GroupingKey.withName((configAsXML \ "GroupingKey" ).text)
//  val minNumNonZeroYValues = (configAsXML \ "minNumNonZeroYValues" ).text.toInt
//  val clusteringAlgorithmParameters = (configAsXML \ "clusteringAlgorithm" \ "_").map( node => (node.label,node.text))
//  assert(ClusteringAlgorithm.values.map(_.toString).contains((configAsXML \"clusteringAlgorithm" \"name").text))
//  val configIdentifier = new File(filePath).getName.split("\\.")(0)


}