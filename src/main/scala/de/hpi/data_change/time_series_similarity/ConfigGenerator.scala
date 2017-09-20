package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.configuration._
import de.hpi.data_change.time_series_similarity.io.DataIO


object ConfigGenerator extends App{

  val sourceFilePathsServer = List("/users/leon.bornemann/data/20120323-en-updates_new_full_repaired_without_Value.csv","/users/leon.bornemann/settlements/dump.csv")
  val targetDir = "/home/leon/Documents/researchProjects/wikidata/configs/"


  generateSingleConfig("seasonalProperties_Normalized_Filtered",DataIO.getFullWikidataSparkCompatibleFile().getAbsolutePath,DataIO.getConfigTargetDir(),"Local")
  //generateSingleConfig("seasonalProperties_Normalized",DataIO.getHDFSWikidataFilePath(),DataIO.getConfigTargetDir())

  def generateSingleConfig(configName: String, sourceFilePath: String,resultPath:String,environment:String = "Server"): Unit = {
    val targetDir = "home/leon/Documents/researchProjects/wikidata/configs/"
    var resultDirectory = ""
    if(environment == "Server") {
      resultDirectory = "/users/leon.bornemann/results/"
    } else{
      assert(environment == "Local")
      resultDirectory = "file:///home/leon/Documents/data/results/localResults/"
    }
    val config = new ClusteringConfig()
    config.timeSeriesFilter = TimeSeriesFilter(Map(("name","MinNonZeryYVals"),("minNumNonZero","6")))
    config.clusteringAlgorithmParameters = Map(("name",ClusteringAlgorithm.KMeans.toString),
      ("FeatureExtractionMethod",FeatureExtractionMethod.EntireTimeSeriesNormalized.toString),
      ("k","13"),
      ("maxIter","100"))
    config.granularity = TimeGranularity.MonthOfYear
    config.groupingKey = GroupingKey.Property
    config.resultDirectory = resultDirectory
    config.sourceFilePath = sourceFilePath
    config.serializeToFile(resultPath + configName + "_config.xml")
  }

  def generate(configName: String, sourceFilePath: String,resultPath:String,environment:String = "Server") = {
    var resultDirectory = ""
    if(environment == "Server") {
      resultDirectory = "/users/leon.bornemann/results/"
    } else{
      assert(environment == "Local")
      resultDirectory = "file:///home/leon/Documents/data/results/localResults/"
    }
    val timeGranularities = TimeGranularity.values
    val groupingKeys = List(GroupingKey.Entity,GroupingKey.Property) //GroupingKey.Value_
    val numClusters = List(2,3,4,5,6)
    val filters = List(TimeSeriesFilter(Map(("name","MaxAverageY"),("maxAvg",(50.0/12.0).toString))))
    val maxIter = 200
    var i = 0
    val configs = scala.collection.mutable.ListBuffer[ClusteringConfig]()
    for(timeGranularity <- timeGranularities;groupingKey <- groupingKeys;k <- numClusters;filter <- filters){
      val config = new ClusteringConfig()
      config.sourceFilePath = sourceFilePath
      config.resultDirectory = resultDirectory
      config.granularity = timeGranularity
      config.groupingKey = groupingKey
      config.timeSeriesFilter =filter
      config.clusteringAlgorithmParameters = Map(("name",ClusteringAlgorithm.KMeans.toString),
        ("FeatureExtractionMethod",FeatureExtractionMethod.EntireTimeSeries.toString),
        ("k",k.toString),
        ("maxIter",maxIter.toString))
      configs.append( config)
      i+=1
    }
    configs.sortBy(c => (c.groupingKey,c.granularity,c.clusteringAlgorithmParameters("k")) ).zipWithIndex.foreach{ case (config,i) => config.serializeToFile(resultPath + configName + "_config" + i +".xml")}
  }
  //generate("settlements",sourceFilePathsLocal(0),targetDir,"Local")
  //generate("settlements",sourceFilePathsServer(0),targetDir)
  //generate("wikidata_complete",sourceFilePathsServer(1),targetDir)


}
