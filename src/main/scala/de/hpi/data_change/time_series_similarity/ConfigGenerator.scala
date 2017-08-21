package de.hpi.data_change.time_series_similarity

import de.hpi.data_change.time_series_similarity.configuration.{ClusteringAlgorithm, FeatureExtractionMethod, GroupingKey, TimeGranularity}


object ConfigGenerator extends App{

  val sourceFilePaths = List("/users/leon.bornemann/settlements/dump.csv","/users/leon.bornemann/wikidata/dump.csv")
  val targetDir = "C:\\Users\\Leon.Bornemann\\Documents\\Database Changes\\Clusterin Execution configs\\generated\\"

  def generate(configName: String, sourceFilePath: String,resultPath:String) = {
    val resultDirectory = "/users/leon.bornemann/results/"
    val timeGranularities = TimeGranularity.values
    val groupingKeys = List(GroupingKey.Entity,GroupingKey.Property)
    val numClusters = List(2,3,4,5,6)
    val maxIter = 200
    var i = 0
    for(timeGranularity <- timeGranularities;groupingKey <- groupingKeys;k <- numClusters){
      val config = new ClusteringConfig()
      config.sourceFilePath = sourceFilePath
      config.resultDirectory = resultDirectory
      config.granularity = timeGranularity
      config.groupingKey = groupingKey
      config.minNumNonZeroYValues =0 //TODO: what do we do here?
      config.clusteringAlgorithmParameters = Map(("name",ClusteringAlgorithm.KMeans.toString),
        ("FeatureExtractionMethod",FeatureExtractionMethod.EntireTimeSeries.toString),
        ("k",k.toString),
        ("maxIter",maxIter.toString))
      config.serializeToFile(resultPath + configName + "_config" + i +".xml")
      i+=1
    }
  }

  generate("settlements",sourceFilePaths(0),targetDir)
  generate("wikidata_complete",sourceFilePaths(1),targetDir)

//  val config = new ClusteringConfig()
//  //extract config
//  val sourceFilePath = config.sourceFilePath
//  var resultDirectory = config.resultDirectory
//  val granularity = config.granularity
//  val groupingKey = config.groupingKey
//  val minNumNonZeroYValues = config.minNumNonZeroYValues
}
