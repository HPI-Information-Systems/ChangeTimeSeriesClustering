package de.hpi.data_change.time_series_similarity.specific_executions

import de.hpi.data_change.time_series_similarity.Clustering
import org.apache.spark.sql.SparkSession

object WikipediaEntityClusteringMain_top34_normalize extends App with Serializable{
  val isLocal = args.length==4 && args(3) == "-local"
  var sparkBuilder = SparkSession
    .builder()
    .appName("Clustering")
    .config("spark.kryoserializer.buffer.max","700MB")
  if(isLocal) {
    sparkBuilder = sparkBuilder.master("local[2]")
  }
  val spark = sparkBuilder.getOrCreate()
  val clusterer = new Clustering(args(1),args(2),spark)
  clusterer.setFileAsDataSource(args(0))
  //fixed params
  clusterer.setGrouper(cr => Seq(cr.entity))
  clusterer.transformation = List("normalize")
  clusterer.numClusters = 34
  clusterer.setTimeBorders(java.sql.Timestamp.valueOf("2001-01-18 00:00:01.0"),java.sql.Timestamp.valueOf("2017-08-02 00:00:01.0"))
  //variable params:
  clusterer.aggregationTimeUnit = "DAYS"
  clusterer.aggregationGranularity = 60
  clusterer.seed = 13
  clusterer.minGroupSize = 50
  clusterer.clustering()
}
